# -*- coding: utf-8 -*-
"""
ProofPay – Telegram Bot (SOL on-chain, Auto-Deposit/Auto-Withdraw, Escrow)
Kompatibel mit: solana==0.25.0  (wichtig!)

Hauptfeatures:
- Nutzer-Registrierung, Hauptmenü (DE/EN)
- Deposit via Source-Wallet-Erkennung
- On-Chain Withdraw (SOL)  [robust: RAW send + flexible result parsing]
- Senden: F&F und Escrow (Hold/Release/Dispute)
- Referral, 2FA, Support
- Backoff & klare Fehlermeldungen

NEU:
- Benutzer-Passwort (Einstellungen) + Abfrage bei Senden/Withdraw (vor 2FA)
- Über uns (DE/EN, je 12 Sätze)
- Richtlinien (AGB, Datenschutz, Risikohinweise)
- Hilfe
- Adminbereich: Gebühren-Summe, Nutzerzahlen, aktive Nutzer (30d), Summe Escrow-„held“,
  sowie Guthaben ändern per Benutzer-ID
"""

import os, json, time, threading, sqlite3, uuid, random, string, re, hashlib, secrets
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
import requests
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# -------- Solana (Version 0.25.0) --------
from solana.keypair import Keypair
from solana.publickey import PublicKey
from solana.system_program import transfer, TransferParams
from solana.rpc.api import Client
from solana.transaction import Transaction
from solana.rpc.types import TxOpts

# ------------------ ENV ---------------------
load_dotenv()
BOT_TOKEN     = os.getenv("BOT_TOKEN","8222875136:AAFF-A-DqFiT7ahzsLZuraAgQj27OYlvqcI").strip()
ADMIN_IDS     = [int(x) for x in os.getenv("ADMIN_IDS","8076025426").split(",") if x.strip().isdigit()]
DEFAULT_LANG  = os.getenv("DEFAULT_LANG","en").strip().lower()
SOL_RPC_URL   = os.getenv("SOL_RPC_URL","https://api.mainnet-beta.solana.com").strip()

FEE_FNF = Decimal(os.getenv("FEE_FNF","2.0"))              # %
FEE_ESCROW_EXTRA = Decimal(os.getenv("FEE_ESCROW_EXTRA","3.0"))  # %

MIN_DEPOSIT_SOL   = Decimal(os.getenv("MIN_DEPOSIT_SOL","0.0005"))
MIN_WITHDRAW_SOL  = Decimal(os.getenv("MIN_WITHDRAW_SOL","0.0005"))
MAX_WITHDRAW_SOL  = Decimal(os.getenv("MAX_WITHDRAW_SOL","50"))
DEPOSIT_POLL_SECONDS = int(os.getenv("DEPOSIT_POLL_SECONDS","15"))

CENTRAL_WALLET_SECRET = os.getenv("CENTRAL_WALLET_SECRET","[216,228,184,240,28,208,86,251,72,207,66,95,46,213,227,92,3,151,107,135,207,35,239,106,204,30,183,73,9,76,39,133,231,92,227,79,168,2,181,228,68,217,227,49,92,136,161,209,206,110,146,237,79,243,145,54,121,109,106,22,160,136,164,90]").strip()
CENTRAL_WALLET_ADDRESS = os.getenv("CENTRAL_WALLET_ADDRESS","Ga9L4teyfbnJcxhhErKAquF8cHy3GR6XPF1sqxji3DN9").strip()

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN fehlt in .env")
if not CENTRAL_WALLET_SECRET or not CENTRAL_WALLET_ADDRESS:
    raise SystemExit("CENTRAL_WALLET_SECRET und CENTRAL_WALLET_ADDRESS sind Pflicht.")

# Keypair aus 64-Byte Array
try:
    secret_list = json.loads(CENTRAL_WALLET_SECRET)
    if not (isinstance(secret_list, list) and len(secret_list) == 64):
        raise ValueError("CENTRAL_WALLET_SECRET muss 64-Byte JSON-Array sein (solana-keygen export).")
    kp = Keypair.from_secret_key(bytes(secret_list))
    if str(kp.public_key) != CENTRAL_WALLET_ADDRESS:
        print("WARN: CENTRAL_WALLET_ADDRESS stimmt nicht mit Secret überein. Bitte prüfen.")
except Exception as e:
    raise SystemExit(f"Ungültiger CENTRAL_WALLET_SECRET: {e}")

# RPC
rpc  = Client(SOL_RPC_URL)
sess = requests.Session()

# ------------------ DB ----------------------
DB="proofpay.db"
conn = sqlite3.connect(DB, check_same_thread=False)
conn.row_factory = sqlite3.Row

def init_db():
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY,
        username TEXT, first_name TEXT, last_name TEXT,
        lang TEXT DEFAULT 'de',
        twofa_enabled INTEGER DEFAULT 1,
        ref_code TEXT, ref_by INTEGER,
        created_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS balances(
        user_id INTEGER, asset TEXT, available REAL DEFAULT 0, held REAL DEFAULT 0,
        PRIMARY KEY(user_id,asset)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS tx_log(
        id TEXT PRIMARY KEY,
        type TEXT,          -- deposit|send|escrow_hold|escrow_release|withdraw
        user_from INTEGER, user_to INTEGER,
        asset TEXT, amount REAL, fee REAL,
        chain_sig TEXT, meta TEXT,
        created_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS deposit_seen(
        sig TEXT PRIMARY KEY
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS expected_sources(
        user_id INTEGER,
        source_addr TEXT,
        created_at TEXT,
        PRIMARY KEY(user_id, source_addr)
    )""")
    conn.commit()
init_db()

# --- Schema-Erweiterungen (NEU) ---
def column_exists(table, col):
    r = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row["name"] == col for row in r)

def ensure_schema():
    # Passwort-Felder
    if not column_exists("users","pass_enabled"):
        conn.execute("ALTER TABLE users ADD COLUMN pass_enabled INTEGER DEFAULT 0")
    if not column_exists("users","pw_hash"):
        conn.execute("ALTER TABLE users ADD COLUMN pw_hash TEXT")
    if not column_exists("users","pw_salt"):
        conn.execute("ALTER TABLE users ADD COLUMN pw_salt TEXT")
    conn.commit()
ensure_schema()

# System-User/Balances sicherstellen (wichtig für Gebühren!)
def ensure_system():
    row = conn.execute("SELECT 1 FROM users WHERE user_id=0").fetchone()
    if not row:
        conn.execute(
            """INSERT INTO users(user_id, username, first_name, last_name, lang, twofa_enabled, ref_code, ref_by, created_at, pass_enabled, pw_hash, pw_salt)
               VALUES(0, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL)""",
            ("system","","", (DEFAULT_LANG if DEFAULT_LANG in ("de","en") else "en"),
             0, "R0", None, datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
    # Sprache reparieren
    row = conn.execute("SELECT lang FROM users WHERE user_id=0").fetchone()
    if row:
        lang = row["lang"]
        if not isinstance(lang, str) or lang.lower() not in ("de","en"):
            conn.execute("UPDATE users SET lang=? WHERE user_id=0",
                         ((DEFAULT_LANG if DEFAULT_LANG in ("de","en") else "en"),))
            conn.commit()
    # Balance SOL für User 0
    b = conn.execute("SELECT 1 FROM balances WHERE user_id=0 AND asset='SOL'").fetchone()
    if not b:
        conn.execute("INSERT INTO balances(user_id, asset, available, held) VALUES(0,'SOL',0,0)")
        conn.commit()
ensure_system()

ASSETS = {"SOL": 9}

def now_iso(): return datetime.now(timezone.utc).isoformat()
def dquant(x, dec): return Decimal(x).quantize(Decimal(10) ** -dec, rounding=ROUND_DOWN)
def fmt(asset, x): return f"{dquant(Decimal(x), ASSETS[asset])} {asset}"
def is_admin(uid): return uid in ADMIN_IDS

def ensure_user(tu, ref_by=None):
    r = conn.execute("SELECT * FROM users WHERE user_id=?", (tu.id,)).fetchone()
    if not r:
        code = f"R{tu.id}"
        conn.execute("""INSERT INTO users(user_id,username,first_name,last_name,lang,twofa_enabled,ref_code,ref_by,created_at,pass_enabled,pw_hash,pw_salt)
                        VALUES(?,?,?,?,?,?,?,?,?,0,NULL,NULL)""",
                     (tu.id, tu.username or "", tu.first_name or "", tu.last_name or "",
                      DEFAULT_LANG if DEFAULT_LANG in ("de","en") else "en", 1, code, ref_by, now_iso()))
        for a in ASSETS: conn.execute("INSERT INTO balances(user_id,asset,available,held) VALUES(?,?,0,0)", (tu.id, a))
        conn.commit()

def get_user(uid): return conn.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
def get_user_by_username(u):
    if not u: return None
    return conn.execute("SELECT * FROM users WHERE lower(username)=?", (u.lstrip("@").lower(),)).fetchone()
def get_username(uid):
    r = conn.execute("SELECT username FROM users WHERE user_id=?", (uid,)).fetchone()
    return (r["username"] or str(uid)) if r else str(uid)

def bal(uid, asset):
    r = conn.execute("SELECT available,held FROM balances WHERE user_id=? AND asset=?", (uid, asset)).fetchone()
    if not r:
        conn.execute("INSERT OR IGNORE INTO balances(user_id,asset,available,held) VALUES(?,?,0,0)", (uid, asset))
        conn.commit()
        r = conn.execute("SELECT available,held FROM balances WHERE user_id=? AND asset=?", (uid, asset)).fetchone()
    return Decimal(str(r["available"])), Decimal(str(r["held"]))

def bal_set(uid, asset, av=None, hd=None):
    a,h = bal(uid, asset)
    if av is None: av = a
    if hd is None: hd = h
    conn.execute("UPDATE balances SET available=?, held=? WHERE user_id=? AND asset=?",
                 (float(av), float(hd), uid, asset)); conn.commit()

def bal_adj(uid, asset, da=Decimal("0"), dh=Decimal("0")):
    a,h = bal(uid, asset)
    bal_set(uid, asset, a+da, h+dh)

# ---------- Passwort Utils ----------
def _hash_pw(pw:str, salt:str)->str:
    return hashlib.sha256((salt + pw).encode("utf-8")).hexdigest()

def user_has_password(uid:int)->bool:
    r = get_user(uid)
    return bool(r and r["pass_enabled"])

def verify_password(uid:int, pw:str)->bool:
    r = get_user(uid)
    if not r or not r["pass_enabled"] or not r["pw_hash"] or not r["pw_salt"]:
        return False
    return _hash_pw(pw, r["pw_salt"]) == r["pw_hash"]

# ------------------ I18N --------------------
I18N = {
 "de":{
  "welcome": "👋 Willkommen bei <b>ProofPay</b>\n\nSichere Krypto-Zahlungen in Telegram – schnell, günstig und mit Verkäuferschutz.\n\nWähle unten eine Aktion:",
  "menu":"🏠 <b>Hauptmenü</b>\n\n• 💰 Guthaben ansehen und verwalten\n• ➕ Einzahlen (SOL) – über deine <b>Quelle-Wallet</b>\n• 📤 Senden – Friends & Family oder 🛡️ Escrow\n• ➖ Auszahlen – On-Chain von der Bot-Wallet\n• 🧾 Verlauf – letzte Transaktionen\n• ⚙️ Einstellungen – Sprache & 2FA\n• 🆘 Support – direkt an Admin",
  "btn_balance":"💰 Guthaben", "btn_deposit":"➕ Einzahlen", "btn_send":"📤 Senden",
  "btn_withdraw":"➖ Auszahlen", "btn_history":"🧾 Verlauf", "btn_settings":"⚙️ Einstellungen", "btn_support":"🆘 Support",
  "btn_about":"ℹ️ Über uns", "btn_policies":"📜 Richtlinien", "btn_help":"❓ Hilfe", "btn_admin":"🛠️ Admin",
  "balance":"<b>Dein Guthaben</b>\n{lines}\n\n📫 <b>Unsere Einzahlungsadresse:</b>\n<code>{addr}</code>\n\nℹ️ Zahle SOL von einer <b>von dir angegebenen Quelle-Wallet</b> auf diese Adresse ein. Wir erkennen die Zahlung automatisch.",
  "line":"• {asset}: Verfügbar <b>{av}</b> | Einbehalten <b>{hd}</b>",
  "deposit_ask_source":"➕ <b>Einzahlen (SOL)</b>\n\nSende jetzt die <b>Absender-Wallet-Adresse</b> (deine SOL-Adresse), von der du die Einzahlung schicken wirst.",
  "deposit_source_ok":"✅ Quelle gespeichert:\n<code>{src}</code>\n\nSende jetzt SOL an unsere Adresse:\n<code>{addr}</code>\n\nMin: {min}\nWir scannen on-chain und schreiben gut, wenn die Zahlung von deiner Quelle kommt.",
  "send_who":"📤 <b>Senden</b>\nWen möchtest du bezahlen? Antworte mit <code>@username</code>.",
  "send_amt":"Empfänger: <b>@{u}</b>\nGib Betrag ein, z. B. <code>0.25</code> (Asset: SOL).",
  "send_mode":"Betrag: <b>{amt}</b> SOL\nWähle den Modus:",
  "mode_fnf":"👥 Friends & Family",
  "mode_escrow":"🛡️ Verkäuferschutz (Escrow)",
  "sent_fnf_sender":"✅ Gesendet an @{u}: {amt} SOL (F&F) – Fee {fee}%",
  "sent_fnf_recv":"📥 Du hast {amt} SOL von @{u} erhalten (F&F).",
  "escrow_hold_s":"🛡️ An @{u} gesendet: {amt} SOL – <b>einbehalten</b> bis Freigabe.",
  "escrow_hold_r":"🛡️ {amt} SOL von @{u} erhalten – <b>einbehalten</b>.",
  "escrow_btn_release":"✅ Ware erhalten → Freigeben",
  "escrow_btn_dispute":"❗ Problem melden",
  "escrow_release_ok":"✅ Escrow freigegeben. Betrag gutgeschrieben.",
  "escrow_dispute_open":"⚠️ Dispute eröffnet. Admin informiert.",
  "withdraw_addr":"➖ <b>Auszahlen</b>\nSende Ziel-Adresse (SOL, Base58). Mindestbetrag: {min}",
  "withdraw_amt":"Gib Betrag in SOL ein (min {min}, max {max}).",
  "withdraw_ok":"💸 Auszahlung erstellt: {amt} SOL\nTx: <code>{sig}</code>",
  "history_none":"(Noch keine Transaktionen.)",
  "settings":"⚙️ <b>Einstellungen</b>\n• Sprache: <b>{lang}</b>\n• 2FA: <b>{twofa}</b>\n• Dein Referral-Code: <code>{ref}</code>\n• Passwortschutz: <b>{pw}</b>",
  "twofa_toggled":"🔐 2FA ist jetzt: {st}",
  "support_prompt":"🆘 Beschreibe dein Anliegen. Wir antworten hier im Chat.",
  "deposit_booked":"✅ Einzahlung verbucht: +{amt} SOL\nTx: <code>{sig}</code>",
  "err_rpc":"RPC überlastet. Bitte kurz später erneut versuchen.",
  "err_amt":"Ungültiger Betrag.",
  "err_balance":"Unzureichendes Guthaben. Verfügbar: {av}",
  "err_addr":"Ungültige SOL-Adresse.",
  "err_src":"Das ist keine gültige Solana-Adresse.",
  # Neu: Passwort & Screens
  "set_pw_prompt":"🔑 Sende dein <b>neues Passwort</b> (mind. 4 Zeichen).",
  "set_pw_confirm":"Bestätige dein Passwort – sende es <b>noch einmal</b>.",
  "set_pw_ok":"✅ Passwort gesetzt. Wird jetzt bei <b>Senden</b> und <b>Auszahlen</b> abgefragt.",
  "del_pw_ok":"✅ Passwortschutz entfernt.",
  "pw_mismatch":"❌ Die beiden Eingaben stimmen nicht überein. Vorgang abgebrochen.",
  "pw_short":"❌ Das Passwort ist zu kurz.",
  "pw_ask":"🔑 Bitte gib dein Passwort ein:",
  "pw_wrong":"❌ Falsches Passwort.",
  # Neu: About/Policies/Help
  "about_text": "ℹ️ <b>Über uns</b>\n"
                "1) ProofPay ermöglicht sichere Krypto-Zahlungen in Telegram.\n"
                "2) Wir setzen auf Escrow zum Käuferschutz.\n"
                "3) Einzahlungen werden automatisch on-chain erkannt.\n"
                "4) Auszahlungen erfolgen direkt von der Hot-Wallet.\n"
                "5) 2FA und Passwortschutz sind verfügbar.\n"
                "6) Gebühren sind transparent und fair.\n"
                "7) Wir nutzen zuverlässige RPC-Endpunkte.\n"
                "8) Deine Daten werden minimal und zweckgebunden gespeichert.\n"
                "9) Disputes werden an Admins gemeldet.\n"
                "10) Referral belohnt das Einladen neuer Nutzer.\n"
                "11) Wir arbeiten kontinuierlich an Stabilität und Sicherheit.\n"
                "12) Lizenzen/Registrierungen gemäß lokalem Recht vorhanden.",
  "policies_text": "📜 <b>Richtlinien</b>\n\n"
                   "<b>AGB (Kurzfassung)</b>\n"
                   "• Nutzung auf eigenes Risiko. Gebühren gemäß Anzeige.\n"
                   "• Verbot von Betrug/illegalen Aktivitäten.\n"
                   "• Wir können Konten bei Verstößen sperren.\n\n"
                   "<b>Datenschutz</b>\n"
                   "• Wir speichern nur, was für den Service nötig ist (z. B. User-ID, Balances, TX-Logs).\n"
                   "• Keine Weitergabe an Dritte außer gesetzliche Pflicht.\n"
                   "• Du kannst Auskunft/Löschung im Rahmen geltenden Rechts anfragen.\n\n"
                   "<b>Risikohinweise</b>\n"
                   "• Kryptowerte sind volatil und Transaktionen sind irreversibel.\n"
                   "• On-Chain-Risiken (Netzwerkstörungen, Gebühren, Fehlbedienungen) liegen beim Nutzer.\n"
                   "• Nutze Escrow nur mit vertrauenswürdigen Handelspartnern und bestätige erst nach Erhalt.\n",
  "help_text": "❓ <b>Hilfe</b>\n"
               "• <b>Einzahlen:</b> Quelle-Wallet angeben, dann an Deposit-Adresse senden.\n"
               "• <b>Senden:</b> @username eingeben, Betrag wählen, Modus (F&F/Escrow) wählen.\n"
               "• <b>Escrow:</b> Betrag wird gehalten, Käufer kann freigeben oder Dispute öffnen.\n"
               "• <b>Auszahlen:</b> Zieladresse angeben, Betrag eingeben, ggf. Passwort/2FA.\n"
               "• <b>Passwort/2FA:</b> in Einstellungen aktivieren/deaktivieren.\n"
               "• <b>Support:</b> schreibe dein Anliegen, Admins melden sich.\n",
  # Admin
  "admin_title":"🛠️ <b>Adminbereich</b>\nGebühren gesamt: {fee}\nNutzer gesamt: {users}\nAktive Nutzer (30T): {active}\nEscrow gehalten: {held}\n",
  "admin_btn_edit_balance":"✏️ Guthaben ändern",
  "admin_edit_prompt":"Sende: <code>UserID Betrag</code> (z. B. <code>123456 0.5</code> für +0.5 SOL; negative Werte für Abzug).",
  "admin_edit_ok":"✅ Guthaben angepasst. Neuer Stand: {av}",
  "admin_edit_err":"❌ Eingabe ungültig oder Nutzer nicht gefunden."
 },
 "en":{
  "welcome":"👋 Welcome to <b>ProofPay</b>\n\nSecure crypto payments in Telegram — fast, low-fee, with seller protection.\n\nChoose an action:",
  "menu":"🏠 <b>Main Menu</b>\n\n• 💰 Balance\n• ➕ Deposit (SOL) – from your <b>source wallet</b>\n• 📤 Send — F&F or 🛡️ Escrow\n• ➖ Withdraw — On-Chain\n• 🧾 History\n• ⚙️ Settings — Language & 2FA\n• 🆘 Support",
  "btn_balance":"💰 Balance", "btn_deposit":"➕ Deposit", "btn_send":"📤 Send",
  "btn_withdraw":"➖ Withdraw", "btn_history":"🧾 History", "btn_settings":"⚙️ Settings", "btn_support":"🆘 Support",
  "btn_about":"ℹ️ About", "btn_policies":"📜 Policies", "btn_help":"❓ Help", "btn_admin":"🛠️ Admin",
  "balance":"<b>Your Balance</b>\n{lines}\n\n📫 <b>Deposit address:</b>\n<code>{addr}</code>\n\nℹ️ Send SOL from a <b>source wallet you’ve provided</b>. We auto-credit once confirmed.",
  "line":"• {asset}: Available <b>{av}</b> | Held <b>{hd}</b>",
  "deposit_ask_source":"➕ <b>Deposit (SOL)</b>\n\nPlease send the <b>sender wallet address</b> (your SOL address) you will deposit from.",
  "deposit_source_ok":"✅ Source saved:\n<code>{src}</code>\n\nNow send SOL to our address:\n<code>{addr}</code>\n\nMin: {min}\nWe’ll scan on-chain and credit only if the payment comes from your source.",
  "send_who":"📤 <b>Send</b>\nWho do you want to pay? Reply with <code>@username</code>.",
  "send_amt":"Receiver: <b>@{u}</b>\nEnter amount, e.g. <code>0.25</code> (Asset: SOL).",
  "send_mode":"Amount: <b>{amt}</b> SOL\nPick a mode:",
  "mode_fnf":"👥 Friends & Family",
  "mode_escrow":"🛡️ Seller Protection (Escrow)",
  "sent_fnf_sender":"✅ Sent to @{u}: {amt} SOL (F&F) – Fee {fee}%",
  "sent_fnf_recv":"📥 You received {amt} SOL from @{u} (F&F).",
  "escrow_hold_s":"🛡️ Sent to @{u}: {amt} SOL — <b>held</b>.",
  "escrow_hold_r":"🛡️ {amt} SOL from @{u} received — <b>held</b>.",
  "escrow_btn_release":"✅ Item received → Release",
  "escrow_btn_dispute":"❗ Open dispute",
  "escrow_release_ok":"✅ Escrow released.",
  "escrow_dispute_open":"⚠️ Dispute opened. Admin notified.",
  "withdraw_addr":"➖ <b>Withdraw</b>\nSend target address (SOL, Base58). Minimum: {min}",
  "withdraw_amt":"Enter amount in SOL (min {min}, max {max}).",
  "withdraw_ok":"💸 Withdrawal created: {amt} SOL\nTx: <code>{sig}</code>",
  "history_none":"(No transactions yet.)",
  "settings":"⚙️ <b>Settings</b>\n• Language: <b>{lang}</b>\n• 2FA: <b>{twofa}</b>\n• Your referral code: <code>{ref}</code>\n• Password lock: <b>{pw}</b>",
  "twofa_toggled":"🔐 2FA is now: {st}",
  "support_prompt":"🆘 Describe your issue. We’ll reply here.",
  "deposit_booked":"✅ Deposit credited: +{amt} SOL\nTx: <code>{sig}</code>",
  "err_rpc":"RPC overloaded. Try again shortly.",
  "err_amt":"Invalid amount.",
  "err_balance":"Insufficient balance. Available: {av}",
  "err_addr":"Invalid SOL address.",
  "err_src":"That’s not a valid Solana address.",
  # PW
  "set_pw_prompt":"🔑 Send your <b>new password</b> (min 4 chars).",
  "set_pw_confirm":"Confirm your password — send it <b>again</b>.",
  "set_pw_ok":"✅ Password set. It will be required for <b>Send</b> and <b>Withdraw</b>.",
  "del_pw_ok":"✅ Password protection removed.",
  "pw_mismatch":"❌ Passwords do not match. Cancelled.",
  "pw_short":"❌ Password too short.",
  "pw_ask":"🔑 Please enter your password:",
  "pw_wrong":"❌ Wrong password.",
  # About/Policies/Help
  "about_text":"ℹ️ <b>About us</b>\n"
               "1) ProofPay enables secure crypto payments in Telegram.\n"
               "2) We rely on escrow for buyer protection.\n"
               "3) Deposits are detected on-chain automatically.\n"
               "4) Withdrawals are sent directly from the hot wallet.\n"
               "5) 2FA and password protection are available.\n"
               "6) Fees are transparent and fair.\n"
               "7) We use reliable RPC endpoints.\n"
               "8) We store only minimal data needed for service.\n"
               "9) Disputes notify admins for review.\n"
               "10) Referral rewards inviting new users.\n"
               "11) We constantly improve stability and security.\n"
               "12) Licenses/registrations as required by local law.",
  "policies_text":"📜 <b>Policies</b>\n\n"
                  "<b>Terms (summary)</b>\n"
                  "• Use at your own risk. Fees as displayed.\n"
                  "• Fraud/illegal use is prohibited.\n"
                  "• We may suspend accounts upon violations.\n\n"
                  "<b>Privacy</b>\n"
                  "• We store only data needed to operate (user id, balances, tx logs).\n"
                  "• No data sharing with third parties unless legally required.\n"
                  "• You can request access/erasure per applicable law.\n\n"
                  "<b>Risk Disclosure</b>\n"
                  "• Crypto assets are volatile; transactions are irreversible.\n"
                  "• On-chain risks (network, fees, user errors) lie with the user.\n"
                  "• Use escrow only with trusted counterparties; release only after receipt.\n",
  "help_text":"❓ <b>Help</b>\n"
              "• <b>Deposit:</b> add source wallet, then send to the deposit address.\n"
              "• <b>Send:</b> enter @username, amount, choose F&F/Escrow.\n"
              "• <b>Escrow:</b> funds are held; buyer can release or open dispute.\n"
              "• <b>Withdraw:</b> enter address, amount, then password/2FA if enabled.\n"
              "• <b>Password/2FA:</b> toggle in Settings.\n"
              "• <b>Support:</b> write your issue, admins will reply.\n",
  # Admin
  "admin_title":"🛠️ <b>Admin</b>\nTotal fees: {fee}\nUsers: {users}\nActive users (30d): {active}\nEscrow held: {held}\n",
  "admin_btn_edit_balance":"✏️ Edit balance",
  "admin_edit_prompt":"Send: <code>UserID Amount</code> (e.g. <code>123456 0.5</code> for +0.5 SOL; negative for debit).",
  "admin_edit_ok":"✅ Balance updated. New available: {av}",
  "admin_edit_err":"❌ Invalid input or user not found."
 }
}

def T(uid, key, **kw):
    try:
        u = get_user(uid)
        lang = (u["lang"] if u else DEFAULT_LANG) or DEFAULT_LANG
        if not isinstance(lang, str):
            lang = DEFAULT_LANG
        lang = lang.lower()
        if lang not in I18N:
            lang = DEFAULT_LANG if DEFAULT_LANG in I18N else "en"
        texts = I18N.get(lang, I18N.get("en", {}))
        template = texts.get(key, I18N.get("en", {}).get(key, key))
        return template.format(**kw)
    except Exception:
        return I18N.get("en", {}).get(key, key)

# ------------------ UI ----------------------
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

def menu(uid):
    u = get_user(uid)
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(I18N[u["lang"]]["btn_balance"], callback_data="m:bal"))
    kb.add(
        InlineKeyboardButton(I18N[u["lang"]]["btn_deposit"], callback_data="m:dep"),
        InlineKeyboardButton(I18N[u["lang"]]["btn_send"],    callback_data="m:send")
    )
    kb.add(
        InlineKeyboardButton(I18N[u["lang"]]["btn_withdraw"], callback_data="m:wd"),
        InlineKeyboardButton(I18N[u["lang"]]["btn_history"],  callback_data="m:hist")
    )
    kb.add(
        InlineKeyboardButton(I18N[u["lang"]]["btn_settings"], callback_data="m:set"),
        InlineKeyboardButton(I18N[u["lang"]]["btn_support"],  callback_data="m:sup")
    )
    kb.add(
        InlineKeyboardButton(I18N[u["lang"]]["btn_about"],     callback_data="m:about"),
        InlineKeyboardButton(I18N[u["lang"]]["btn_policies"],  callback_data="m:pol")
    )
    kb.add(InlineKeyboardButton(I18N[u["lang"]]["btn_help"], callback_data="m:help"))
    if is_admin(uid):
        kb.add(InlineKeyboardButton(I18N[u["lang"]]["btn_admin"], callback_data="m:admin"))
    return kb

def safe_edit(chat_id, msg_id, text, reply_markup=None):
    try:
        bot.edit_message_text(text, chat_id, msg_id, reply_markup=reply_markup)
    except Exception:
        bot.send_message(chat_id, text, reply_markup=reply_markup)

# ------------------ RPC helper ----------------
def rpc_post(method, params):
    tries=0; delay=0.8
    while True:
        r = sess.post(SOL_RPC_URL, json={"jsonrpc":"2.0","id":1,"method":method,"params":params}, timeout=25)
        if r.status_code==429:
            tries+=1
            if tries>6: raise requests.HTTPError("429 Too Many Requests")
            time.sleep(min(delay,5)); delay*=1.6; continue
        r.raise_for_status()
        j=r.json()
        if "error" in j:
            if j["error"].get("code") in (-32005,):
                tries+=1
                if tries>6: raise RuntimeError(j["error"])
                time.sleep(min(delay,5)); delay*=1.6; continue
            raise RuntimeError(j["error"])
        return j["result"]

def get_sigs_for(addr, limit=50):
    return rpc_post("getSignaturesForAddress", [addr, {"limit": limit}]) or []

def get_tx(sig):
    return rpc_post("getTransaction", [sig, {"encoding":"jsonParsed","maxSupportedTransactionVersion":0}])

# ------------------ Commands ------------------
@bot.message_handler(commands=["start"])
def start(m):
    ref_by=None
    if m.text and len(m.text.split())>1:
        code=m.text.split()[1].strip()
        if code.startswith("R") and code[1:].isdigit():
            ref_by=int(code[1:]);  ref_by = None if ref_by==m.from_user.id else ref_by
    ensure_user(m.from_user, ref_by=ref_by)
    bot.reply_to(m, T(m.from_user.id,"welcome"), reply_markup=menu(m.from_user.id))

@bot.message_handler(commands=["menu"])
def cmd_menu(m):
    ensure_user(m.from_user)
    bot.reply_to(m, T(m.from_user.id,"menu"), reply_markup=menu(m.from_user.id))

# ------------------ DEPOSIT FLOW (Quelle-Wallet) ----------------
def is_valid_pubkey(addr:str)->bool:
    return bool(re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", addr or ""))

def on_deposit_source(uid, m):
    src = (m.text or "").strip()
    if not is_valid_pubkey(src):
        bot.reply_to(m, T(uid,"err_src")); return
    conn.execute("INSERT OR IGNORE INTO expected_sources(user_id,source_addr,created_at) VALUES(?,?,?)",
                 (uid, src, now_iso()))
    conn.commit()
    bot.reply_to(m, T(uid,"deposit_source_ok", src=src, addr=CENTRAL_WALLET_ADDRESS, min=f"{MIN_DEPOSIT_SOL} SOL"),
                 reply_markup=menu(uid))

def credit_deposit(uid, sol_amt, sig):
    bal_adj(uid, "SOL", da=Decimal(sol_amt))
    conn.execute("INSERT INTO tx_log(id,type,user_from,user_to,asset,amount,fee,chain_sig,meta,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                 (str(uuid.uuid4()),"deposit",uid,None,"SOL",float(sol_amt),0,str(sig),"{}",now_iso()))
    conn.commit()
    try: bot.send_message(uid, T(uid,"deposit_booked", amt=str(sol_amt), sig=sig))
    except: pass

def is_sol_deposit_from_source(tx_result, source_addr: str, dest_addr: str, min_sol: Decimal):
    try:
        meta    = tx_result.get("meta", {})
        txn     = tx_result.get("transaction", {})
        message = txn.get("message", {})
        for inst in message.get("instructions", []):
            if inst.get("program") == "system" and inst.get("parsed", {}).get("type") == "transfer":
                info = inst["parsed"]["info"]
                if info.get("source")==source_addr and info.get("destination")==dest_addr:
                    lamports = int(info.get("lamports", 0))
                    sol = Decimal(lamports) / Decimal(1_000_000_000)
                    if sol >= min_sol:
                        return True, dquant(sol, 9)
        keys = message.get("accountKeys", [])
        if source_addr in [k.get("pubkey") if isinstance(k, dict) else k for k in keys] and \
           dest_addr   in [k.get("pubkey") if isinstance(k, dict) else k for k in keys]:
            flat = [k.get("pubkey") if isinstance(k, dict) else k for k in keys]
            i_src = flat.index(source_addr)
            i_dst = flat.index(dest_addr)
            pre  = meta.get("preBalances", [])
            post = meta.get("postBalances", [])
            if i_src < len(pre) and i_src < len(post) and i_dst < len(pre) and i_dst < len(post):
                delta_src = (pre[i_src] - post[i_src]) / 1_000_000_000
                delta_dst = (post[i_dst] - pre[i_dst]) / 1_000_000_000
                if delta_src > 0 and abs(delta_src - delta_dst) < 1e-9 and delta_dst >= float(min_sol):
                    return True, dquant(Decimal(delta_dst), 9)
    except Exception:
        pass
    return False, Decimal("0")

def scan_deposits_loop():
    print("Deposit-Scanner gestartet.")
    while True:
        try:
            sigs = get_sigs_for(CENTRAL_WALLET_ADDRESS, limit=60)
            rows = conn.execute("SELECT user_id, source_addr FROM expected_sources").fetchall()
            expected = {}
            for r in rows:
                expected.setdefault(r["source_addr"], set()).add(r["user_id"])

            for s in sigs:
                sig = s.get("signature") or s.get("sig")
                if not sig: continue
                if conn.execute("SELECT 1 FROM deposit_seen WHERE sig=?", (sig,)).fetchone(): continue

                tx = None
                for attempt in range(5):
                    try:
                        tx = get_tx(sig); break
                    except Exception as e:
                        if "429" in str(e): time.sleep(0.6*(attempt+1)); continue
                        tx = None; break
                if not tx:
                    conn.execute("INSERT OR IGNORE INTO deposit_seen(sig) VALUES(?)", (sig,)); conn.commit()
                    continue

                result = tx
                ok_src = None; amt_sol = Decimal("0")
                try:
                    msg = result.get("transaction", {}).get("message", {})
                    for inst in msg.get("instructions", []):
                        if inst.get("program")=="system" and inst.get("parsed",{}).get("type")=="transfer":
                            info=inst["parsed"]["info"]
                            src = info.get("source"); dst=info.get("destination")
                            if dst == CENTRAL_WALLET_ADDRESS and src in expected:
                                ok, a = is_sol_deposit_from_source(result, src, CENTRAL_WALLET_ADDRESS, MIN_DEPOSIT_SOL)
                                if ok: ok_src = src; amt_sol = a; break
                except Exception:
                    pass

                if ok_src:
                    for uid in expected.get(ok_src, []):
                        credit_deposit(uid, amt_sol, sig)

                conn.execute("INSERT OR IGNORE INTO deposit_seen(sig) VALUES(?)", (sig,))
                conn.commit()

        except requests.HTTPError:
            time.sleep(3)
        except Exception as e:
            print("Scan-Fehler:", e)
        time.sleep(DEPOSIT_POLL_SECONDS)

# ------------------ SEND FLOW -------------------
def send_who(m):
    u = (m.text or "").strip().lstrip("@")
    row = get_user_by_username(u)
    if not row:
        bot.reply_to(m, f"@{u} nicht gefunden. (Empfänger muss /start ausführen.)"); return
    if row["user_id"]==m.from_user.id:
        bot.reply_to(m, "Du kannst dir selbst nichts senden."); return
    msg = bot.reply_to(m, T(m.from_user.id,"send_amt", u=row["username"] or row["user_id"]))
    bot.register_next_step_handler(msg, lambda x: send_amount(x, row["user_id"], row["username"] or str(row["user_id"])))

def send_amount(m, to_uid, to_uname):
    try:
        amt = Decimal((m.text or "").replace(",",".").strip())
        if amt<=0: raise ValueError
    except Exception:
        bot.reply_to(m, T(m.from_user.id,"err_amt")); return
    av,_ = bal(m.from_user.id, "SOL")
    if amt>av:
        bot.reply_to(m, T(m.from_user.id,"err_balance", av=fmt("SOL",av))); return
    kb=InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(I18N[get_user(m.from_user.id)["lang"]]["mode_fnf"],
                                callback_data=f"send:go:FNF:{to_uid}:{to_uname}:{amt}"))
    kb.add(InlineKeyboardButton(I18N[get_user(m.from_user.id)["lang"]]["mode_escrow"],
                                callback_data=f"send:go:ESCROW:{to_uid}:{to_uname}:{amt}"))
    bot.reply_to(m, T(m.from_user.id,"send_mode", amt=str(amt)), reply_markup=kb)

def _ask_password_then(fn, *args):
    """Wenn Passwort aktiv: abfragen; sonst direkt weiter."""
    chat_id = args[0] if args else None
    uid = args[1] if len(args)>1 else None
    if user_has_password(uid):
        msg = bot.send_message(chat_id, T(uid, "pw_ask"))
        def check_pw(m):
            if not verify_password(uid, (m.text or "")):
                bot.reply_to(m, T(uid,"pw_wrong")); return
            fn(*args)
        bot.register_next_step_handler(msg, check_pw)
    else:
        fn(*args)

def do_send(chat_id, from_uid, to_uid, to_uname, amt, mode):
    av,_=bal(from_uid, "SOL")
    if amt>av:
        bot.send_message(chat_id, T(from_uid,"err_balance", av=fmt("SOL",av))); return
    fee_percent = FEE_FNF + (FEE_ESCROW_EXTRA if mode=="ESCROW" else Decimal("0"))
    fee = dquant(amt * fee_percent / Decimal("100"), 9)
    net = dquant(amt - fee, 9)
    bal_adj(from_uid, "SOL", da=-amt)
    bal_adj(0, "SOL", da=fee)
    if mode=="FNF":
        bal_adj(to_uid, "SOL", da=net)
        conn.execute("INSERT INTO tx_log VALUES(?,?,?,?,?,?,?,?,?,?)",
                     (str(uuid.uuid4()),"send",from_uid,to_uid,"SOL",float(net),float(fee),None,"{}",now_iso()))
        bot.send_message(chat_id, T(from_uid,"sent_fnf_sender", u=(get_username(to_uid)), amt=str(net), fee=str(fee_percent)))
        bot.send_message(to_uid,   T(to_uid,"sent_fnf_recv",   u=(get_username(from_uid)), amt=str(net)))
    else:
        bal_adj(to_uid, "SOL", dh=net)
        t_id=str(uuid.uuid4())
        conn.execute("INSERT INTO tx_log VALUES(?,?,?,?,?,?,?,?,?,?)",
                     (t_id,"escrow_hold",from_uid,to_uid,"SOL",float(net),float(fee),None,"{}",now_iso()))
        kb=InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(T(from_uid,"escrow_btn_release"), callback_data=f"esc:release:{t_id}"),
               InlineKeyboardButton(T(from_uid,"escrow_btn_dispute"), callback_data=f"esc:dispute:{t_id}"))
        bot.send_message(chat_id, T(from_uid,"escrow_hold_s", u=get_username(to_uid), amt=str(net)), reply_markup=kb)
        bot.send_message(to_uid, T(to_uid,"escrow_hold_r", u=get_username(from_uid), amt=str(net)))
    conn.commit()

# 2FA-Callbacks & Support & Neues im gemeinsamen Handler
@bot.callback_query_handler(func=lambda c: True)
def on_cb(c):
    ensure_user(c.from_user)
    data=c.data or ""

    if data.startswith("send:go:"):
        _,_,mode,to_uid,to_uname,amt = data.split(":")
        to_uid=int(to_uid); amt=Decimal(amt)
        u=get_user(c.from_user.id)

        def proceed_after_pw():
            if u["twofa_enabled"]:
                code=''.join(random.choices(string.digits,k=6))
                bot.answer_callback_query(c.id, f"🔐 Code: {code}")
                bot.send_message(c.message.chat.id, f"🔐 2FA – antworte mit: <code>{code}</code>")
                def check_code(m):
                    if (m.text or "").strip()!=code: bot.reply_to(m,"Falscher Code."); return
                    do_send(m.chat.id, c.from_user.id, to_uid, to_uname, amt, mode)
                bot.register_next_step_handler(c.message, check_code)
            else:
                do_send(c.message.chat.id, c.from_user.id, to_uid, to_uname, amt, mode)

        _ask_password_then(lambda chat_id, uid, *_: proceed_after_pw(), c.message.chat.id, c.from_user.id)
        return

    if data=="m:sup":
        msg=bot.send_message(c.message.chat.id, T(c.from_user.id,"support_prompt"))
        bot.register_next_step_handler(msg, sup_msg)
        return

    if data=="m:about":
        bot.send_message(c.message.chat.id, T(c.from_user.id,"about_text"))
        return

    if data=="m:pol":
        bot.send_message(c.message.chat.id, T(c.from_user.id,"policies_text"))
        return

    if data=="m:help":
        bot.send_message(c.message.chat.id, T(c.from_user.id,"help_text"))
        return

    if data=="m:admin" and is_admin(c.from_user.id):
        # Stats
        fee = conn.execute("SELECT COALESCE(SUM(fee),0) AS s FROM tx_log").fetchone()["s"]
        users = conn.execute("SELECT COUNT(*) AS c FROM users WHERE user_id>0").fetchone()["c"]
        since = (datetime.utcnow() - timedelta(days=30)).isoformat()
        active = conn.execute("""SELECT COUNT(DISTINCT u) AS c FROM (
                                   SELECT user_from AS u, created_at FROM tx_log
                                   UNION ALL
                                   SELECT user_to AS u, created_at FROM tx_log
                                 ) WHERE u IS NOT NULL AND u>0 AND datetime(created_at)>datetime(?)
                              """, (since,)).fetchone()["c"]
        held = conn.execute("SELECT COALESCE(SUM(held),0) AS h FROM balances WHERE asset='SOL' AND user_id>0").fetchone()["h"]
        txt = T(c.from_user.id,"admin_title", fee=f"{Decimal(str(fee)):.9f} SOL", users=users, active=active, held=f"{Decimal(str(held)):.9f} SOL")
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(T(c.from_user.id,"admin_btn_edit_balance"), callback_data="admin:editbal"))
        safe_edit(c.message.chat.id, c.message.message_id, txt, reply_markup=kb)
        return

    if data=="admin:editbal" and is_admin(c.from_user.id):
        msg = bot.send_message(c.message.chat.id, T(c.from_user.id,"admin_edit_prompt"))
        def take_edit(m):
            try:
                parts = (m.text or "").strip().split()
                uid = int(parts[0]); amount = Decimal(parts[1])
                # ensure user & asset
                r = get_user(uid)
                if not r: raise ValueError("no user")
                bal_adj(uid, "SOL", da=amount)
                av,_ = bal(uid, "SOL")
                bot.reply_to(m, T(c.from_user.id,"admin_edit_ok", av=fmt("SOL", av)))
            except Exception:
                bot.reply_to(m, T(c.from_user.id,"admin_edit_err"))
        bot.register_next_step_handler(msg, take_edit)
        return

    if data in ("m:home","m:bal"):
        lines=[]
        for a in ASSETS:
            av,hd = bal(c.from_user.id,a)
            lines.append(T(c.from_user.id,"line", asset=a, av=fmt(a,av), hd=fmt(a,hd)))
        txt=T(c.from_user.id,"balance", lines="\n".join(lines), addr=CENTRAL_WALLET_ADDRESS)
        safe_edit(c.message.chat.id, c.message.message_id, txt, reply_markup=menu(c.from_user.id))

    elif data=="m:dep":
        msg = bot.send_message(c.message.chat.id, T(c.from_user.id,"deposit_ask_source"))
        bot.register_next_step_handler(msg, lambda x: on_deposit_source(c.from_user.id, x))

    elif data=="m:send":
        msg=bot.send_message(c.message.chat.id, T(c.from_user.id,"send_who"))
        bot.register_next_step_handler(msg, send_who)

    elif data=="m:hist":
        rows = conn.execute("SELECT * FROM tx_log WHERE user_from=? OR user_to=? ORDER BY datetime(created_at) DESC LIMIT 20",
                            (c.from_user.id, c.from_user.id)).fetchall()
        if not rows:
            txt = T(c.from_user.id,"history_none")
        else:
            out=["🧾 <b>Verlauf</b>"]
            for r in rows:
                meta = json.loads(r["meta"] or "{}")
                sig = r["chain_sig"] or "-"
                if r["type"]=="deposit":
                    out.append(f"➕ {fmt(r['asset'], r['amount'])} | tx: <code>{sig}</code>")
                elif r["type"]=="send":
                    other = r["user_to"]
                    out.append(f"📤 {fmt(r['asset'], r['amount'])} → @{get_username(other)} (fee {Decimal(str(r['fee']))}%)")
                elif r["type"]=="escrow_hold":
                    other = r["user_to"]
                    out.append(f"🛡️ HOLD {fmt(r['asset'], r['amount'])} → @{get_username(other)} (fee {Decimal(str(r['fee']))}%)")
                elif r["type"]=="escrow_release":
                    other = r["user_to"]
                    out.append(f"✅ RELEASE {fmt(r['asset'], r['amount'])} → @{get_username(other)}")
                elif r["type"]=="withdraw":
                    out.append(f"💸 {fmt(r['asset'], r['amount'])} → {meta.get('to','addr')} | tx: <code>{sig}</code>")
            txt="\n".join(out)
        safe_edit(c.message.chat.id, c.message.message_id, txt, reply_markup=menu(c.from_user.id))

    elif data=="m:wd":
        msg=bot.send_message(c.message.chat.id, T(c.from_user.id,"withdraw_addr", min=f"{MIN_WITHDRAW_SOL} SOL"))
        bot.register_next_step_handler(msg, wd_addr)

    elif data=="m:set":
        u=get_user(c.from_user.id); twofa="AN" if u["twofa_enabled"] else "AUS"
        pwst = "AN" if u["pass_enabled"] else "AUS"
        txt=T(c.from_user.id,"settings", lang=u["lang"], twofa=twofa, ref=u["ref_code"], pw=pwst)
        kb=InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🔐 2FA AN/AUS", callback_data="set:2fa"))
        kb.add(InlineKeyboardButton("🔑 Passwort setzen/ändern", callback_data="set:pw"),
               InlineKeyboardButton("🗑️ Passwort entfernen", callback_data="set:pwdel"))
        kb.add(InlineKeyboardButton("🌐 Deutsch", callback_data="set:lang:de"),
               InlineKeyboardButton("🌐 English", callback_data="set:lang:en"))
        kb.add(InlineKeyboardButton("⬅️ Back", callback_data="m:home"))
        safe_edit(c.message.chat.id, c.message.message_id, txt, reply_markup=kb)

    elif data=="set:2fa":
        u=get_user(c.from_user.id)
        val=0 if u["twofa_enabled"] else 1
        conn.execute("UPDATE users SET twofa_enabled=? WHERE user_id=?", (val, c.from_user.id)); conn.commit()
        bot.answer_callback_query(c.id, T(c.from_user.id,"twofa_toggled", st=("AN" if val else "AUS")))
        on_cb(type("obj",(),{"data":"m:set","from_user":c.from_user,"message":c.message,"id":c.id}))

    elif data=="set:pw":
        uid=c.from_user.id
        msg=bot.send_message(c.message.chat.id, T(uid,"set_pw_prompt"))
        def first(m):
            pw1=(m.text or "")
            if len(pw1)<4: bot.reply_to(m, T(uid,"pw_short")); return
            msg2=bot.reply_to(m, T(uid,"set_pw_confirm"))
            def second(m2):
                pw2=(m2.text or "")
                if pw2!=pw1: bot.reply_to(m2, T(uid,"pw_mismatch")); return
                salt=secrets.token_hex(16)
                h=_hash_pw(pw1, salt)
                conn.execute("UPDATE users SET pass_enabled=1, pw_hash=?, pw_salt=? WHERE user_id=?",(h,salt,uid)); conn.commit()
                bot.reply_to(m2, T(uid,"set_pw_ok"), reply_markup=menu(uid))
            bot.register_next_step_handler(msg2, second)
        bot.register_next_step_handler(msg, first)

    elif data=="set:pwdel":
        uid=c.from_user.id
        conn.execute("UPDATE users SET pass_enabled=0, pw_hash=NULL, pw_salt=NULL WHERE user_id=?", (uid,))
        conn.commit()
        bot.answer_callback_query(c.id, T(uid,"del_pw_ok"))
        on_cb(type("obj",(),{"data":"m:set","from_user":c.from_user,"message":c.message,"id":c.id}))

    elif data.startswith("set:lang:"):
        lang=data.split(":")[2]
        if lang in ("de","en"):
            conn.execute("UPDATE users SET lang=? WHERE user_id=?", (lang, c.from_user.id)); conn.commit()
        on_cb(type("obj",(),{"data":"m:set","from_user":c.from_user,"message":c.message,"id":c.id}))

    elif data.startswith("esc:release:"):
        t_id=data.split(":")[2]
        tr = conn.execute("SELECT * FROM tx_log WHERE id=? AND type='escrow_hold'", (t_id,)).fetchone()
        if not tr or tr["user_from"]!=c.from_user.id:
            bot.answer_callback_query(c.id, "Nicht zulässig.", show_alert=True); return
        amt=Decimal(str(tr["amount"])); asset=tr["asset"]; seller=tr["user_to"]
        av,hd = bal(seller, asset)
        if hd < amt:
            bot.answer_callback_query(c.id, "Fehler: nicht genug gehalten.", show_alert=True); return
        bal_set(seller, asset, av+amt, hd-amt)
        conn.execute("INSERT INTO tx_log(id,type,user_from,user_to,asset,amount,fee,chain_sig,meta,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                     (str(uuid.uuid4()),"escrow_release",tr["user_from"],tr["user_to"],asset,float(amt),0,None,"{}",now_iso()))
        conn.commit()
        bot.answer_callback_query(c.id, T(c.from_user.id,"escrow_release_ok"))
        bot.send_message(tr["user_to"], "✅ Betrag aus Escrow freigegeben.")

    elif data.startswith("esc:dispute:"):
        t_id=data.split(":")[2]
        tr = conn.execute("SELECT * FROM tx_log WHERE id=? AND type='escrow_hold'", (t_id,)).fetchone()
        if not tr:
            bot.answer_callback_query(c.id, "Nicht zulässig.", show_alert=True); return
        for a in ADMIN_IDS:
            bot.send_message(a, f"⚠️ Dispute: BUYER @{get_username(tr['user_from'])} vs SELLER @{get_username(tr['user_to'])} | {fmt(tr['asset'],tr['amount'])}\nTxID: {t_id}")
        bot.answer_callback_query(c.id, T(c.from_user.id,"escrow_dispute_open"))

# ------------------ WITHDRAW (echte On-Chain) --------------
def _extract_sig(resp):
    try:
        if isinstance(resp, str):
            return resp
        if hasattr(resp, "value"):
            v = getattr(resp, "value")
            if isinstance(v, str) and len(v) > 40:
                return v
        if isinstance(resp, dict):
            if isinstance(resp.get("result"), str):
                return resp["result"]
            if isinstance(resp.get("value"), str):
                return resp["value"]
            if isinstance(resp.get("result"), dict):
                inner = resp["result"]
                if isinstance(inner.get("value"), str):
                    return inner["value"]
        return str(resp)
    except Exception:
        return str(resp)

def _get_blockhash_robust():
    rb = rpc.get_recent_blockhash()
    if isinstance(rb, dict):
        if "blockhash" in rb: return rb["blockhash"]
        if "result" in rb and isinstance(rb["result"], dict):
            val = rb["result"].get("value", {})
            if isinstance(val, dict) and "blockhash" in val:
                return val["blockhash"]
        if "value" in rb and isinstance(rb["value"], dict) and "blockhash" in rb["value"]:
            return rb["value"]["blockhash"]
    if hasattr(rb, "value"):
        v = getattr(rb, "value")
        if isinstance(v, str): return v
        if isinstance(v, dict) and "blockhash" in v: return v["blockhash"]
        if hasattr(v, "blockhash"): return getattr(v, "blockhash")
    try:
        lr = rpc_post("getLatestBlockhash", [{}])
        if isinstance(lr, dict):
            v = lr.get("value", {})
            if isinstance(v, dict):
                bh = v.get("blockhash") or (v.get("blockhashes", [None])[0] if isinstance(v.get("blockhashes"), list) else None)
                if bh: return bh
    except Exception:
        pass
    raise RuntimeError("Kein Blockhash vom RPC erhalten")

def withdraw_sol(to_addr: str, sol_amt: Decimal) -> str:
    if not re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", to_addr or ""):
        raise ValueError("invalid address")
    to_pk = PublicKey(to_addr)
    ix = transfer(TransferParams(
        from_pubkey=kp.public_key,
        to_pubkey=to_pk,
        lamports=int(sol_amt * Decimal(1_000_000_000))
    ))
    tx = Transaction(fee_payer=kp.public_key)
    tx.add(ix)
    tx.recent_blockhash = _get_blockhash_robust()
    tx.sign(kp)
    raw = tx.serialize()
    resp = rpc.send_raw_transaction(raw, opts=TxOpts(skip_preflight=False, max_retries=5))
    sig = _extract_sig(resp)
    try:
        rpc.confirm_transaction(sig)
    except Exception:
        pass
    return sig

def wd_addr(m):
    addr=(m.text or "").strip()
    if not is_valid_pubkey(addr):
        bot.reply_to(m, T(m.from_user.id,"err_addr")); return
    msg=bot.reply_to(m, T(m.from_user.id,"withdraw_amt", min=f"{MIN_WITHDRAW_SOL} SOL", max=f"{MAX_WITHDRAW_SOL} SOL"))
    bot.register_next_step_handler(msg, lambda x: wd_amount(x, addr))

def wd_amount(m, to_addr):
    try:
        amt = Decimal((m.text or "").replace(",",".").strip())
        if amt<=0 or amt<MIN_WITHDRAW_SOL or amt>MAX_WITHDRAW_SOL: raise ValueError
    except Exception:
        bot.reply_to(m, T(m.from_user.id,"err_amt")); return
    av,_=bal(m.from_user.id, "SOL")
    if amt>av:
        bot.reply_to(m, T(m.from_user.id,"err_balance", av=fmt("SOL",av))); return

    u=get_user(m.from_user.id)

    def finalize_send():
        try:
            sig = withdraw_sol(to_addr, dquant(amt,9))
        except requests.HTTPError:
            bot.reply_to(m, T(m.from_user.id,"err_rpc")); return
        except Exception as e:
            bot.reply_to(m, f"Auszahlung fehlgeschlagen: {e}"); return
        bal_adj(m.from_user.id, "SOL", da=-amt)
        meta={"to": to_addr}
        conn.execute("INSERT INTO tx_log VALUES(?,?,?,?,?,?,?,?,?,?)",
                     (str(uuid.uuid4()),"withdraw",m.from_user.id,None,"SOL",float(amt),0,str(sig),json.dumps(meta),now_iso()))
        conn.commit()
        bot.reply_to(m, T(m.from_user.id,"withdraw_ok", amt=str(amt), sig=str(sig)), reply_markup=menu(m.from_user.id))

    def after_pw():
        if u["twofa_enabled"]:
            code=''.join(random.choices(string.digits,k=6))
            bot.reply_to(m, f"🔐 2FA – antworte mit: <code>{code}</code>")
            def check_code(x):
                if (x.text or "").strip()!=code: bot.reply_to(x,"Falscher Code."); return
                finalize_send()
            bot.register_next_step_handler(m, check_code)
        else:
            finalize_send()

    # zuerst Passwort prüfen (falls aktiv)
    if user_has_password(m.from_user.id):
        msg_pw = bot.reply_to(m, T(m.from_user.id,"pw_ask"))
        def check_pw(x):
            if not verify_password(m.from_user.id, (x.text or "")):
                bot.reply_to(x, T(m.from_user.id,"pw_wrong")); return
            after_pw()
        bot.register_next_step_handler(msg_pw, check_pw)
    else:
        after_pw()

# ------------------ SUPPORT -------------------
def sup_msg(m):
    txt=m.text or "(ohne Text)"
    for a in ADMIN_IDS:
        try:
            bot.send_message(a, f"🆘 Support von @{get_username(m.from_user.id)} ({m.from_user.id}):\n\n{txt}")
        except: pass
    bot.reply_to(m, "Danke! Wir melden uns hier im Chat.", reply_markup=menu(m.from_user.id))

# ------------------ FALLBACK ------------------
@bot.message_handler(content_types=["text","photo","document","sticker","video","audio","voice"])
def any_msg(m):
    ensure_user(m.from_user)
    bot.send_message(m.chat.id, T(m.from_user.id,"menu"), reply_markup=menu(m.from_user.id))

# ------------------ START SCANNER THREAD ------
def start_threads():
    t=threading.Thread(target=scan_deposits_loop, daemon=True)
    t.start()

# ------------------ MAIN ----------------------
if __name__=="__main__":
    print("Starting ProofPay (SOL live).")
    start_threads()
    bot.infinity_polling(skip_pending=True, timeout=20)