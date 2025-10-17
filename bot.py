# -*- coding: utf-8 -*-
"""
Crypto Pay Telegram Bot – Solana (SOL, USDC, USDT) mit:
- Zentrale Wallet (Custody), Einzahlungen per RPC (gratis), automatische Verbuchung
- Interne Konten (available/held)
- Friends & Family & Verkäuferschutz (Escrow)
- 2FA-Bestätigung (optional)
- Multi-Asset (SOL/USDC/USDT – SPL)
- Sprachumschaltung (de/en)
- Gebührenmodell (percentage + fixed) + eigene Escrow-Gebühr
- Referral-System (Anteil der Plattformgebühr an Werber)
- Auszahlungen AUTOMATISCH on-chain (SOL & SPL) – Bot signiert mit zentralem Secret Key
- Support-Chat & Admin-Panel
- SQLite als Storage
"""

import os, re, json, uuid, random, string, sqlite3, requests, base64
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# ---------- Konfiguration ----------
BOT_TOKEN = os.getenv("BOT_TOKEN", "8222875136:AAFAa9HtRL-g23ganuckjCq5IIW9udQXOZo")
ADMIN_IDS = [123456789]  # int IDs
SOL_RPC_URL = os.getenv("SOL_RPC_URL", "https://api.mainnet-beta.solana.com")

CENTRAL_WALLET_ADDRESS = os.getenv("CENTRAL_WALLET_ADDRESS", "3wyVwpcbWt96mphJjskFsR2qoyafqJuSfGZYmiipW4oy")
# Base58-encoded 64-byte secret (ed25519). Niemals hardcoden – nutze ENV!
CENTRAL_WALLET_SECRET = os.getenv("CENTRAL_WALLET_SECRET", "3sBeqpypNPzPASEYnuoURGCjFHtYnArGvfHos4kBbnCem9xX4X3TU8J51cGEpH7FBoVHF2H99oAwUqBzievSZvRM")

APP_NAME = "KryptoPayBot"
DEFAULT_LANG = "de"
CURRENCY_SOL = "SOL"
ASSETS = {
    "SOL": {"type":"SOL", "decimals":9},
    "USDC": {
        "type":"SPL",
        "mint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "decimals":6
    },
    "USDT": {
        "type":"SPL",
        "mint":"Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
        "decimals":6
    }
}

# Gebühren (Plattform) – Prozent in %, Fixed in Asset-Einheiten (optional)
FEE_PERCENT = 0.6          # auf Transfers (F&F u. Escrow), wird vom Betrag abgezogen
FEE_FIXED = 0.0            # z.B. 0.0005 SOL – pro Transfer (optional)
ESCROW_EXTRA_FEE_PERCENT = 0.2   # zusätzl. Prozent bei Verkäuferschutz
WITHDRAW_FEE_PERCENT = 0.0  # interne Service-Gebühr bei Auszahlung (nicht Network-Fee)
WITHDRAW_FEE_FIXED = 0.0

# Referral – Anteil der eingenommenen Plattformgebühr an den Werber
REFERRAL_REBATE_PERCENT = 25.0  # z.B. 25% der Plattformgebühr gehen an Werber

# ---------- Libraries für Solana Transaktionen ----------
from solana.rpc.api import Client
from solana.publickey import PublicKey
from solana.keypair import Keypair
from solana.transaction import Transaction
from solana.rpc.types import TxOpts
from solana.system_program import TransferParams, transfer
from spl.token.constants import TOKEN_PROGRAM_ID, ASSOCIATED_TOKEN_PROGRAM_ID
from spl.token.instructions import (
    get_associated_token_address, create_associated_token_account, transfer_checked,
)

sol_client = Client(SOL_RPC_URL)

# ---------- Bot ----------
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ---------- DB ----------
DB = "cryptopay_v2.db"
conn = sqlite3.connect(DB, check_same_thread=False)
conn.row_factory = sqlite3.Row

def init_db():
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS users(
      user_id INTEGER PRIMARY KEY,
      username TEXT,
      first_name TEXT,
      last_name TEXT,
      created_at TEXT,
      lang TEXT DEFAULT 'de',
      source_wallet TEXT,
      twofa_enabled INTEGER DEFAULT 1,
      twofa_payload TEXT,
      referrer_id INTEGER
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS balances(
      user_id INTEGER,
      asset TEXT,
      available REAL DEFAULT 0,
      held REAL DEFAULT 0,
      PRIMARY KEY(user_id, asset)
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS deposits(
      id TEXT PRIMARY KEY,
      user_id INTEGER,
      asset TEXT,
      tx_sig TEXT UNIQUE,
      from_address TEXT,
      amount REAL,
      created_at TEXT
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS transfers(
      id TEXT PRIMARY KEY,
      type TEXT,              -- FNF|ESCROW
      asset TEXT,
      from_user INTEGER,
      to_user INTEGER,
      amount REAL,            -- brutto
      fee_taken REAL,         -- gebühr in asset
      status TEXT,            -- completed|held|released|reversed
      created_at TEXT,
      released_at TEXT
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS fees_ledger(
      id TEXT PRIMARY KEY,
      transfer_id TEXT,
      asset TEXT,
      amount REAL,
      created_at TEXT
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS referrals(
      id TEXT PRIMARY KEY,
      transfer_id TEXT,
      referrer_id INTEGER,
      referee_id INTEGER,
      asset TEXT,
      rebate_amount REAL,
      created_at TEXT
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS withdrawals(
      id TEXT PRIMARY KEY,
      user_id INTEGER,
      asset TEXT,
      to_address TEXT,      -- SOL: Pubkey; SPL: user ATA wird automatisch genommen/erstellt
      amount REAL,          -- brutto user-wunsch
      fee_taken REAL,       -- interne service-gebühr
      status TEXT,          -- pending|paid|rejected|error
      tx_sig TEXT,
      error TEXT,
      created_at TEXT,
      updated_at TEXT
    );
    """)
    conn.commit()

init_db()

# ---------- Helpers ----------
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def dquant(x, decimals):
    q = Decimal(10) ** -decimals
    return Decimal(x).quantize(q, rounding=ROUND_DOWN)

def get_user(uid): return conn.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()

def ensure_user(tg_user, ref=None):
    row = get_user(tg_user.id)
    if not row:
        conn.execute("INSERT INTO users(user_id,username,first_name,last_name,created_at,lang,referrer_id) VALUES(?,?,?,?,?,?,?)",
                     (tg_user.id, tg_user.username or "", tg_user.first_name or "", tg_user.last_name or "", now_iso(), DEFAULT_LANG, ref))
        for a in ASSETS.keys():
            conn.execute("INSERT INTO balances(user_id,asset,available,held) VALUES(?,?,0,0)", (tg_user.id, a))
        conn.commit()
    else:
        # update username names
        conn.execute("UPDATE users SET username=?, first_name=?, last_name=? WHERE user_id=?",
                     (tg_user.username or "", tg_user.first_name or "", tg_user.last_name or "", tg_user.id))
        conn.commit()

def get_balance(uid, asset):
    r = conn.execute("SELECT available,held FROM balances WHERE user_id=? AND asset=?", (uid, asset)).fetchone()
    if not r: return Decimal("0"), Decimal("0")
    return Decimal(str(r["available"])), Decimal(str(r["held"]))

def set_balance(uid, asset, avail=None, held=None):
    av, hd = get_balance(uid, asset)
    if avail is None: avail = av
    if held is None: held = hd
    conn.execute("UPDATE balances SET available=?, held=? WHERE user_id=? AND asset=?",
                 (float(avail), float(held), uid, asset))
    conn.commit()

def adj_balance(uid, asset, delta_av=Decimal("0"), delta_hd=Decimal("0")):
    av, hd = get_balance(uid, asset)
    set_balance(uid, asset, av+delta_av, hd+delta_hd)

def fmt_amount(asset, x):
    dec = ASSETS[asset]["decimals"]
    return f"{dquant(x, dec)} {asset}"

def is_admin(uid): return int(uid) in ADMIN_IDS

def gen_code(n=6): return ''.join(random.choices(string.digits, k=n))

def get_user_by_username(username):
    if not username: return None
    u = username.lstrip("@").lower()
    return conn.execute("SELECT * FROM users WHERE lower(username)=?", (u,)).fetchone()

def get_username(uid):
    r = conn.execute("SELECT username FROM users WHERE user_id=?", (uid,)).fetchone()
    return r["username"] if r and r["username"] else str(uid)

# ---------- i18n ----------
I18N = {
    "de": {
        "welcome": "Willkommen bei <b>{app}</b>! Zentrale Wallet:\n<code>{addr}</code>\n\n⚠️ Nur von <b>Phantom/DEX</b> einzahlen – <u>keine</u> Börsen.\n",
        "menu": "Wähle eine Funktion:",
        "btn_balance": "💰 Guthaben",
        "btn_deposit": "➕ Einzahlen",
        "btn_withdraw": "➖ Auszahlen",
        "btn_send": "📤 Senden",
        "btn_history": "🧾 Verlauf",
        "btn_settings": "⚙️ Einstellungen",
        "btn_support": "🆘 Support",
        "balance": "💰 <b>Guthaben</b>\n{lines}",
        "line_balance": "{asset}: Verfügbar <b>{av}</b> | Einbehalten <b>{hd}</b>",
        "deposit_title": "➕ <b>Einzahlen</b> – Asset wählen:",
        "deposit_instr": "Absender-Adresse: <code>{src}</code>\nZentrale Wallet: <code>{central}</code>\n⚠️ Nur Phantom/DEX; keine Börsen!",
        "set_src_prompt": "Sende mir jetzt <b>deine Absender-Wallet</b> (Solana-Adresse).",
        "check_dep_none": "Keine neuen Einzahlungen gefunden.",
        "send_who": "Wem möchtest du senden? Antworte mit <code>@username</code>.",
        "send_amount": "Empfänger: <b>@{u}</b>\nGib Betrag und Asset an, z. B. <code>0.5 SOL</code> oder <code>10 USDC</code>.",
        "send_mode": "Betrag: <b>{amt}</b>\nWähle den Sendemodus:",
        "mode_fnf": "👥 Friends & Family",
        "mode_escrow": "🛡️ Verkäuferschutz",
        "confirm": "Bestätigen?\nEmpfänger: <b>@{to}</b>\nBetrag: <b>{amt}</b>\nModus: <b>{mode}</b>\nGebühr: <b>{fee}</b>",
        "need_src": "Bitte zuerst eine Absender-Adresse in den Einzahlungs-Einstellungen setzen.",
        "withdraw_addr": "➖ <b>Auszahlen</b>\nSende die Ziel-Adresse (Solana-Pubkey). Für USDC/USDT wird das ATA automatisch erzeugt.",
        "withdraw_amount": "Gib Betrag und Asset an (z. B. <code>0.05 SOL</code> oder <code>10 USDT</code>).",
        "withdraw_created": "✅ Auszahlungsauftrag erstellt und wird on-chain ausgeführt.",
        "withdraw_done": "💸 Auszahlung gesendet: <code>{tx}</code>",
        "withdraw_err": "❌ Auszahlung fehlgeschlagen: {e}",
        "settings": "⚙️ <b>Einstellungen</b>\n• Sprache: <b>{lang}</b>\n• 2FA: <b>{twofa}</b>\n• Absender-Adresse: <code>{src}</code>",
        "toggle_2fa": "🔐 2-Stufen-Bestätigung: {state}",
        "lang_switched": "Sprache gesetzt auf: {lang}",
        "support_prompt": "🆘 <b>Support</b>\nSchildere dein Anliegen.",
        "history_none": "🧾 Kein Verlauf.",
        "history_line": "{dir} {amt} mit @{other} | {mode} | {status}",
        "seller_shipped_buyer": "📦 Verkäufer meldet: versendet. Bitte bestätige bei Erhalt.",
        "escrow_held_sender": "🛡️ Gesendet an <b>@{to}</b>: {amt} (einbehalten)",
        "escrow_held_receiver": "🛡️ {amt} von <b>@{from}</b> eingegangen – <b>einbehalten</b>.",
        "fnf_sent_sender": "✅ Gesendet an <b>@{to}</b>: {amt} (Friends & Family)",
        "fnf_sent_receiver": "📥 Du hast {amt} von <b>@{from}</b> erhalten (Friends & Family).",
        "release_ok": "✅ Freigabe erteilt.",
        "dispute_opened": "Dispute eröffnet. Admin informiert.",
        "btn_back": "⬅️ Zurück",
    },
    "en": {
        "welcome": "Welcome to <b>{app}</b>! Central wallet:\n<code>{addr}</code>\n\n⚠️ Deposit only from <b>Phantom/DEX</b> – <u>no</u> exchanges.\n",
        "menu": "Choose an option:",
        "btn_balance": "💰 Balance",
        "btn_deposit": "➕ Deposit",
        "btn_withdraw": "➖ Withdraw",
        "btn_send": "📤 Send",
        "btn_history": "🧾 History",
        "btn_settings": "⚙️ Settings",
        "btn_support": "🆘 Support",
        "balance": "💰 <b>Your Balance</b>\n{lines}",
        "line_balance": "{asset}: Available <b>{av}</b> | Held <b>{hd}</b>",
        "deposit_title": "➕ <b>Deposit</b> – Choose asset:",
        "deposit_instr": "Source address: <code>{src}</code>\nCentral wallet: <code>{central}</code>\n⚠️ Phantom/DEX only; no exchanges!",
        "set_src_prompt": "Send me your <b>source wallet</b> (Solana address).",
        "check_dep_none": "No new deposits found.",
        "send_who": "Who to send? Reply with <code>@username</code>.",
        "send_amount": "Receiver: <b>@{u}</b>\nSend amount and asset, e.g. <code>0.5 SOL</code> or <code>10 USDC</code>.",
        "send_mode": "Amount: <b>{amt}</b>\nChoose sending mode:",
        "mode_fnf": "👥 Friends & Family",
        "mode_escrow": "🛡️ Seller Protection",
        "confirm": "Confirm?\nTo: <b>@{to}</b>\nAmount: <b>{amt}</b>\nMode: <b>{mode}</b>\nFee: <b>{fee}</b>",
        "need_src": "Please set a source address in deposit settings first.",
        "withdraw_addr": "➖ <b>Withdraw</b>\nSend target Solana address. For USDC/USDT ATA will be created automatically.",
        "withdraw_amount": "Send amount and asset (e.g. <code>0.05 SOL</code> or <code>10 USDT</code>).",
        "withdraw_created": "✅ Withdrawal created and will be executed on-chain.",
        "withdraw_done": "💸 Withdrawal sent: <code>{tx}</code>",
        "withdraw_err": "❌ Withdrawal failed: {e}",
        "settings": "⚙️ <b>Settings</b>\n• Language: <b>{lang}</b>\n• 2FA: <b>{twofa}</b>\n• Source address: <code>{src}</code>",
        "toggle_2fa": "🔐 Two-factor: {state}",
        "lang_switched": "Language set to: {lang}",
        "support_prompt": "🆘 <b>Support</b>\nDescribe your issue.",
        "history_none": "🧾 No history.",
        "history_line": "{dir} {amt} with @{other} | {mode} | {status}",
        "seller_shipped_buyer": "📦 Seller says: shipped. Please confirm on delivery.",
        "escrow_held_sender": "🛡️ Sent to <b>@{to}</b>: {amt} (held)",
        "escrow_held_receiver": "🛡️ {amt} from <b>@{from}</b> received – <b>held</b>.",
        "fnf_sent_sender": "✅ Sent to <b>@{to}</b>: {amt} (Friends & Family)",
        "fnf_sent_receiver": "📥 You received {amt} from <b>@{from}</b> (Friends & Family).",
        "release_ok": "✅ Released.",
        "dispute_opened": "Dispute opened. Admin notified.",
        "btn_back": "⬅️ Back",
    }
}

def T(uid_or_lang, key, **kw):
    if isinstance(uid_or_lang, str):
        lang = uid_or_lang
    else:
        row = get_user(uid_or_lang)
        lang = row["lang"] if row else DEFAULT_LANG
    return I18N[lang][key].format(**kw)

# ---------- RPC helpers ----------
def rpc(method, params):
    r = requests.post(SOL_RPC_URL, json={"jsonrpc":"2.0","id":1,"method":method,"params":params}, timeout=25)
    r.raise_for_status()
    j = r.json()
    if "error" in j: raise RuntimeError(j["error"])
    return j["result"]

def get_sigs_for(addr, limit=50):
    return rpc("getSignaturesForAddress", [addr, {"limit": limit}]) or []

def get_tx(sig):
    return rpc("getTransaction", [sig, {"encoding":"jsonParsed","maxSupportedTransactionVersion":0}])

def central_keypair():
    from base58 import b58decode
    raw = b58decode(CENTRAL_WALLET_SECRET)
    return Keypair.from_secret_key(raw)

def central_pubkey():
    return PublicKey(CENTRAL_WALLET_ADDRESS)

def asset_dec(asset): return ASSETS[asset]["decimals"]

def central_ata(asset):
    mint = PublicKey(ASSETS[asset]["mint"])
    return get_associated_token_address(central_pubkey(), mint)

# ---------- UI ----------
def main_menu(uid):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(T(uid, "btn_balance"), callback_data="m:bal"))
    kb.add(InlineKeyboardButton(T(uid, "btn_deposit"), callback_data="m:dep"),
           InlineKeyboardButton(T(uid, "btn_withdraw"), callback_data="m:wd"))
    kb.add(InlineKeyboardButton(T(uid, "btn_send"), callback_data="m:send"),
           InlineKeyboardButton(T(uid, "btn_history"), callback_data="m:hist"))
    kb.add(InlineKeyboardButton(T(uid, "btn_settings"), callback_data="m:set"),
           InlineKeyboardButton(T(uid, "btn_support"), callback_data="m:sup"))
    return kb

def asset_menu(uid, back_tag):
    kb = InlineKeyboardMarkup()
    for a in ASSETS.keys():
        kb.add(InlineKeyboardButton(a, callback_data=f"asset:{back_tag}:{a}"))
    kb.add(InlineKeyboardButton(T(uid,"btn_back"), callback_data="m:home"))
    return kb

# ---------- Start / Referral ----------
@bot.message_handler(commands=["start"])
def cmd_start(m):
    ref = None
    if " " in m.text:
        q = m.text.split(" ",1)[1]
        if q.startswith("ref="):
            try:
                ref = int(q.split("=",1)[1])
                if ref == m.from_user.id: ref = None
            except: ref=None
    ensure_user(m.from_user, ref=ref)
    bot.send_message(m.chat.id, T(m.from_user.id,"welcome", app=APP_NAME, addr=CENTRAL_WALLET_ADDRESS))
    bot.send_message(m.chat.id, T(m.from_user.id,"menu"), reply_markup=main_menu(m.from_user.id))

# ---------- Callbacks ----------
@bot.callback_query_handler(func=lambda c: True)
def on_cb(c: CallbackQuery):
    data = c.data
    if data == "m:home":
        bot.edit_message_text(T(c.from_user.id,"menu"), c.message.chat.id, c.message.message_id, reply_markup=main_menu(c.from_user.id))
    elif data == "m:bal":
        lines=[]
        for a in ASSETS.keys():
            av, hd = get_balance(c.from_user.id, a)
            lines.append(T(c.from_user.id,"line_balance", asset=a, av=fmt_amount(a,av), hd=fmt_amount(a,hd)))
        bot.edit_message_text(T(c.from_user.id,"balance", lines="\n".join(lines)), c.message.chat.id, c.message.message_id, reply_markup=main_menu(c.from_user.id))
    elif data == "m:dep":
        kb = asset_menu(c.from_user.id, "dep")
        bot.edit_message_text(T(c.from_user.id,"deposit_title"), c.message.chat.id, c.message.message_id, reply_markup=kb)
    elif data.startswith("asset:dep:"):
        asset = data.split(":")[2]
        u = get_user(c.from_user.id)
        src = u["source_wallet"] or "—"
        txt = T(c.from_user.id,"deposit_instr", src=src, central=CENTRAL_WALLET_ADDRESS) + f"\nAsset: <b>{asset}</b>"
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🏷 Source-Adresse setzen/ändern", callback_data=f"dep:setsrc"))
        kb.add(InlineKeyboardButton("🧾 Einzahlung prüfen", callback_data=f"dep:check:{asset}"))
        kb.add(InlineKeyboardButton(T(c.from_user.id,"btn_back"), callback_data="m:home"))
        bot.edit_message_text(txt, c.message.chat.id, c.message.message_id, reply_markup=kb)
    elif data == "dep:setsrc":
        msg = bot.send_message(c.message.chat.id, T(c.from_user.id,"set_src_prompt"))
        bot.register_next_step_handler(msg, on_set_source_wallet)
    elif data.startswith("dep:check:"):
        asset = data.split(":")[2]
        u = get_user(c.from_user.id)
        if not u["source_wallet"]:
            bot.answer_callback_query(c.id, T(c.from_user.id,"need_src"), show_alert=True); return
        booked, total = check_and_book_deposits(c.from_user.id, u["source_wallet"], asset)
        if booked:
            lines = [f"✔️ {len(booked)} Tx – Summe: {fmt_amount(asset, total)}"]
            for b in booked:
                lines.append(f"• {fmt_amount(asset, Decimal(str(b['amount'])))} | {b['tx'][:8]}…")
            bot.edit_message_text("\n".join(lines), c.message.chat.id, c.message.message_id, reply_markup=main_menu(c.from_user.id))
        else:
            bot.edit_message_text(T(c.from_user.id,"check_dep_none"), c.message.chat.id, c.message.message_id, reply_markup=main_menu(c.from_user.id))
    elif data == "m:send":
        msg = bot.send_message(c.message.chat.id, T(c.from_user.id,"send_who"))
        bot.register_next_step_handler(msg, on_send_user)
    elif data == "m:hist":
        txt = render_history(c.from_user.id)
        bot.edit_message_text(txt, c.message.chat.id, c.message.message_id, reply_markup=main_menu(c.from_user.id))
    elif data == "m:wd":
        msg = bot.send_message(c.message.chat.id, T(c.from_user.id,"withdraw_addr"))
        bot.register_next_step_handler(msg, on_withdraw_addr)
    elif data == "m:set":
        u = get_user(c.from_user.id)
        twofa = "AN" if u["twofa_enabled"] else "AUS"
        txt = T(c.from_user.id,"settings", lang=u["lang"], twofa=twofa, src=u["source_wallet"] or "—")
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🔐 2FA AN/AUS", callback_data="set:2fa"))
        kb.add(InlineKeyboardButton("🌐 Sprache: Deutsch", callback_data="set:lang:de"),
               InlineKeyboardButton("🌐 Language: English", callback_data="set:lang:en"))
        kb.add(InlineKeyboardButton("🏷 Source-Adresse", callback_data="dep:setsrc"))
        kb.add(InlineKeyboardButton(T(c.from_user.id,"btn_back"), callback_data="m:home"))
        bot.edit_message_text(txt, c.message.chat.id, c.message.message_id, reply_markup=kb)
    elif data == "set:2fa":
        u = get_user(c.from_user.id)
        val = 0 if u["twofa_enabled"] else 1
        conn.execute("UPDATE users SET twofa_enabled=? WHERE user_id=?", (val, c.from_user.id)); conn.commit()
        bot.answer_callback_query(c.id, T(c.from_user.id,"toggle_2fa", state=("AN" if val else "AUS")))
        on_cb(CallbackQuery(id=c.id, from_user=c.from_user, message=c.message, data="m:set"))
    elif data.startswith("set:lang:"):
        lang = data.split(":")[2]
        conn.execute("UPDATE users SET lang=? WHERE user_id=?", (lang, c.from_user.id)); conn.commit()
        bot.answer_callback_query(c.id, T(lang,"lang_switched", lang=("Deutsch" if lang=="de" else "English")))
        bot.edit_message_text(T(lang,"menu"), c.message.chat.id, c.message.message_id, reply_markup=main_menu(c.from_user.id))
    elif data.startswith("esc:"):
        # handled via dedicated handlers below
        pass

# ---------- Step handlers ----------
def on_set_source_wallet(m):
    ensure_user(m.from_user)
    a = m.text.strip()
    if not re.match(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$", a):
        bot.reply_to(m, "Das ist keine gültige Solana-Adresse.")
        return
    conn.execute("UPDATE users SET source_wallet=? WHERE user_id=?", (a, m.from_user.id)); conn.commit()
    bot.reply_to(m, f"✅ Gespeichert.\nZentrale Wallet:\n<code>{CENTRAL_WALLET_ADDRESS}</code>\n⚠️ Keine Börsen benutzen.")

def parse_amount_asset(text):
    # e.g. "0.5 SOL" / "10 usdc"
    parts = text.strip().replace(",", ".").split()
    if len(parts) == 1:
        return Decimal(parts[0]), "SOL"
    amt = Decimal(parts[0])
    asset = parts[1].upper()
    if asset not in ASSETS: raise ValueError("asset")
    return amt, asset

def on_send_user(m):
    ensure_user(m.from_user)
    u = m.text.strip().lstrip("@")
    row = get_user_by_username(u)
    if not row:
        bot.reply_to(m, f"Kein Nutzer @${u} gefunden (Empfänger muss /start ausführen)."); return
    if row["user_id"] == m.from_user.id:
        bot.reply_to(m, "Du kannst dir selbst nichts senden."); return
    bot.reply_to(m, T(m.from_user.id,"send_amount", u=u))
    bot.register_next_step_handler(m, lambda x: on_send_amount(x, row["user_id"], u))

def on_send_amount(m, to_uid, to_uname):
    try:
        amt, asset = parse_amount_asset(m.text)
        if amt <= 0: raise ValueError()
    except Exception:
        bot.reply_to(m, "Ungültiger Betrag/Asset."); return
    av, _ = get_balance(m.from_user.id, asset)
    if amt > av:
        bot.reply_to(m, f"Unzureichendes Guthaben. Verfügbar: {fmt_amount(asset,av)}"); return
    # Gebühren berechnen
    fee_percent = Decimal(str(FEE_PERCENT)) + (Decimal(str(ESCROW_EXTRA_FEE_PERCENT)))
    # Mode wählen
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(T(m.from_user.id,"mode_fnf"), callback_data=f"send:mode:FNF:{to_uid}:{to_uname}:{amt}:{asset}"),
           InlineKeyboardButton(T(m.from_user.id,"mode_escrow"), callback_data=f"send:mode:ESCROW:{to_uid}:{to_uname}:{amt}:{asset}"))
    kb.add(InlineKeyboardButton(T(m.from_user.id,"btn_back"), callback_data="m:home"))
    bot.reply_to(m, T(m.from_user.id,"send_mode", amt=f"{fmt_amount(asset,amt)}"))

@bot.callback_query_handler(func=lambda c: c.data.startswith("send:mode:"))
def on_send_mode(c):
    _,_,mode,to_uid,to_uname,amt,asset = c.data.split(":")
    amt = Decimal(amt); to_uid = int(to_uid)
    # tatsächliche Gebühren:
    p = Decimal(str(FEE_PERCENT))
    extra = Decimal("0")
    if mode=="ESCROW":
        extra = Decimal(str(ESCROW_EXTRA_FEE_PERCENT))
    fee = (amt * (p+extra) / Decimal("100")) + Decimal(str(FEE_FIXED))
    fee = dquant(fee, ASSETS[asset]["decimals"])
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ OK", callback_data=f"send:go:{mode}:{to_uid}:{to_uname}:{amt}:{asset}:{fee}"),
           InlineKeyboardButton(T(c.from_user.id,"btn_back"), callback_data="m:home"))
    bot.edit_message_text(T(c.from_user.id,"confirm", to=to_uname, amt=f"{fmt_amount(asset,amt)}",
                            mode=("Friends & Family" if mode=="FNF" else "Escrow"),
                            fee=f"{fmt_amount(asset,fee)}"),
                          c.message.chat.id, c.message.message_id, reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("send:go:"))
def on_send_go(c):
    _,_,mode,to_uid,to_uname,amt,asset,fee = c.data.split(":")
    to_uid = int(to_uid); amt = Decimal(amt); fee = Decimal(fee)
    u = get_user(c.from_user.id)
    payload = {"stage":"send","mode":mode,"to":to_uid,"amount":str(amt),"asset":asset,"fee":str(fee)}
    if u["twofa_enabled"]:
        code = gen_code()
        payload["code"]=code
        conn.execute("UPDATE users SET twofa_payload=? WHERE user_id=?", (json.dumps(payload), c.from_user.id)); conn.commit()
        bot.edit_message_text(f"🔐 Code: <code>{code}</code> – antworte mit dem Code zur Bestätigung.", c.message.chat.id, c.message.message_id)
        bot.register_next_step_handler(c.message, lambda m: confirm_send_code(m, payload))
    else:
        perform_send(c.message.chat.id, c.from_user.id, payload)

def confirm_send_code(m, payload):
    if m.text.strip() != payload.get("code"):
        bot.reply_to(m, "Falscher Code."); conn.execute("UPDATE users SET twofa_payload=NULL WHERE user_id=?", (m.from_user.id,)); conn.commit(); return
    perform_send(m.chat.id, m.from_user.id, payload)
    conn.execute("UPDATE users SET twofa_payload=NULL WHERE user_id=?", (m.from_user.id,)); conn.commit()

def perform_send(chat_id, from_uid, payload):
    mode = payload["mode"]; to_uid = int(payload["to"]); amt = Decimal(payload["amount"]); asset = payload["asset"]; fee = Decimal(payload["fee"])
    av,_ = get_balance(from_uid, asset)
    if amt > av:
        bot.send_message(chat_id, "Unzureichendes Guthaben."); return
    # Gebühren abziehen, Netto an Empfänger
    net = dquant(amt - fee, ASSETS[asset]["decimals"])
    if net <= 0:
        bot.send_message(chat_id, "Betrag zu klein nach Gebühren."); return
    # Abzüge beim Sender
    adj_balance(from_uid, asset, delta_av=-amt)
    t_id = str(uuid.uuid4())
    # Plattformgebühr verbuchen
    conn.execute("INSERT INTO fees_ledger(id, transfer_id, asset, amount, created_at) VALUES(?,?,?,?,?)",
                 (str(uuid.uuid4()), t_id, asset, float(fee), now_iso()))
    # Referral Rebate (aus der Gebühr)
    ref = get_user(from_uid)["referrer_id"]
    if ref and ref != to_uid and ref != from_uid:
        rebate = dquant((fee * Decimal(str(REFERRAL_REBATE_PERCENT)) / Decimal("100")), ASSETS[asset]["decimals"])
        if rebate > 0:
            adj_balance(ref, asset, delta_av=rebate)
            conn.execute("INSERT INTO referrals(id,transfer_id,referrer_id,referee_id,asset,rebate_amount,created_at) VALUES(?,?,?,?,?,?,?)",
                         (str(uuid.uuid4()), t_id, ref, from_uid, asset, float(rebate), now_iso()))
            # Plattformgebühr reduziert sich nicht intern (du kannst sie so oder so buchen) – hier lassen wir sie separat.

    if mode=="FNF":
        adj_balance(to_uid, asset, delta_av=net)
        status="completed"
    else:
        adj_balance(to_uid, asset, delta_hd=net)
        status="held"
    conn.execute("INSERT INTO transfers(id,type,asset,from_user,to_user,amount,fee_taken,status,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                 (t_id, mode, asset, from_uid, to_uid, float(amt), float(fee), status, now_iso()))
    conn.commit()
    if mode=="FNF":
        bot.send_message(chat_id, T(from_uid,"fnf_sent_sender", to=get_username(to_uid), amt=fmt_amount(asset,net)))
        bot.send_message(to_uid, T(to_uid, "fnf_sent_receiver", sender=get_username(from_uid), amt=fmt_amount(asset, net)))
    else:
        kb_sender = InlineKeyboardMarkup()
        kb_sender.add(InlineKeyboardButton("✅ Ware erhalten → Freigeben", callback_data=f"esc:release:{t_id}"),
                      InlineKeyboardButton("❗ Problem melden", callback_data=f"esc:dispute:{t_id}"))
        bot.send_message(chat_id, T(from_uid,"escrow_held_sender", to=get_username(to_uid), amt=fmt_amount(asset,net)), reply_markup=kb_sender)
        kb_recv = InlineKeyboardMarkup()
        kb_recv.add(InlineKeyboardButton("📦 Ich habe versendet", callback_data=f"esc:shipped:{t_id}"))
        bot.send_message(to_uid, T(to_uid,"escrow_held_receiver", from=get_username(from_uid), amt=fmt_amount(asset,net)), reply_markup=kb_recv)

# Escrow Actions
@bot.callback_query_handler(func=lambda c: c.data.startswith("esc:release:"))
def esc_release(c):
    t_id = c.data.split(":")[2]
    tr = conn.execute("SELECT * FROM transfers WHERE id=?", (t_id,)).fetchone()
    if not tr or tr["type"]!="ESCROW" or tr["from_user"]!=c.from_user.id or tr["status"]!="held":
        bot.answer_callback_query(c.id, "Nicht zulässig.", show_alert=True); return
    u = get_user(c.from_user.id)
    if u["twofa_enabled"]:
        code = gen_code()
        conn.execute("UPDATE users SET twofa_payload=? WHERE user_id=?", (json.dumps({"stage":"escrow","id":t_id,"code":code}), c.from_user.id)); conn.commit()
        bot.send_message(c.message.chat.id, f"🔐 Code: <code>{code}</code> – antworte zur Bestätigung.")
        bot.register_next_step_handler(c.message, lambda m: esc_release_code(m, t_id, code))
    else:
        do_release(t_id); bot.answer_callback_query(c.id, T(c.from_user.id,"release_ok"))

def esc_release_code(m, t_id, code):
    row = get_user(m.from_user.id)
    payload = row["twofa_payload"]
    if not payload: return
    p = json.loads(payload)
    if m.text.strip()!=p.get("code"): bot.reply_to(m,"Falscher Code."); return
    do_release(t_id); conn.execute("UPDATE users SET twofa_payload=NULL WHERE user_id=?", (m.from_user.id,)); conn.commit()
    bot.reply_to(m, T(m.from_user.id,"release_ok"))

def do_release(t_id):
    tr = conn.execute("SELECT * FROM transfers WHERE id=?", (t_id,)).fetchone()
    if not tr or tr["status"]!="held": return
    asset = tr["asset"]; amt = Decimal(str(tr["amount"])) - Decimal(str(tr["fee_taken"]))
    # Move held -> available for receiver
    av, hd = get_balance(tr["to_user"], asset)
    if hd < amt: return
    set_balance(tr["to_user"], asset, avail=av+amt, held=hd-amt)
    conn.execute("UPDATE transfers SET status='released', released_at=? WHERE id=?", (now_iso(), t_id)); conn.commit()
    bot.send_message(tr["to_user"], "✅ Escrow freigegeben.")
    bot.send_message(tr["from_user"], "✅ Freigabe erfolgreich.")

@bot.callback_query_handler(func=lambda c: c.data.startswith("esc:shipped:"))
def esc_shipped(c):
    t_id = c.data.split(":")[2]
    tr = conn.execute("SELECT * FROM transfers WHERE id=?", (t_id,)).fetchone()
    if not tr or tr["type"]!="ESCROW" or tr["to_user"]!=c.from_user.id:
        bot.answer_callback_query(c.id, "Nicht zulässig.", show_alert=True); return
    bot.answer_callback_query(c.id, "Versand gemeldet.")
    bot.send_message(tr["from_user"], T(tr["from_user"],"seller_shipped_buyer"))

@bot.callback_query_handler(func=lambda c: c.data.startswith("esc:dispute:"))
def esc_dispute(c):
    t_id = c.data.split(":")[2]
    tr = conn.execute("SELECT * FROM transfers WHERE id=?", (t_id,)).fetchone()
    if not tr or tr["type"]!="ESCROW":
        bot.answer_callback_query(c.id, "Nicht zulässig.", show_alert=True); return
    bot.answer_callback_query(c.id, T(c.from_user.id,"dispute_opened"))
    for aid in ADMIN_IDS:
        bot.send_message(aid, f"❗ Dispute\nTransfer {t_id}\n{get_username(tr['from_user'])} → {get_username(tr['to_user'])}\nAsset: {tr['asset']}\nAmount: {tr['amount']}")

# ---------- History ----------
def render_history(uid, limit=12):
    rows = conn.execute("""
      SELECT * FROM transfers WHERE from_user=? OR to_user=? ORDER BY datetime(created_at) DESC LIMIT ?
    """, (uid, uid, limit)).fetchall()
    if not rows:
        return T(uid,"history_none")
    out=["🧾"]
    for r in rows:
        direction = "📤" if r["from_user"]==uid else "📥"
        other = r["to_user"] if r["from_user"]==uid else r["from_user"]
        mode = "F&F" if r["type"]=="FNF" else "ESCROW"
        net = Decimal(str(r["amount"])) - Decimal(str(r["fee_taken"]))
        out.append(T(uid,"history_line", dir=direction, amt=f"{fmt_amount(r['asset'],net)}", other=get_username(other), mode=mode, status=r["status"]))
    return "\n".join(out)

# ---------- Deposits (SOL + SPL) ----------
def check_and_book_deposits(uid, src_addr, asset):
    booked=[]; total=Decimal("0")
    if ASSETS[asset]["type"]=="SOL":
        # Prüfe Zentraladresse (native SOL transfers)
        for s in get_sigs_for(CENTRAL_WALLET_ADDRESS, limit=50):
            sig = s["signature"]
            if conn.execute("SELECT 1 FROM deposits WHERE tx_sig=?", (sig,)).fetchone(): continue
            tx = get_tx(sig)
            try:
                insts = tx["transaction"]["message"]["instructions"]
                for ins in insts:
                    if ins.get("program")=="system" and ins.get("parsed",{}).get("type")=="transfer":
                        info = ins["parsed"]["info"]
                        if info["destination"]==CENTRAL_WALLET_ADDRESS and info["source"]==src_addr:
                            lam = int(info["lamports"]); amt = Decimal(lam)/Decimal(1e9)
                            # buchen
                            dep_id = str(uuid.uuid4())
                            conn.execute("INSERT INTO deposits(id,user_id,asset,tx_sig,from_address,amount,created_at) VALUES(?,?,?,?,?,?,?)",
                                         (dep_id, uid, asset, sig, src_addr, float(amt), now_iso()))
                            adj_balance(uid, asset, delta_av=amt)
                            booked.append({"tx":sig,"amount":float(amt)})
                            total += amt
                            break
            except: pass
    else:
        # SPL: prüfe zentrale ATA
        ata = str(central_ata(asset))
        for s in get_sigs_for(ata, limit=50):
            sig = s["signature"]
            if conn.execute("SELECT 1 FROM deposits WHERE tx_sig=?", (sig,)).fetchone(): continue
            tx = get_tx(sig)
            try:
                meta = tx["meta"]
                post = meta.get("postTokenBalances",[])
                pre = meta.get("preTokenBalances",[])
                # parser: suche increase des zentralen ATA vom richtigen mint, und Owner=central wallet
                mint = ASSETS[asset]["mint"]
                # Finde delta
                inc = Decimal("0")
                for pb, qb in zip(pre, post):
                    if qb["mint"]==mint and qb["owner"]==CENTRAL_WALLET_ADDRESS and qb["accountIndex"]!=pb["accountIndex"]:
                        # Fallback – unterschiedliche Reihenfolge möglich
                        pass
                # Robust: vergleiche sums
                def sum_bal(bal_list):
                    s=Decimal("0")
                    for b in bal_list:
                        if b["mint"]==mint and b["owner"]==CENTRAL_WALLET_ADDRESS:
                            ui = Decimal(b["uiTokenAmount"]["uiAmountString"])
                            s += ui
                    return s
                pre_sum = sum_bal(pre)
                post_sum = sum_bal(post)
                if post_sum > pre_sum:
                    inc = post_sum - pre_sum
                if inc>0:
                    # Hinweis: Wir können die exakte Sender-Adresse aus parsed inner instructions ziehen; hier matchen wir optional grob
                    dep_id = str(uuid.uuid4())
                    conn.execute("INSERT INTO deposits(id,user_id,asset,tx_sig,from_address,amount,created_at) VALUES(?,?,?,?,?,?,?)",
                                 (dep_id, uid, asset, sig, src_addr, float(inc), now_iso()))
                    adj_balance(uid, asset, delta_av=inc)
                    booked.append({"tx":sig,"amount":float(inc)})
                    total += inc
            except: pass
    conn.commit()
    return booked, float(total)

# ---------- Withdrawals (AUTO on-chain) ----------
def send_sol(to_addr, amount_sol: Decimal):
    kp = central_keypair()
    tx = Transaction()
    tx.add(transfer(TransferParams(from_pubkey=central_pubkey(), to_pubkey=PublicKey(to_addr), lamports=int(amount_sol*Decimal(1e9)))))
    res = sol_client.send_transaction(tx, kp, opts=TxOpts(skip_preflight=True))
    # wait confirmation (best-effort)
    sol_client.confirm_transaction(res.value)
    return res.value

def ensure_ata_for(asset, owner_pubkey: PublicKey):
    mint = PublicKey(ASSETS[asset]["mint"])
    ata = get_associated_token_address(owner_pubkey, mint)
    # check if exists
    info = sol_client.get_account_info(ata)
    if info.value is None:
        kp = central_keypair()
        tx = Transaction()
        tx.add(create_associated_token_account(payer=central_pubkey(), owner=owner_pubkey, mint=mint))
        res = sol_client.send_transaction(tx, kp, opts=TxOpts(skip_preflight=True))
        sol_client.confirm_transaction(res.value)
    return ata

def send_spl(asset, to_owner_addr, amount_ui: Decimal):
    kp = central_keypair()
    mint = PublicKey(ASSETS[asset]["mint"])
    dec = ASSETS[asset]["decimals"]
    to_owner = PublicKey(to_owner_addr)
    dest_ata = ensure_ata_for(asset, to_owner)
    src_ata = central_ata(asset)
    amt = int(amount_ui * (10**dec))
    tx = Transaction()
    tx.add(transfer_checked(
        program_id=TOKEN_PROGRAM_ID,
        source=src_ata,
        mint=mint,
        dest=dest_ata,
        owner=central_pubkey(),
        amount=amt,
        decimals=dec,
        signers=[]
    ))
    res = sol_client.send_transaction(tx, kp, opts=TxOpts(skip_preflight=True))
    sol_client.confirm_transaction(res.value)
    return res.value

def create_withdraw(uid, asset, to_addr, amount_ui: Decimal):
    # interne fee
    fee = dquant((amount_ui*Decimal(str(WITHDRAW_FEE_PERCENT))/Decimal("100")) + Decimal(str(WITHDRAW_FEE_FIXED)), ASSETS[asset]["decimals"])
    pay = dquant(amount_ui - fee, ASSETS[asset]["decimals"])
    if pay <= 0: raise RuntimeError("amount too small")
    # Saldo prüfen & reservieren
    av,_ = get_balance(uid, asset)
    if amount_ui > av: raise RuntimeError("insufficient balance")
    adj_balance(uid, asset, delta_av=-amount_ui)
    wid = str(uuid.uuid4())
    conn.execute("""INSERT INTO withdrawals(id,user_id,asset,to_address,amount,fee_taken,status,created_at,updated_at)
                    VALUES(?,?,?,?,?,?,?, ?, ?)""",
                 (wid, uid, asset, to_addr, float(amount_ui), float(fee), "pending", now_iso(), now_iso())); conn.commit()
    # On-chain senden
    try:
        if ASSETS[asset]["type"]=="SOL":
            sig = send_sol(to_addr, pay)
        else:
            sig = send_spl(asset, to_addr, pay)
        conn.execute("UPDATE withdrawals SET status='paid', tx_sig=?, updated_at=? WHERE id=?",
                     (sig, now_iso(), wid)); conn.commit()
        bot.send_message(uid, T(uid,"withdraw_done", tx=sig))
    except Exception as e:
        # rollback Guthaben
        adj_balance(uid, asset, delta_av=amount_ui)
        conn.execute("UPDATE withdrawals SET status='error', error=?, updated_at=? WHERE id=?",
                     (str(e), now_iso(), wid)); conn.commit()
        bot.send_message(uid, T(uid,"withdraw_err", e=str(e)))
        raise

def on_withdraw_addr(m):
    addr = m.text.strip()
    if not re.match(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$", addr):
        bot.reply_to(m, "Bitte eine gültige Solana-Adresse senden."); return
    bot.reply_to(m, T(m.from_user.id,"withdraw_amount"))
    bot.register_next_step_handler(m, lambda x: on_withdraw_amount(x, addr))

def on_withdraw_amount(m, to_addr):
    try:
        amt, asset = parse_amount_asset(m.text)
        if amt <= 0: raise ValueError()
    except Exception:
        bot.reply_to(m, "Ungültig."); return
    u = get_user(m.from_user.id)
    payload = {"stage":"wd","to":to_addr,"amt":str(amt),"asset":asset}
    if u["twofa_enabled"]:
        code = gen_code()
        payload["code"]=code
        conn.execute("UPDATE users SET twofa_payload=? WHERE user_id=?", (json.dumps(payload), m.from_user.id)); conn.commit()
        bot.reply_to(m, f"🔐 Code: <code>{code}</code> – antworte zur Bestätigung.")
        bot.register_next_step_handler(m, lambda x: confirm_withdraw_code(x, payload))
    else:
        try:
            create_withdraw(m.from_user.id, asset, to_addr, amt)
            bot.reply_to(m, T(m.from_user.id,"withdraw_created"))
        except Exception as e:
            bot.reply_to(m, T(m.from_user.id,"withdraw_err", e=str(e)))

def confirm_withdraw_code(m, payload):
    if m.text.strip()!=payload.get("code"):
        bot.reply_to(m, "Falscher Code."); conn.execute("UPDATE users SET twofa_payload=NULL WHERE user_id=?", (m.from_user.id,)); conn.commit(); return
    try:
        create_withdraw(m.from_user.id, payload["asset"], payload["to"], Decimal(payload["amt"]))
        bot.reply_to(m, T(m.from_user.id,"withdraw_created"))
    except Exception as e:
        bot.reply_to(m, T(m.from_user.id,"withdraw_err", e=str(e)))
    finally:
        conn.execute("UPDATE users SET twofa_payload=NULL WHERE user_id=?", (m.from_user.id,)); conn.commit()

# ---------- Support ----------
@bot.message_handler(commands=["reply"])
def admin_reply(m):
    if not is_admin(m.from_user.id): return
    try:
        _, uid, text = m.text.split(maxsplit=2)
        uid = int(uid)
    except:
        bot.reply_to(m, "Usage: /reply <user_id> <text>"); return
    bot.send_message(uid, f"🛠 Support: {text}")
    bot.reply_to(m, "Gesendet.")

@bot.callback_query_handler(func=lambda c: c.data=="m:sup")
def cb_support(c):
    msg = bot.send_message(c.message.chat.id, T(c.from_user.id,"support_prompt"))
    bot.register_next_step_handler(msg, on_support_msg)

def on_support_msg(m):
    txt = m.text or "(ohne Text)"
    for aid in ADMIN_IDS:
        bot.send_message(aid, f"🆘 Support von @{get_username(m.from_user.id)} ({m.from_user.id}):\n\n{txt}\n\n/reply {m.from_user.id} <Text>")
    bot.reply_to(m, "Danke! Wir melden uns hier im Chat.")

# ---------- Fallback ----------
@bot.message_handler(commands=["menu"])
def cmd_menu(m):
    ensure_user(m.from_user)
    bot.send_message(m.chat.id, T(m.from_user.id,"menu"), reply_markup=main_menu(m.from_user.id))

@bot.message_handler(content_types=["text","photo","document","sticker","video","audio","voice"])
def any_msg(m):
    ensure_user(m.from_user)
    bot.send_message(m.chat.id, T(m.from_user.id,"menu"), reply_markup=main_menu(m.from_user.id))

# ---------- Run ----------
if __name__ == "__main__":
    print("Starting bot...")
    bot.infinity_polling(skip_pending=True, timeout=20)