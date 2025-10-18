#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, time, sqlite3, threading, math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from telebot import TeleBot, types
from telebot.apihelper import ApiTelegramException

# ======== ENV / CONFIG ========
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "8222875136:AAFAa9HtRL-g23ganuckjCq5IIW9udQXOZo").strip()
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS","8076025426").replace(";",",").split(",") if x.strip().isdigit()]

SOL_RPC_URL = os.getenv("SOL_RPC_URL", "https://api.mainnet-beta.solana.com").strip()
RPC_HEADER_KEY = os.getenv("RPC_HEADER_KEY","").strip() or None
RPC_HEADER_VALUE = os.getenv("RPC_HEADER_VALUE","").strip() or None
RPC_HEADERS = {RPC_HEADER_KEY: RPC_HEADER_VALUE} if (RPC_HEADER_KEY and RPC_HEADER_VALUE) else {}

CENTRAL_WALLET_ADDRESS = os.getenv("CENTRAL_WALLET_ADDRESS","Ga9L4teyfbnJcxhhErKAquF8cHy3GR6XPF1sqxji3DN9").strip()
CENTRAL_WALLET_SECRET_RAW = os.getenv("CENTRAL_WALLET_SECRET","[216,228,184,240,28,208,86,251,72,207,66,95,46,213,227,92,3,151,107,135,207,35,239,106,204,30,183,73,9,76,39,133,231,92,227,79,168,2,181,228,68,217,227,49,92,136,161,209,206,110,146,237,79,243,145,54,121,109,106,22,160,136,164,90]").strip()

TOKENS_ENV = os.getenv("TOKENS","").strip()
# Format: "MINT:SYMBOL:DECIMALS, MINT2:SYMBOL2:DECIMALS"
TOKEN_WHITELIST = {}  # mint -> (symbol, decimals)
if TOKENS_ENV:
    for part in [p.strip() for p in TOKENS_ENV.split(",") if p.strip()]:
        try:
            mint, sym, dec = [x.strip() for x in part.split(":")]
            TOKEN_WHITELIST[mint] = (sym, int(dec))
        except Exception:
            pass

DB_PATH = os.getenv("DB_PATH","cryptopay_v2.db")
MIN_SOL_DEPOSIT = float(os.getenv("MIN_SOL_DEPOSIT", "0.0001"))  # mindest Betrag SOL
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "12"))  # Sekunden zwischen Scans
SIG_LIMIT = int(os.getenv("SIG_LIMIT", "50"))          # getSignaturesForAddress Limit

# ======== SIMPLE WALLET LOAD (nur für Auszahlungen, optional) ========
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.instruction import Instruction
from solders.message import Message
from solders.hash import Hash
from solders.transaction import Transaction

def load_keypair(raw: str) -> Keypair:
    raw = raw.strip()
    if raw.startswith("["):
        arr = json.loads(raw)
        if not isinstance(arr, list) or len(arr) != 64:
            raise SystemExit("CENTRAL_WALLET_SECRET: erwarte 64-Byte JSON-Array")
        return Keypair.from_bytes(bytes(arr))
    else:
        # Base58 wird nicht erwartet, nur JSON-Array (vereinfacht)
        raise SystemExit("CENTRAL_WALLET_SECRET: Bitte 64-Byte JSON-Array verwenden.")
    
CENTRAL_KP = load_keypair(CENTRAL_WALLET_SECRET_RAW)
if CENTRAL_WALLET_ADDRESS and str(CENTRAL_KP.pubkey()) != CENTRAL_WALLET_ADDRESS:
    print("WARN: CENTRAL_WALLET_ADDRESS != Secret.pubkey()")

# ======== TELEGRAM INIT ========
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN fehlt in .env")

bot = TeleBot(BOT_TOKEN, parse_mode="HTML")

def edit_message_text_safe(chat_id: int, message_id: int, text: str, reply_markup=None):
    """Ignoriere TG 400 'message is not modified'."""
    try:
        bot.edit_message_text(text, chat_id, message_id, reply_markup=reply_markup)
    except ApiTelegramException as e:
        if "message is not modified" in str(e):
            return
        raise

# ======== DB ========
def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

CONN = db()
CONN.execute("""
CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  username TEXT,
  created_at INTEGER DEFAULT (strftime('%s','now'))
)
""")
CONN.execute("""
CREATE TABLE IF NOT EXISTS wallets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  source_address TEXT NOT NULL,
  verified INTEGER DEFAULT 1,
  UNIQUE(user_id, source_address)
)
""")
CONN.execute("""
CREATE TABLE IF NOT EXISTS balances (
  user_id INTEGER NOT NULL,
  mint TEXT NOT NULL,            -- "SOL" oder SPL Mint
  amount REAL NOT NULL DEFAULT 0,
  PRIMARY KEY (user_id, mint)
)
""")
CONN.execute("""
CREATE TABLE IF NOT EXISTS deposits (
  sig TEXT PRIMARY KEY,
  user_id INTEGER,
  from_addr TEXT,
  to_addr TEXT,
  mint TEXT,
  amount REAL,
  ts INTEGER DEFAULT (strftime('%s','now'))
)
""")
CONN.commit()

def get_or_create_user(uid: int, username: Optional[str]):
    CONN.execute("INSERT OR IGNORE INTO users(user_id, username) VALUES (?,?)", (uid, username or ""))
    if username:
        CONN.execute("UPDATE users SET username=? WHERE user_id=?", (username, uid))
    CONN.commit()

def add_source_wallet(uid: int, addr: str):
    CONN.execute("INSERT OR IGNORE INTO wallets(user_id, source_address, verified) VALUES(?,?,1)", (uid, addr))
    CONN.commit()

def list_source_wallets(uid: int) -> List[str]:
    cur = CONN.execute("SELECT source_address FROM wallets WHERE user_id=? AND verified=1", (uid,))
    return [r[0] for r in cur.fetchall()]

def credit(uid: int, mint: str, amount: float):
    CONN.execute("INSERT INTO balances(user_id, mint, amount) VALUES(?,?,?) ON CONFLICT(user_id, mint) DO UPDATE SET amount=amount+excluded.amount", (uid, mint, amount))
    CONN.commit()

def get_balances(uid: int) -> List[Tuple[str,float]]:
    cur = CONN.execute("SELECT mint, amount FROM balances WHERE user_id=?", (uid,))
    return cur.fetchall()

def already_booked(sig: str) -> bool:
    cur = CONN.execute("SELECT 1 FROM deposits WHERE sig=?", (sig,))
    return cur.fetchone() is not None

def mark_deposit(sig: str, uid: int, from_addr: str, to_addr: str, mint: str, amount: float):
    CONN.execute("INSERT OR IGNORE INTO deposits(sig, user_id, from_addr, to_addr, mint, amount) VALUES(?,?,?,?,?,?)",
                 (sig, uid, from_addr, to_addr, mint, amount))
    CONN.commit()

# ======== RPC HELPERS ========
def rpc(method: str, params):
    payload = {"jsonrpc":"2.0","id":1,"method":method,"params":params}
    backoff = 1.0
    for i in range(6):
        r = requests.post(SOL_RPC_URL, json=payload, headers=RPC_HEADERS, timeout=30)
        if r.status_code == 200:
            j = r.json()
            if "error" in j:
                # z.B. blockhash not found etc.
                raise RuntimeError(f"RPC error {method}: {j['error']}")
            return j.get("result")
        # 429 o.ä. -> Backoff
        time.sleep(backoff)
        backoff = min(backoff*1.6, 10.0)
    r.raise_for_status()

def get_sigs_for(addr: str, limit: int = 50):
    res = rpc("getSignaturesForAddress", [addr, {"limit": limit}]) or []
    return res

def get_tx(sig: str):
    return rpc("getTransaction", [sig, {"encoding":"jsonParsed","maxSupportedTransactionVersion":0}])

# ======== PARSER: finde Einzahlungen an CENTRAL_WALLET_ADDRESS nach sender ========
@dataclass
class DepositHit:
    sig: str
    from_addr: str
    to_addr: str
    mint: str  # "SOL" oder SPL mint
    amount_ui: float

def parse_transfer(txj) -> List[DepositHit]:
    """
    Liefert gefundene SOL/SPL-Transfers an CENTRAL_WALLET_ADDRESS inkl. from_addr.
    """
    hits: List[DepositHit] = []
    if not txj or not txj.get("meta") or not txj.get("transaction"):
        return hits

    tx = txj["transaction"]
    meta = txj["meta"]
    sig = (txj.get("transaction", {}).get("signatures") or [""])[0]

    # --- SOL-Transfers (System Program) ---
    try:
        insts = tx["message"]["instructions"]
        account_keys = tx["message"]["accountKeys"]
        # neue Form seit v0: accountKeys sind Objekte mit "pubkey"
        aks = [a["pubkey"] if isinstance(a, dict) and "pubkey" in a else a for a in account_keys]
        # parse each instruction
        for ix in insts:
            # ix kann parsed sein oder raw
            prog = ix.get("program") or ix.get("programId") or ix.get("programIdIndex")
            parsed = ix.get("parsed")
            if parsed and parsed.get("type") == "transfer" and parsed.get("info"):
                info = parsed["info"]
                source = info.get("source")
                dest = info.get("destination")
                lamports = int(info.get("lamports", 0))
                if dest == CENTRAL_WALLET_ADDRESS and lamports > 0:
                    amt_ui = lamports / 1e9
                    hits.append(DepositHit(sig=sig, from_addr=source, to_addr=dest, mint="SOL", amount_ui=amt_ui))
    except Exception:
        pass

    # --- SPL-Token Transfers (Token Program 2022 / klassisch) ---
    # Wir scannen meta["postTokenBalances"] - ["owner"] + delta
    try:
        post_tb = meta.get("postTokenBalances") or []
        pre_tb = meta.get("preTokenBalances") or []
        # Map (accountIndex -> (mint, owner, ui_amount))
        def tb_map(lst):
            m = {}
            for e in lst:
                idx = e["accountIndex"]
                mint = e["mint"]
                owner = e.get("owner")
                ui = float(e.get("uiTokenAmount", {}).get("uiAmountString") or e.get("uiTokenAmount", {}).get("uiAmount") or 0)
                m[idx] = (mint, owner, ui)
            return m
        pre = tb_map(pre_tb)
        post = tb_map(post_tb)
        # finde Veränderungen, bei denen der Owner die CENTRAL_WALLET_ADDRESS ist
        # (owner ist EOA; der tatsächliche Token-Account ist accountIndex in accountKeys)
        for idx, (mint, owner, post_ui) in post.items():
            if not owner or owner != CENTRAL_WALLET_ADDRESS:
                continue
            pre_ui = pre.get(idx, (mint, owner, 0))[2]
            delta = post_ui - pre_ui
            if delta > 0:  # Einzahlung
                # Quelle herausfinden: wer hat denselben Mint mit negativem Delta?
                # Grobe Heuristik: suche irgendeinen Owner (nicht central) mit -delta (toleranz)
                from_owner = None
                for idx2, (m2, o2, post2_ui) in post.items():
                    if m2 != mint or o2 == CENTRAL_WALLET_ADDRESS or o2 is None:
                        continue
                    pre2_ui = pre.get(idx2, (m2, o2, 0))[2]
                    d2 = post2_ui - pre2_ui
                    if d2 < 0 and abs(d2 + delta) < max(1e-6, delta * 1e-6):
                        from_owner = o2
                        break
                if from_owner:
                    hits.append(DepositHit(sig=sig, from_addr=from_owner, to_addr=CENTRAL_WALLET_ADDRESS, mint=mint, amount_ui=delta))
    except Exception:
        pass

    return hits

# ======== SCANNER ========
def scan_once(user_source_map: Dict[str, int]) -> int:
    """
    user_source_map: { source_address -> user_id }
    return: anzahl neu gebuchte
    """
    booked = 0
    sigs = get_sigs_for(CENTRAL_WALLET_ADDRESS, limit=SIG_LIMIT) or []
    for s in sigs:
        sig = s["signature"]
        if already_booked(sig):
            continue
        tx = get_tx(sig)
        hits = parse_transfer(tx)
        for h in hits:
            # Nur Einzahlungen von bekannten Source-Adressen buchen
            uid = user_source_map.get(h.from_addr)
            if not uid:
                continue
            # Mindestgrenze für SOL anwenden
            if h.mint == "SOL" and h.amount_ui < MIN_SOL_DEPOSIT:
                continue
            # Token Whitelist?
            if h.mint != "SOL" and h.mint not in TOKEN_WHITELIST:
                continue

            symbol, _dec = ("SOL", 9) if h.mint == "SOL" else TOKEN_WHITELIST[h.mint]
            credit(uid, h.mint, h.amount_ui)
            mark_deposit(h.sig, uid, h.from_addr, h.to_addr, h.mint, h.amount_ui)
            booked += 1
            # Notify User
            try:
                sym = symbol
                bot.send_message(uid, f"✅ Einzahlung erkannt\n<b>Asset:</b> {sym}\n<b>Betrag:</b> {h.amount_ui}\n<b>Von:</b> <code>{h.from_addr}</code>\n<b>Tx:</b> <code>{h.sig}</code>")
            except Exception:
                pass
    return booked

def build_user_source_map() -> Dict[str, int]:
    cur = CONN.execute("SELECT user_id, source_address FROM wallets WHERE verified=1")
    out = {}
    for uid, addr in cur.fetchall():
        out[addr] = uid
    return out

def scan_loop():
    print("Deposit-Scanner gestartet.")
    while True:
        try:
            m = build_user_source_map()
            if m:
                scan_once(m)
        except Exception as e:
            print("Scan-Fehler:", e)
        time.sleep(SCAN_INTERVAL)

# ======== UI / BOT ========
def fmt_balances(uid: int) -> str:
    bals = get_balances(uid)
    if not bals:
        return "Keine Guthaben."
    lines = []
    for mint, amt in bals:
        if mint == "SOL":
            sym = "SOL"
        else:
            sym = TOKEN_WHITELIST.get(mint, (mint[:4]+"…", 0))[0]
        lines.append(f"• {sym}: {amt}")
    return "\n".join(lines)

def main_menu_kb(uid: int):
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("➕ Einzahlungs-Quelle hinzufügen", callback_data="m:addsource"))
    kb.add(types.InlineKeyboardButton("💰 Guthaben anzeigen", callback_data="m:bal"))
    kb.add(types.InlineKeyboardButton("↗️ Auszahlung (SOL)", callback_data="m:wd_sol"))
    kb.add(types.InlineKeyboardButton("🔁 Neu laden", callback_data="m:refresh"))
    return kb

@bot.message_handler(commands=["start"])
def on_start(m):
    get_or_create_user(m.from_user.id, m.from_user.username)
    bot.send_message(m.chat.id,
        "👋 Willkommen!\n\n"
        "Dieser Bot erkennt Einzahlungen **ohne Memo**, nur anhand deiner **Absender-Wallet**.\n\n"
        "1) Tippe auf <b>„Einzahlungs-Quelle hinzufügen“</b> und sende deine Wallet-Adresse\n"
        "2) Sende dann SOL/USDC/USDT von genau <u>dieser</u> Adresse an die zentrale Adresse:\n"
        f"<code>{CENTRAL_WALLET_ADDRESS}</code>\n\n"
        "Danach wird die Einzahlung automatisch gebucht.",
        reply_markup=main_menu_kb(m.from_user.id))

@bot.callback_query_handler(func=lambda c: True)
def on_cb(c):
    get_or_create_user(c.from_user.id, c.from_user.username)
    if c.data == "m:refresh":
        edit_message_text_safe(c.message.chat.id, c.message.message_id, "🔄 Aktualisiert.", reply_markup=main_menu_kb(c.from_user.id))
    elif c.data == "m:bal":
        txt = "💰 <b>Dein Guthaben</b>\n\n" + fmt_balances(c.from_user.id)
        edit_message_text_safe(c.message.chat.id, c.message.message_id, txt, reply_markup=main_menu_kb(c.from_user.id))
    elif c.data == "m:addsource":
        msg = bot.send_message(c.message.chat.id, "Bitte sende mir jetzt die <b>Wallet-Adresse</b>, von der deine Einzahlungen kommen (z. B. Phantom-Adresse).")
        bot.register_next_step_handler(msg, on_add_source_addr)
    elif c.data == "m:wd_sol":
        msg = bot.send_message(c.message.chat.id, "Gib bitte die Ziel-Adresse für die <b>SOL-Auszahlung</b> an:")
        bot.register_next_step_handler(msg, on_withdraw_sol_address)
    else:
        edit_message_text_safe(c.message.chat.id, c.message.message_id, "Menü:", reply_markup=main_menu_kb(c.from_user.id))

def on_add_source_addr(m):
    addr = m.text.strip()
    if len(addr) < 32 or len(addr) > 60:
        bot.reply_to(m, "Das sieht nicht wie eine Solana-Adresse aus. Bitte nochmal.")
        return
    get_or_create_user(m.from_user.id, m.from_user.username)
    add_source_wallet(m.from_user.id, addr)
    wallets = list_source_wallets(m.from_user.id)
    bot.reply_to(m,
        "✅ Quelle gespeichert.\n"
        f"<b>Deine verifizierten Quellen:</b>\n" + "\n".join([f"• <code>{w}</code>" for w in wallets]) +
        f"\n\n<b>Einzahlen an:</b>\n<code>{CENTRAL_WALLET_ADDRESS}</code>\n"
        "Sende nun SOL/USDC/USDT <u>von genau dieser Quelle</u>. Ich buche automatisch.")

# ======== Auszahlen (SOL) mit Memo "Withdrawal ..." ========
def get_latest_blockhash() -> Hash:
    res = rpc("getLatestBlockhash", [{"commitment":"finalized"}])
    bh = res["value"]["blockhash"]
    return Hash.from_string(bh)

def sol_transfer(sender_kp: Keypair, to_addr: str, amount_sol: float, memo: Optional[str]=None) -> str:
    sender = sender_kp.pubkey()
    to = Pubkey.from_string(to_addr)
    lamports = int(amount_sol * 1e9)
    tx_insts = []
    tx_insts.append(transfer(TransferParams(from_pubkey=sender, to_pubkey=to, lamports=lamports)))
    if memo:
        memo_ix = Instruction(
            program_id=Pubkey.from_string("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"),
            accounts=[],
            data=memo.encode("utf-8")
        )
        tx_insts.append(memo_ix)
    msg = Message(tx_insts)
    recent = get_latest_blockhash()
    tx = Transaction([sender_kp], msg, recent)
    raw = bytes(tx)
    # sendTransaction (base64)
    import base64
    b64 = base64.b64encode(raw).decode()
    res = rpc("sendTransaction", [b64, {"skipPreflight": False, "preflightCommitment":"confirmed"}])
    return res

def on_withdraw_sol_address(m):
    addr = m.text.strip()
    if len(addr) < 32 or len(addr) > 60:
        bot.reply_to(m, "Ungültige Adresse. Abbruch.")
        return
    msg = bot.reply_to(m, "Wie viel SOL möchtest du auszahlen? (z. B. 0.05)")
    bot.register_next_step_handler(msg, lambda x: on_withdraw_sol_amount(addr, x))

def on_withdraw_sol_amount(addr: str, m):
    try:
        amt = float(m.text.strip().replace(",","."))
        if amt <= 0:
            raise ValueError
    except Exception:
        bot.reply_to(m, "Ungültiger Betrag. Abbruch.")
        return
    # Prüfe Guthaben
    cur = CONN.execute("SELECT amount FROM balances WHERE user_id=? AND mint='SOL'", (m.from_user.id,))
    row = cur.fetchone()
    bal = row[0] if row else 0.0
    if bal + 1e-12 < amt:
        bot.reply_to(m, f"Nicht genug Guthaben. Dein SOL-Guthaben: {bal}")
        return
    # Sende Auszahlung
    try:
        sig = sol_transfer(CENTRAL_KP, addr, amt, memo=f"Withdrawal @{m.from_user.username or m.from_user.id}")
        # belaste Guthaben
        CONN.execute("UPDATE balances SET amount=amount-? WHERE user_id=? AND mint='SOL'", (amt, m.from_user.id))
        CONN.commit()
        bot.reply_to(m, f"✅ Auszahlung gesendet.\n<b>Betrag:</b> {amt} SOL\n<b>Ziel:</b> <code>{addr}</code>\n<b>Tx:</b> <code>{sig}</code>")
    except Exception as e:
        bot.reply_to(m, f"❌ Auszahlung fehlgeschlagen: {e}")

# ======== START ========
if __name__ == "__main__":
    if not CENTRAL_WALLET_ADDRESS:
        raise SystemExit("CENTRAL_WALLET_ADDRESS fehlt in .env")
    print("Starting ProofPay (SOL live).")
    t = threading.Thread(target=scan_loop, daemon=True)
    t.start()
    bot.infinity_polling(timeout=30, long_polling_timeout=30)