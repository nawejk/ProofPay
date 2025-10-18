# -*- coding: utf-8 -*-
"""
ProofPay – Telegram Bot (SOL on-chain, Auto-Deposit/Auto-Withdraw, Escrow)
Kompatibel mit: solana==0.25.0  (getestet)
"""

import os, json, time, threading, sqlite3, uuid, random, string, re
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone

from dotenv import load_dotenv
import requests
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ---- Solana libs: solana 0.25.x ----
from solana.keypair import Keypair
from solana.publickey import PublicKey
from solana.system_program import transfer, TransferParams
from solana.rpc.api import Client
from solana.transaction import Transaction
from solana.rpc.types import TxOpts

# ------------------ ENV ---------------------
load_dotenv()
BOT_TOKEN     = os.getenv("BOT_TOKEN","8222875136:AAFAa9HtRL-g23ganuckjCq5IIW9udQXOZo").strip()
ADMIN_IDS     = [int(x) for x in os.getenv("ADMIN_IDS","8076025426").split(",") if x.strip().isdigit()]
DEFAULT_LANG  = os.getenv("DEFAULT_LANG","en").strip().lower()
SOL_RPC_URL   = os.getenv("SOL_RPC_URL","https://api.mainnet-beta.solana.com").strip()

FEE_FNF = Decimal(os.getenv("FEE_FNF","0.6"))
FEE_ESCROW_EXTRA = Decimal(os.getenv("FEE_ESCROW_EXTRA","0.2"))

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

# Parse Solana Keypair (64-byte JSON array)
try:
    secret_list = json.loads(CENTRAL_WALLET_SECRET)
    if not (isinstance(secret_list, list) and len(secret_list) == 64):
        raise ValueError("CENTRAL_WALLET_SECRET muss 64-Byte JSON-Array sein (solana-keygen export).")
    kp = Keypair.from_secret_key(bytes(secret_list))
    if str(kp.public_key) != CENTRAL_WALLET_ADDRESS:
        print("WARN: CENTRAL_WALLET_ADDRESS stimmt nicht mit Secret überein. Bitte prüfen.")
except Exception as e:
    raise SystemExit(f"Ungültiger CENTRAL_WALLET_SECRET: {e}")

# RPC Clients
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

ASSETS = {"SOL": 9}

def ensure_system():
    # System-User (id=0) + SOL-Balance anlegen (für Fees), falls nicht vorhanden
    r = conn.execute("SELECT 1 FROM users WHERE user_id=0").fetchone()
    if not r:
        # FIX: 9 Platzhalter (inkl. user_id)
        conn.execute(
            """INSERT INTO users(user_id,username,first_name,last_name,lang,twofa_enabled,ref_code,ref_by,created_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (0, "system", "", "", DEFAULT_LANG, 0, "R0", None, datetime.now(timezone.utc).isoformat())
        )
    # sicherstellen, dass Balance-Zeile existiert
    for a in ASSETS:
        b = conn.execute("SELECT 1 FROM balances WHERE user_id=? AND asset=?", (0, a)).fetchone()
        if not b:
            conn.execute("INSERT INTO balances(user_id,asset,available,held) VALUES(?,?,0,0)", (0, a))
    conn.commit()

init_db()
ensure_system()

def now_iso(): return datetime.now(timezone.utc).isoformat()
def dquant(x, dec): return Decimal(x).quantize(Decimal(10) ** -dec, rounding=ROUND_DOWN)
def fmt(asset, x): return f"{dquant(Decimal(x), ASSETS[asset])} {asset}"
def is_admin(uid): return uid in ADMIN_IDS

def ensure_user(tu, ref_by=None):
    r = conn.execute("SELECT * FROM users WHERE user_id=?", (tu.id,)).fetchone()
    if not r:
        code = f"R{tu.id}"
        conn.execute("""INSERT INTO users(user_id,username,first_name,last_name,lang,twofa_enabled,ref_code,ref_by,created_at)
                        VALUES(?,?,?,?,?,?,?,?,?)""",
                     (tu.id, tu.username or "", tu.first_name or "", tu.last_name or "",
                      DEFAULT_LANG, 1, code, ref_by, now_iso()))
        for a in ASSETS: conn.execute("INSERT INTO balances(user_id,asset,available,held) VALUES(?,?,0,0)", (tu.id, a))
        conn.commit()

def get_user(uid): return conn.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
def get_user_by_username(u):
    if not u: return None
    return conn.execute("SELECT * FROM users WHERE lower(username)=?", (u.lstrip("@").lower(),)).fetchone()
def get_username(uid):
    if uid == 0: return "system"
    r = conn.execute("SELECT username FROM users WHERE user_id=?", (uid,)).fetchone()
    return (r["username"] or str(uid)) if r else str(uid)

def bal(uid, asset):
    r = conn.execute("SELECT available,held FROM balances WHERE user_id=? AND asset=?", (uid, asset)).fetchone()
    # robust: fehlende Zeile automatisch anlegen
    if not r:
        conn.execute("INSERT OR IGNORE INTO balances(user_id,asset,available,held) VALUES(?,?,0,0)", (uid, asset))
        conn.commit()
        return Decimal("0"), Decimal("0")
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

# ------------------ I18N --------------------
I18N = {
 # (unverändert, wie bei dir) ...
 "de": { ... },
 "en": { ... }
}

def T(uid, key, **kw):
    u = get_user(uid)
    lang = u["lang"] if u else DEFAULT_LANG
    return I18N[lang][key].format(**kw)

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
    return kb

def safe_edit(chat_id, msg_id, text, reply_markup=None):
    try:
        bot.edit_message_text(text, chat_id, msg_id, reply_markup=reply_markup)
    except Exception:
        bot.send_message(chat_id, text, reply_markup=reply_markup)

# ------------------ RPC helper ----------------
def rpc_post(method, params):
    tries=0
    delay=0.8
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
            ref_by=int(code[1:])
            if ref_by==m.from_user.id: ref_by=None
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
    try:
        bot.send_message(uid, T(uid,"deposit_booked", amt=str(sol_amt), sig=sig))
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
                if not sig:
                    continue
                if conn.execute("SELECT 1 FROM deposit_seen WHERE sig=?", (sig,)).fetchone():
                    continue

                tx = None
                for attempt in range(5):
                    try:
                        tx = get_tx(sig)
                        break
                    except Exception as e:
                        if "429" in str(e):
                            time.sleep(0.6*(attempt+1)); continue
                        tx = None; break
                if not tx:
                    conn.execute("INSERT OR IGNORE INTO deposit_seen(sig) VALUES(?)", (sig,)); conn.commit()
                    continue

                result = tx
                ok_src = None
                amt_sol = Decimal("0")

                try:
                    msg = result.get("transaction", {}).get("message", {})
                    for inst in msg.get("instructions", []):
                        if inst.get("program")=="system" and inst.get("parsed",{}).get("type")=="transfer":
                            info=inst["parsed"]["info"]
                            src = info.get("source"); dst=info.get("destination")
                            if dst == CENTRAL_WALLET_ADDRESS and src in expected:
                                ok, a = is_sol_deposit_from_source(result, src, CENTRAL_WALLET_ADDRESS, MIN_DEPOSIT_SOL)
                                if ok:
                                    ok_src = src; amt_sol = a
                                    break
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

def do_send(chat_id, from_uid, to_uid, to_uname, amt, mode):
    av,_=bal(from_uid, "SOL")
    if amt>av:
        bot.send_message(chat_id, T(from_uid,"err_balance", av=fmt("SOL",av))); return
    fee_percent = FEE_FNF + (FEE_ESCROW_EXTRA if mode=="ESCROW" else Decimal("0"))
    fee = dquant(amt * fee_percent / Decimal("100"), 9)
    net = dquant(amt - fee, 9)
    bal_adj(from_uid, "SOL", da=-amt)
    bal_adj(0, "SOL", da=fee)  # Fee in System-Konto (User 0)
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

# ------------------ WITHDRAW (echte On-Chain) --------------
def _extract_latest_blockhash(resp):
    try:
        return resp["result"]["value"]["blockhash"]
    except Exception:
        pass
    try:
        return resp.value.blockhash
    except Exception:
        pass
    raise RuntimeError(f"unexpected get_latest_blockhash() response: {resp}")

def _extract_sig(resp):
    if isinstance(resp, dict):
        if "result" in resp and isinstance(resp["result"], str):
            return resp["result"]
        if "value" in resp and isinstance(resp["value"], str):
            return resp["value"]
    try:
        return resp.value
    except Exception:
        pass
    raise RuntimeError(f"unexpected send_transaction() response: {resp}")

def withdraw_sol(to_addr: str, sol_amt: Decimal) -> str:
    if not re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", to_addr or ""):
        raise ValueError("invalid address")
    to_pk = PublicKey(to_addr)
    tx = Transaction()
    instr = transfer(
        TransferParams(
            from_pubkey=kp.public_key,
            to_pubkey=to_pk,
            lamports=int(sol_amt * Decimal(1_000_000_000))
        )
    )
    tx.add(instr)
    # Blockhash & FeePayer setzen
    bh_resp = rpc.get_latest_blockhash()
    bh = _extract_latest_blockhash(bh_resp)
    tx.recent_blockhash = bh
    tx.fee_payer = kp.public_key
    # Für solana 0.25.x: Wallet direkt an send_transaction übergeben (signing inside)
    resp = rpc.send_transaction(tx, kp, opts=TxOpts(skip_preflight=False, max_retries=3))
    sig = _extract_sig(resp)
    rpc.confirm_transaction(sig)
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
    def finalize():
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

    if u["twofa_enabled"]:
        code=''.join(random.choices(string.digits,k=6))
        msg = bot.reply_to(m, f"🔐 2FA – antworte mit: <code>{code}</code>")
        def check_code(x):
            if (x.text or "").strip()!=code: bot.reply_to(x,"Falscher Code."); return
            finalize()
        bot.register_next_step_handler(msg, check_code)
    else:
        finalize()

# ------------------ SUPPORT -------------------
def sup_msg(m):
    txt=m.text or "(ohne Text)"
    for a in ADMIN_IDS:
        try:
            bot.send_message(a, f"🆘 Support von @{get_username(m.from_user.id)} ({m.from_user.id}):\n\n{txt}")
        except: pass
    bot.reply_to(m, "Danke! Wir melden uns hier im Chat.", reply_markup=menu(m.from_user.id))

# ------------- Callbacks / Screens (catch-all) ------------
@bot.callback_query_handler(func=lambda c: True)
def on_cb(c):
    ensure_user(c.from_user)
    data=c.data or ""

    # Senden mit 2FA
    if data.startswith("send:go:"):
        _,_,mode,to_uid,to_uname,amt = data.split(":")
        to_uid=int(to_uid); amt=Decimal(amt)
        u=get_user(c.from_user.id)
        if u["twofa_enabled"]:
            code=''.join(random.choices(string.digits,k=6))
            bot.answer_callback_query(c.id, f"🔐 Code: {code}")
            msg = bot.send_message(c.message.chat.id, f"🔐 2FA – antworte mit: <code>{code}</code>")
            def check_code(m):
                if (m.text or "").strip()!=code: bot.reply_to(m,"Falscher Code."); return
                do_send(m.chat.id, c.from_user.id, to_uid, to_uname, amt, mode)
            bot.register_next_step_handler(msg, check_code)
        else:
            do_send(c.message.chat.id, c.from_user.id, to_uid, to_uname, amt, mode)
        return

    if data=="m:sup":
        msg=bot.send_message(c.message.chat.id, T(c.from_user.id,"support_prompt"))
        bot.register_next_step_handler(msg, sup_msg)
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
        txt=T(c.from_user.id,"settings", lang=u["lang"], twofa=twofa, ref=u["ref_code"])
        kb=InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🔐 2FA AN/AUS", callback_data="set:2fa"))
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