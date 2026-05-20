from playwright.sync_api import sync_playwright, TimeoutError
from datetime import datetime, timedelta
import re
import os
import json
import time
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI

CODE_VERSION = "npb_python_pair_blocks_v10_20260517"

FIXTURES_URL = "https://www.betexplorer.com/baseball/japan/npb/fixtures/"
BASE_URL = "https://www.betexplorer.com"

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1vjgGVoDYwmdEOjz8qcMFG_-7GIgg7ZISTt428ylAmmo/edit"
WORKSHEET_GID = 1879745082
HANDICAP_INPUT_GID = 1658757991

BETEXPLORER_TO_JST_HOURS = 7
HEADLESS = True
TARGET_HOURS = 48
LEAGUE_NAME = "NPB"

CHUNK_SIZE = 3
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

HANDICAP_LINES = [-3.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 3.5]

CLEAR_RANGES = [
    "A10:E70",
    "J10:AA70",
]

BLOCK_RESOURCE_TYPES = {"image", "font", "media", "stylesheet"}

KNOWN_BOOKMAKER_KEYWORDS = [
    "bet", "bet365", "betinasia", "pinnacle", "1xbet", "888sport",
    "unibet", "williamhill", "stake", "bwin", "betfair", "betmgm",
    "duelbits", "roobet", "n1bet", "megapari", "mozzartbet",
]

ALLOWED_HANDICAP_VALUES = {
    "home_0", "home_0.1", "home_0.2", "home_0.3", "home_0.4", "home_0.5", "home_0.6", "home_0.7", "home_0.8", "home_0.9",
    "home_1", "home_1.1", "home_1.2", "home_1.3", "home_1.4", "home_1.5", "home_1.6", "home_1.7", "home_1.8", "home_1.9",
    "home_2", "home_2.1", "home_2.2", "home_2.3", "home_2.4", "home_2.5", "home_2.6", "home_2.7", "home_2.8", "home_2.9",
    "home_3", "home_3.1", "home_3.2", "home_3.3", "home_3.4", "home_3.5", "home_3.6", "home_3.7", "home_3.8", "home_3.9",
    "home_0半", "home_0半1", "home_0半2", "home_0半3", "home_0半4", "home_0半5", "home_0半6", "home_0半7", "home_0半8", "home_0半9",
    "home_1半", "home_1半1", "home_1半2", "home_1半3", "home_1半4", "home_1半5", "home_1半6", "home_1半7", "home_1半8", "home_1半9",
    "home_2半", "home_2半1", "home_2半2", "home_2半3", "home_2半4", "home_2半5", "home_2半6", "home_2半7", "home_2半8", "home_2半9",
    "away_0", "away_0.1", "away_0.2", "away_0.3", "away_0.4", "away_0.5", "away_0.6", "away_0.7", "away_0.8", "away_0.9",
    "away_1", "away_1.1", "away_1.2", "away_1.3", "away_1.4", "away_1.5", "away_1.6", "away_1.7", "away_1.8", "away_1.9",
    "away_2", "away_2.1", "away_2.2", "away_2.3", "away_2.4", "away_2.5", "away_2.6", "away_2.7", "away_2.8", "away_2.9",
    "away_3", "away_3.1", "away_3.2", "away_3.3", "away_3.4", "away_3.5", "away_3.6", "away_3.7", "away_3.8", "away_3.9",
    "away_0半", "away_0半1", "away_0半2", "away_0半3", "away_0半4", "away_0半5", "away_0半6", "away_0半7", "away_0半8", "away_0半9",
    "away_1半", "away_1半1", "away_1半2", "away_1半3", "away_1半4", "away_1半5", "away_1半6", "away_1半7", "away_1半8", "away_1半9",
    "away_2半", "away_2半1", "away_2半2", "away_2半3", "away_2半4", "away_2半5", "away_2半6", "away_2半7", "away_2半8", "away_2半9",
}

NPB_TEAM_HINTS = """
NPBチーム名対応ヒント:
巨人 = Yomiuri Giants
読売 = Yomiuri Giants
ジャイアンツ = Yomiuri Giants
横浜 = Yokohama BayStars
DeNA = Yokohama BayStars
ＤｅＮＡ = Yokohama BayStars
ベイスターズ = Yokohama BayStars
阪神 = Hanshin Tigers
タイガース = Hanshin Tigers
広島 = Hiroshima Carp
カープ = Hiroshima Carp
中日 = Chunichi Dragons
ドラゴンズ = Chunichi Dragons
ヤクルト = Yakult Swallows
スワローズ = Yakult Swallows
日本ハム = Nippon Ham Fighters
日ハム = Nippon Ham Fighters
ハム = Nippon Ham Fighters
西武 = Seibu Lions
ライオンズ = Seibu Lions
楽天 = Rakuten Gold. Eagles
イーグルス = Rakuten Gold. Eagles
ソフト = Fukuoka S. Hawks
ソフトバンク = Fukuoka S. Hawks
ホークス = Fukuoka S. Hawks
ロッテ = Chiba Lotte Marines
マリーンズ = Chiba Lotte Marines
オリックス = Orix Buffaloes
バファローズ = Orix Buffaloes
"""

NPB_TEAM_ALIASES = {
    "Yomiuri Giants": [
        "巨人", "読売", "読売巨人", "読売ジャイアンツ", "ジャイアンツ", "GIANTS",
        "Giants", "Yomiuri", "Yomiuri Giants", "YG", "兎",
    ],
    "Yokohama BayStars": [
        "横浜", "横浜dena", "dena", "denaベイスターズ", "横浜denaベイスターズ",
        "ベイスターズ", "baystars", "BayStars", "Yokohama", "Yokohama BayStars",
        "DeNA", "ＤｅＮＡ", "ＤＥＮＡ", "DNA",
    ],
    "Hanshin Tigers": [
        "阪神", "阪神タイガース", "タイガース", "Tigers", "Hanshin",
        "Hanshin Tigers", "虎",
    ],
    "Hiroshima Carp": [
        "広島", "広島東洋", "広島東洋カープ", "カープ", "Carp", "Hiroshima",
        "Hiroshima Carp", "鯉",
    ],
    "Chunichi Dragons": [
        "中日", "中日ドラゴンズ", "ドラゴンズ", "Dragons", "Chunichi",
        "Chunichi Dragons", "竜",
    ],
    "Yakult Swallows": [
        "ヤクルト", "東京ヤクルト", "東京ヤクルトスワローズ", "スワローズ",
        "Yakult", "Swallows", "Yakult Swallows", "燕",
    ],
    "Nippon Ham Fighters": [
        "日本ハム", "日ハム", "ハム", "北海道日本ハム", "北海道日本ハムファイターズ",
        "ファイターズ", "Fighters", "Nippon Ham", "NipponHam", "Nippon Ham Fighters",
        "日公",
    ],
    "Seibu Lions": [
        "西武", "埼玉西武", "埼玉西武ライオンズ", "ライオンズ", "Lions",
        "Seibu", "Seibu Lions", "獅子",
    ],
    "Rakuten Gold. Eagles": [
        "楽天", "東北楽天", "東北楽天ゴールデンイーグルス", "楽天イーグルス",
        "イーグルス", "ゴールデンイーグルス", "Rakuten", "Eagles",
        "Golden Eagles", "Rakuten Eagles", "Rakuten Gold. Eagles", "Rakuten Golden Eagles",
    ],
    "Fukuoka S. Hawks": [
        "ソフト", "ソフトバンク", "福岡ソフトバンク", "福岡ソフトバンクホークス",
        "ホークス", "SB", "SoftBank", "Softbank", "Hawks", "S. Hawks",
        "Fukuoka S. Hawks", "Fukuoka SoftBank Hawks", "鷹",
    ],
    "Chiba Lotte Marines": [
        "ロッテ", "千葉ロッテ", "千葉ロッテマリーンズ", "マリーンズ",
        "Lotte", "Marines", "Chiba Lotte", "Chiba Lotte Marines", "鴎",
    ],
    "Orix Buffaloes": [
        "オリックス", "オリ", "オリク", "オリックスバファローズ", "バファローズ",
        "バッファローズ", "Buffaloes", "Buffalo", "Orix", "Orix Buffaloes", "檻",
    ],
}


def normalize_team_key(text):
    s = str(text).strip()
    s = s.replace("　", "").replace(" ", "")
    s = s.replace("・", "").replace(".", "").replace("．", "")
    s = s.replace("ー", "-").replace("－", "-")
    s = s.lower()
    s = re.sub(r"[^0-9a-zぁ-んァ-ン一-龥]", "", s)
    return s


TEAM_ALIAS_TO_CANONICAL = {}

for canonical, aliases in NPB_TEAM_ALIASES.items():
    TEAM_ALIAS_TO_CANONICAL[normalize_team_key(canonical)] = canonical
    for alias in aliases:
        TEAM_ALIAS_TO_CANONICAL[normalize_team_key(alias)] = canonical


def identify_npb_team(team_text):
    """
    胴元側の日本語/略称チーム名を、BetExplorer表記に寄せたcanonical名へ変換する。
    まず完全一致、次に部分一致。
    曖昧な場合はNone。
    """
    key = normalize_team_key(team_text)

    if not key:
        return None

    if key in TEAM_ALIAS_TO_CANONICAL:
        return TEAM_ALIAS_TO_CANONICAL[key]

    matches = []

    # aliasが含まれる / aliasに含まれる の両方を見る
    for alias_key, canonical in TEAM_ALIAS_TO_CANONICAL.items():
        if not alias_key:
            continue

        if alias_key in key or key in alias_key:
            matches.append(canonical)

    unique = sorted(set(matches))

    if len(unique) == 1:
        return unique[0]

    return None


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def normalize_text(text):
    return re.sub(r"[^a-z0-9]", "", str(text).lower())


def is_bet_in_asia(text):
    return "betinasia" in normalize_text(text)


def looks_like_bookmaker(name):
    n = normalize_text(name)
    if not n:
        return False
    if "bookmakers" in n:
        return False
    return any(k in n for k in KNOWN_BOOKMAKER_KEYWORDS)


def parse_float(value):
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def is_valid_decimal_odds(value):
    return value is not None and 1.01 < value <= 20


def get_cell_texts(row):
    try:
        return row.locator("th, td").evaluate_all(
            "(els) => els.map(e => e.innerText.trim())"
        )
    except Exception:
        return []


def parse_betexplorer_datetime(date_label, time_text):
    now = datetime.now()

    if date_label == "Today":
        base_date = now.date()
    elif date_label == "Tomorrow":
        base_date = (now + timedelta(days=1)).date()
    else:
        m = re.match(r"(\d{1,2})\.(\d{1,2})\.", date_label)
        if not m:
            return None
        day = int(m.group(1))
        month = int(m.group(2))
        base_date = datetime(now.year, month, day).date()

    hour, minute = map(int, time_text.split(":"))
    raw_dt = datetime(base_date.year, base_date.month, base_date.day, hour, minute)
    return raw_dt + timedelta(hours=BETEXPLORER_TO_JST_HOURS)


def is_within_target_hours(dt):
    now = datetime.now()
    limit = now + timedelta(hours=TARGET_HOURS)
    return now <= dt <= limit


def launch_browser(playwright):
    return playwright.chromium.launch(
        headless=HEADLESS,
        args=[
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-extensions",
            "--disable-background-networking",
            "--disable-background-timer-throttling",
            "--disable-renderer-backgrounding",
            "--disable-sync",
            "--metrics-recording-only",
            "--mute-audio",
            "--no-first-run",
            "--disable-default-apps",
            "--aggressive-cache-discard",
            "--disable-cache",
        ]
    )


def new_light_page(browser):
    page = browser.new_page()

    def block_heavy_resources(route):
        if route.request.resource_type in BLOCK_RESOURCE_TYPES:
            route.abort()
        else:
            route.continue_()

    page.route("**/*", block_heavy_resources)
    return page


def safe_goto(page, url):
    start = time.time()
    page.goto(url, wait_until="domcontentloaded", timeout=30000)

    try:
        page.wait_for_selector("table", timeout=12000)
    except TimeoutError:
        raise Exception(f"table not found: {url}")

    log(f"loaded {time.time() - start:.1f}s: {url}")


def get_fixture_rows(page):
    safe_goto(page, FIXTURES_URL)

    rows = page.locator("table tr").all()
    fixtures = []
    last_date_label = None
    last_time_text = None

    for row in rows:
        try:
            text = row.inner_text().strip()
        except Exception:
            continue

        if " - " not in text:
            continue

        parts = text.split()

        if len(parts) >= 2 and ":" in parts[1]:
            last_date_label = parts[0]
            last_time_text = parts[1]

        if last_date_label is None or last_time_text is None:
            continue

        link = row.locator("a").first
        match_name = link.inner_text().strip()
        href = link.get_attribute("href")

        if not href or " - " not in match_name:
            continue

        home, away = [x.strip() for x in match_name.split(" - ", 1)]
        start_time = parse_betexplorer_datetime(last_date_label, last_time_text)

        if start_time is None or not is_within_target_hours(start_time):
            continue

        decimal_numbers = re.findall(r"\d+\.\d+", text)
        if len(decimal_numbers) < 2:
            continue

        fixtures.append({
            "league": LEAGUE_NAME,
            "start_time_jst": start_time.strftime("%Y-%m-%d %H:%M"),
            "home": home,
            "away": away,
            "match_url": BASE_URL + href,
            "ah_url": BASE_URL + href + "#ah"
        })

    log(f"fixtures: {len(fixtures)}")
    return fixtures


def moneyline_rows_visible(page):
    rows = page.locator("table tr").all()

    for row in rows[:40]:
        texts = get_cell_texts(row)

        if len(texts) >= 6:
            bookmaker = texts[0]
            if not looks_like_bookmaker(bookmaker):
                continue

            home_ml = parse_float(texts[4])
            away_ml = parse_float(texts[5])

            if is_valid_decimal_odds(home_ml) and is_valid_decimal_odds(away_ml):
                return True

    return False


def wait_until_moneyline_loaded(page):
    for _ in range(10):
        if moneyline_rows_visible(page):
            return True
        page.wait_for_timeout(1000)
    return False


def try_show_moneyline_tab(page):
    selectors = [
        "text=Home/Away",
        "text=Moneyline",
        "text=Match Winner",
        "text=1x2",
        "text=1X2",
        "text=Odds",
    ]

    for selector in selectors:
        try:
            page.locator(selector).first.click(timeout=5000)
            page.wait_for_timeout(3000)

            if moneyline_rows_visible(page):
                return True
        except Exception:
            pass

    return False


def extract_moneyline_odds_from_current_page(page, fixture, retry=True):
    wait_until_moneyline_loaded(page)

    rows = page.locator("table tr").all()
    candidates = []

    for row in rows:
        texts = get_cell_texts(row)

        if len(texts) < 2:
            continue

        bookmaker = texts[0]

        if not looks_like_bookmaker(bookmaker):
            continue

        home_ml = None
        away_ml = None

        if len(texts) >= 6:
            home_ml = parse_float(texts[4])
            away_ml = parse_float(texts[5])

        if not is_valid_decimal_odds(home_ml) or not is_valid_decimal_odds(away_ml):
            row_text = " ".join(texts)
            nums = []

            for n in re.findall(r"\d+\.\d+", row_text):
                v = parse_float(n)
                if is_valid_decimal_odds(v):
                    nums.append(v)

            if len(nums) >= 2:
                home_ml = nums[-2]
                away_ml = nums[-1]

        if not is_valid_decimal_odds(home_ml) or not is_valid_decimal_odds(away_ml):
            continue

        candidates.append({
            "bookmaker": bookmaker,
            "is_bia": is_bet_in_asia(bookmaker),
            "home_ml": home_ml,
            "away_ml": away_ml
        })

    if not candidates and retry:
        log(f"ML候補なし・Home/Away再試行: {fixture['home']} vs {fixture['away']}")
        try_show_moneyline_tab(page)
        wait_until_moneyline_loaded(page)
        return extract_moneyline_odds_from_current_page(page, fixture, retry=False)

    if not candidates:
        log(f"ML候補なし: {fixture['home']} vs {fixture['away']}")
        for i, row in enumerate(rows[:8]):
            texts = get_cell_texts(row)
            log(f"ML DEBUG ROW {i}: {texts}")
        raise Exception("ML candidate not found")

    selected = next((c for c in candidates if c["is_bia"]), candidates[0])

    log(
        f"ML {fixture['home']} vs {fixture['away']} "
        f"/ {selected['bookmaker']} "
        f"/ {selected['home_ml']} - {selected['away_ml']}"
    )

    return {
        "home_ml": selected["home_ml"],
        "away_ml": selected["away_ml"]
    }


def ah_lines_visible(page):
    rows = page.locator("table tr").all()

    for row in rows[:80]:
        try:
            text = row.inner_text(timeout=3000)
        except Exception:
            continue

        if (
            "-1.5" in text
            or "+1.5" in text
            or "-2.5" in text
            or "+2.5" in text
            or "-3.5" in text
            or "+3.5" in text
        ):
            return True

    return False


def wait_until_ah_loaded(page):
    for _ in range(5):
        if ah_lines_visible(page):
            return True
        page.wait_for_timeout(800)
    return False


def ensure_ah_tab(page, ah_url):
    if wait_until_ah_loaded(page):
        return

    ah_selectors = [
        "text=AH",
        "text=Asian Handicap",
    ]

    for selector in ah_selectors:
        try:
            page.locator(selector).first.click(timeout=3000)
            if wait_until_ah_loaded(page):
                return
        except Exception:
            pass

    try:
        page.goto(ah_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_selector("table", timeout=12000)

        if wait_until_ah_loaded(page):
            return
    except Exception:
        pass

    for selector in ah_selectors:
        try:
            page.locator(selector).first.click(timeout=3000)
            if wait_until_ah_loaded(page):
                return
        except Exception:
            pass

    raise Exception("AH tab not visible")


def extract_ah_odds_from_current_page(page, fixture):
    ensure_ah_tab(page, fixture["ah_url"])

    rows = page.locator("table tr").all()
    candidates = {line: [] for line in HANDICAP_LINES}

    for row in rows:
        texts = get_cell_texts(row)

        if len(texts) < 2:
            continue

        bookmaker = texts[0]

        if not looks_like_bookmaker(bookmaker):
            continue

        line = None
        home_odds = None
        away_odds = None

        if len(texts) >= 7:
            line = parse_float(texts[4])
            home_odds = parse_float(texts[5])
            away_odds = parse_float(texts[6])

        if line is None or line not in HANDICAP_LINES:
            continue

        if not is_valid_decimal_odds(home_odds) or not is_valid_decimal_odds(away_odds):
            continue

        candidates[line].append({
            "bookmaker": bookmaker,
            "is_bia": is_bet_in_asia(bookmaker),
            "home": home_odds,
            "away": away_odds
        })

    result = {}

    for line in HANDICAP_LINES:
        result[f"home_ah_{line:+.1f}"] = ""
        result[f"away_ah_{line:+.1f}"] = ""

    result["home_ah_-0.5"] = fixture["home_ml"]
    result["home_ah_+0.5"] = fixture["home_ml"]
    result["away_ah_-0.5"] = fixture["away_ml"]
    result["away_ah_+0.5"] = fixture["away_ml"]

    for line in HANDICAP_LINES:
        if line in [-0.5, 0.5]:
            continue

        if not candidates[line]:
            continue

        selected = next((c for c in candidates[line] if c["is_bia"]), candidates[line][0])

        log(
            f"AH {fixture['home']} vs {fixture['away']} "
            f"/ line={line:+.1f} "
            f"/ {selected['bookmaker']} "
            f"/ {selected['home']} - {selected['away']}"
        )

        result[f"home_ah_{line:+.1f}"] = selected["home"]
        result[f"away_ah_{-line:+.1f}"] = selected["away"]

    return result


def empty_ah_result(fixture):
    result = {}

    for line in HANDICAP_LINES:
        result[f"home_ah_{line:+.1f}"] = ""
        result[f"away_ah_{line:+.1f}"] = ""

    result["home_ah_-0.5"] = fixture.get("home_ml", "")
    result["home_ah_+0.5"] = fixture.get("home_ml", "")
    result["away_ah_-0.5"] = fixture.get("away_ml", "")
    result["away_ah_+0.5"] = fixture.get("away_ml", "")

    return result


def get_gspread_client():
    credentials_json = os.environ.get("GOOGLE_CREDENTIALS")

    if credentials_json:
        credentials_info = json.loads(credentials_json)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        return gspread.authorize(creds)

    return gspread.service_account(filename="credentials.json")


def get_worksheet_by_gid(spreadsheet, gid):
    for ws in spreadsheet.worksheets():
        if ws.id == gid:
            return ws
    raise Exception(f"Worksheet not found: gid={gid}")


def read_handicap_raw_text(spreadsheet):
    try:
        ws = get_worksheet_by_gid(spreadsheet, HANDICAP_INPUT_GID)
        values = ws.get("A1:Z100")
    except Exception as e:
        log(f"ハンデ入力シート読み取り失敗: {e}")
        return ""

    lines = []
    for row in values:
        for cell in row:
            text = str(cell).strip()
            if text:
                lines.append(text)

    return "\n".join(lines).strip()


def normalize_extracted_lines_text(text):
    lines = []
    for line in str(text).splitlines():
        line = line.strip()
        if line:
            lines.append(line)
    return "\n".join(lines).strip()


def get_time_text_from_line(line):
    """
    13:00 または 13:00<0> のような行から時刻部分だけを取り出す。
    """
    m = re.match(r"^(\d{1,2}:\d{2})(?:\s*<[^<>]+>)?$", str(line).strip())
    if not m:
        return ""
    return m.group(1)


def is_time_line(line):
    return bool(get_time_text_from_line(line))


def parse_handicap_token_from_line(line):
    """
    チーム行から <07>, <1.2>, <1半3> などを抽出する。
    戻り値:
      (team_text_without_token, token_text, handicap_value_text)
    handicap_value_text は許可リストにある値のうち side を除いた部分。
    """
    line = str(line).strip()
    m = re.search(r"<([^<>]+)>", line)

    if not m:
        return line, None, None

    token = m.group(1).strip()
    team_text = (line[:m.start()] + line[m.end():]).strip()

    # 全角数字などを軽く補正
    token_norm = (
        token.replace("０", "0")
        .replace("１", "1")
        .replace("２", "2")
        .replace("３", "3")
        .replace("４", "4")
        .replace("５", "5")
        .replace("６", "6")
        .replace("７", "7")
        .replace("８", "8")
        .replace("９", "9")
        .replace("．", ".")
        .replace(" ", "")
        .replace("　", "")
    )

    # 半を含む表記はそのまま残す。例: 1半3
    if "半" in token_norm:
        value = token_norm
        return team_text, token, value

    # 05, 04, 03 などは 0.5, 0.4, 0.3
    if re.fullmatch(r"\d{2}", token_norm):
        value = f"0.{int(token_norm)}"
        # 05 -> 0.5, 07 -> 0.7
        value = str(float(value)).rstrip("0").rstrip(".")
        return team_text, token, value

    # 5 のような1桁だけ来た場合は 0.5 と解釈
    if re.fullmatch(r"\d", token_norm):
        value = f"0.{token_norm}"
        value = str(float(value)).rstrip("0").rstrip(".")
        return team_text, token, value

    # 1.2 など
    if re.fullmatch(r"\d+(?:\.\d+)?", token_norm):
        num = float(token_norm)
        if num.is_integer():
            value = str(int(num))
        else:
            value = str(num).rstrip("0").rstrip(".")
        return team_text, token, value

    return team_text, token, None


def strip_handicap_token(line):
    line = str(line).strip()
    return re.sub(r"<[^<>]+>", "", line).strip()


def is_known_npb_team_line(line):
    """
    NPB用。<...> を外した行がNPBチーム名として認識できるか。
    """
    if is_time_line(line):
        return False
    team_text = strip_handicap_token(line)
    return identify_npb_team(team_text) is not None


def filter_npb_relevant_lines(raw_text):
    """
    AIに抽出させず、Pythonで必要そうな行だけ残す。
    - NPBチーム名として認識できる行
    - 13:00 / 13:00<0> の時刻行
    だけを残す。
    """
    lines = []
    for line in str(raw_text).splitlines():
        line = str(line).strip()
        if not line:
            continue
        if is_time_line(line) or is_known_npb_team_line(line):
            lines.append(line)
    return "\n".join(lines).strip()


def build_match_blocks(text):
    """
    NPB用ブロック化。
    以下の両方に対応する。
    - チーム / 時刻 / チーム
    - チーム / 時刻<0> / チーム
    - チーム / チーム  （時刻が省略されたカード）
    行の内容自体は変更しない。
    """
    lines = [
        str(line).strip()
        for line in str(text).splitlines()
        if str(line).strip()
    ]

    blocks = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # チーム / 時刻 / チーム
        if (
            i + 2 < len(lines)
            and is_known_npb_team_line(lines[i])
            and is_time_line(lines[i + 1])
            and is_known_npb_team_line(lines[i + 2])
        ):
            blocks.append([lines[i], lines[i + 1], lines[i + 2]])
            i += 3
            continue

        # チーム / チーム
        if (
            i + 1 < len(lines)
            and is_known_npb_team_line(lines[i])
            and is_known_npb_team_line(lines[i + 1])
        ):
            blocks.append([lines[i], lines[i + 1]])
            i += 2
            continue

        # どちらにも当てはまらない行は単独で残すが、基本的に後段では無視される
        blocks.append([line])
        i += 1

    return blocks


def format_blocks_for_sheet(text):
    blocks = build_match_blocks(text)

    values = []
    for block_index, block in enumerate(blocks):
        if block_index > 0:
            values.append([""])

        for line in block:
            values.append([line])

    return values


def format_blocks_for_ai(text):
    blocks = build_match_blocks(text)

    parts = []
    for i, block in enumerate(blocks, start=1):
        parts.append(f"MATCH {i}:\n" + "\n".join(block))

    return "\n\n".join(parts).strip()


def extract_relevant_handicap_lines_with_openai(raw_text):
    """
    胴元原文から必要行だけを抜き出す。
    重要: チーム名行や <...> 付き行は絶対に書き換えず、原文の行をそのまま残す。
    """
    api_key = os.environ.get("OPENAI_API_KEY")

    if not api_key:
        log("OPENAI_API_KEY未設定のため、ハンデ入力整形をスキップ")
        return raw_text

    if not raw_text:
        return ""

    prompt = f"""
あなたは胴元メッセージから、野球の試合カード情報だけを抜き出す抽出器です。

目的:
不要な説明行・見出し行を除外し、試合カードを表す行だけを残してください。

絶対ルール:
- 原文に存在する行だけを出力してください。
- チーム名が書かれた行を一文字も変更してはいけません。
- <...> が付いている行を一文字も変更してはいけません。
- 13:00<0> のように、時刻行に <0> が付く場合があります。
- 13:00<0> は試合時刻行なので、必ず原文のまま残してください。
- 13:00<0> を 13:00 と <0> に分離してはいけません。
- 13:00<0> の <0> をチーム行へ移動してはいけません。
- 13:00<0> を 13:00 に書き換えてはいけません。
- <...> を別の行・別のチームへ移動してはいけません。
- <...> の中身を変更してはいけません。
- チーム名を英語に変換してはいけません。
- チーム名を正式名称に補完してはいけません。
- 行の順番を変えてはいけません。
- 不要行を削除するだけにしてください。
- 判断に迷う行は残してください。

残すべき行:
- チーム名と思われる行
- <05> や <1.2> や <1半3> のようなハンデ付きチーム行
- 13:00 のような試合時刻行
- 13:00<0> のような <0> 付き試合時刻行

消してよい行:
- 延長なし
- 試合開始◯分前締切
- 締切説明
- 見出し
- 空行
- URL
- メニュー表示

出力形式:
- JSONのみ
- {{"lines": ["原文の行1", "原文の行2"]}} の形式
- linesの各要素は原文の1行をそのまま入れてください。

胴元原文:
{raw_text}
"""

    try:
        client = OpenAI(api_key=api_key)

        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "Return strict JSON only. Extract lines only. Do not rewrite any line."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )

        content = response.choices[0].message.content
        data = json.loads(content)
        lines = data.get("lines", [])

        if not isinstance(lines, list):
            return raw_text

        extracted = "\n".join(str(line).strip() for line in lines if str(line).strip())
        return normalize_extracted_lines_text(extracted) or raw_text

    except Exception as e:
        log(f"OpenAIハンデ入力整形失敗: {e}")
        return raw_text


def write_formatted_handicap_input(spreadsheet, formatted_text):
    try:
        ws = get_worksheet_by_gid(spreadsheet, HANDICAP_INPUT_GID)
        ws.batch_clear(["A1:Z100"])

        values = format_blocks_for_sheet(formatted_text)

        if values:
            ws.update("A1", values)

        log("ハンデ入力シートを試合ごと空行区切りで更新")
    except Exception as e:
        log(f"ハンデ入力シート更新失敗: {e}")


def normalize_handicap_value(value):
    if value is None:
        return ""

    text = str(value).strip()
    text = text.replace(" ", "").replace("　", "")

    m = re.match(r"^(home|away)_([0-9]+(?:\.[0-9]+)?)$", text)

    if m:
        side = m.group(1)
        num = float(m.group(2))

        if num.is_integer():
            text = f"{side}_{int(num)}"
        else:
            num_text = str(num).rstrip("0").rstrip(".")
            text = f"{side}_{num_text}"

    if text not in ALLOWED_HANDICAP_VALUES:
        log(f"許可外ハンデのため破棄: {value}")
        return ""

    return text


def extract_handicap_blocks(formatted_text):
    """
    ブロックからハンデ付きチームとハンデ値をPythonで抽出する。
    数字・<...> の位置はAIに判断させない。

    特例:
    - 13:00<0> のように時刻行に <0> が付く場合は、その試合の home_0 として扱う。
    """
    blocks = build_match_blocks(formatted_text)
    extracted = []

    for block_index, block in enumerate(blocks, start=1):
        time_text = ""
        teams = []
        handicap_team = ""
        handicap_value = ""
        forced_side = ""

        for line in block:
            if is_time_line(line):
                time_text = get_time_text_from_line(line)

                # 時刻行に <0> がある場合は、home_0として扱う
                _, token, value = parse_handicap_token_from_line(line)
                if token is not None:
                    token_value = value or ""
                    if token_value in ["0", "0.0"]:
                        handicap_value = "0"
                        forced_side = "home"
                        handicap_team = "__HOME__"

                continue

            team_text, token, value = parse_handicap_token_from_line(line)
            teams.append(team_text)

            if token is not None:
                handicap_team = team_text
                handicap_value = value or ""
                forced_side = ""

        if not handicap_value:
            continue

        # 時刻行<0>の場合はhome固定。チーム名はブロック内2チームからカード照合する。
        if forced_side == "home":
            handicap_team = teams[0] if teams else "__HOME__"

        if not handicap_team:
            continue

        extracted.append({
            "block_id": block_index,
            "time": time_text,
            "teams": teams,
            "handicap_team": handicap_team,
            "handicap_value": handicap_value,
            "forced_side": forced_side,
            "raw_block": "\n".join(block),
        })

    return extracted


def match_handicap_blocks_with_python(blocks, fixtures):
    """
    NPB用。AIを使わず、辞書でハンデ付きチームと対戦カードを照合する。
    - ハンデ値はPythonで抽出済み
    - <...>付きチームもPythonで抽出済み
    - home/awayも現在のfixturesのhome/away列でPythonが決める
    - 時刻行<0>はforced_side=homeとして扱う
    """
    if not blocks:
        log("ハンデ付きブロックなし")
        return []

    fixture_records = []

    for i, f in enumerate(fixtures):
        row_number = 10 + i

        fixture_records.append({
            "row": row_number,
            "time": f.get("start_time_jst", "")[-5:],
            "home": f.get("home", ""),
            "away": f.get("away", ""),
            "home_canonical": f.get("home", ""),
            "away_canonical": f.get("away", ""),
        })

    final_items = []

    for block in blocks:
        block_id = int(block["block_id"])
        block_time = block.get("time", "")
        teams_raw = block.get("teams", [])
        handicap_team_raw = block.get("handicap_team", "")
        handicap_value = str(block.get("handicap_value", "")).strip()
        forced_side = str(block.get("forced_side", "")).strip()

        canonical_teams = []
        for t in teams_raw:
            canonical = identify_npb_team(t)
            if canonical:
                canonical_teams.append(canonical)

        canonical_teams = list(dict.fromkeys(canonical_teams))

        if forced_side == "home":
            handicap_canonical = "__HOME__"
        else:
            handicap_canonical = identify_npb_team(handicap_team_raw)

        log(
            f"辞書照合 block={block_id} "
            f"teams={teams_raw}->{canonical_teams} "
            f"handicap_team={handicap_team_raw}->{handicap_canonical} "
            f"value={handicap_value} forced_side={forced_side}"
        )

        if not handicap_canonical or not handicap_value:
            continue

        if len(canonical_teams) < 2:
            continue

        candidate_fixtures = []

        for fx in fixture_records:
            fixture_team_set = {fx["home_canonical"], fx["away_canonical"]}
            block_team_set = set(canonical_teams)

            if not block_team_set.issubset(fixture_team_set):
                continue

            time_score = 1 if block_time and fx["time"] == block_time else 0
            candidate_fixtures.append((time_score, fx))

        if not candidate_fixtures:
            continue

        candidate_fixtures.sort(key=lambda x: x[0], reverse=True)
        best_score = candidate_fixtures[0][0]
        best = [fx for score, fx in candidate_fixtures if score == best_score]

        if len(best) != 1:
            log(f"候補が曖昧なためスキップ block={block_id}: {best}")
            continue

        fx = best[0]

        if forced_side == "home":
            side = "home"
        elif handicap_canonical == fx["home_canonical"]:
            side = "home"
        elif handicap_canonical == fx["away_canonical"]:
            side = "away"
        else:
            continue

        handicap = normalize_handicap_value(f"{side}_{handicap_value}")

        if not handicap:
            continue

        final_items.append({
            "row": fx["row"],
            "handicap": handicap,
            "block_id": block_id,
            "raw_block": block.get("raw_block", ""),
        })

    log("最終ハンデ反映候補:")
    log(json.dumps(final_items, ensure_ascii=False, indent=2))

    return final_items


def parse_handicaps_with_openai(formatted_text, fixtures):
    """
    互換用関数名。
    NPBではAI照合を使わず、Python辞書でrow/home/awayまで決定する。
    AIは原文から不要行を抜き出す処理にのみ使用する。
    """
    formatted_text = normalize_extracted_lines_text(formatted_text)

    if not formatted_text:
        log("ハンデ入力テキストなし")
        return []

    blocks = extract_handicap_blocks(formatted_text)

    log("Python抽出ハンデブロック:")
    log(json.dumps(blocks, ensure_ascii=False, indent=2))

    return match_handicap_blocks_with_python(blocks, fixtures)


def apply_handicaps_to_sheet(spreadsheet, worksheet, fixtures):
    raw_text = read_handicap_raw_text(spreadsheet)

    if not raw_text:
        log("ハンデ入力テキストなし")
        return

    # NPBはAI抽出でブロックが崩れるリスクがあるため、Pythonのみで必要行を抽出する
    formatted_text = filter_npb_relevant_lines(raw_text)
    formatted_text = normalize_extracted_lines_text(formatted_text)

    if not formatted_text:
        log("ハンデ入力テキストはPython抽出後に空")
        return

    write_formatted_handicap_input(spreadsheet, formatted_text)

    items = parse_handicaps_with_openai(formatted_text, fixtures)

    if not items:
        log("反映対象ハンデなし")
        return

    valid_rows = set(range(10, 10 + len(fixtures)))
    updates = []

    for item in items:
        try:
            row = int(item.get("row"))
        except Exception:
            continue

        if row not in valid_rows:
            continue

        handicap = normalize_handicap_value(item.get("handicap"))
        if not handicap:
            continue

        updates.append({
            "range": f"E{row}",
            "values": [[handicap]]
        })

    if not updates:
        log("有効なハンデ更新なし")
        return

    worksheet.batch_update(updates)
    log(f"ハンデ自動入力: {len(updates)}件")


def write_to_sheet(data):
    gc = get_gspread_client()
    sh = gc.open_by_url(SPREADSHEET_URL)
    ws = get_worksheet_by_gid(sh, WORKSHEET_GID)

    ws.batch_clear(CLEAR_RANGES)

    left_values = []
    right_values = []

    for r in data:
        left_values.append([
            r["league"],
            r["start_time_jst"],
            r["home"],
            r["away"],
            "",
        ])

        right_values.append([
            r["match_url"],
            r["ah_url"],
            r["home_ah_-3.5"],
            r["home_ah_-2.5"],
            r["home_ah_-1.5"],
            r["home_ah_-0.5"],
            r["home_ah_+0.5"],
            r["home_ah_+1.5"],
            r["home_ah_+2.5"],
            r["home_ah_+3.5"],
            r["away_ah_+3.5"],
            r["away_ah_+2.5"],
            r["away_ah_+1.5"],
            r["away_ah_+0.5"],
            r["away_ah_-0.5"],
            r["away_ah_-1.5"],
            r["away_ah_-2.5"],
            r["away_ah_-3.5"],
        ])

    if left_values:
        ws.update("A10", left_values)

    if right_values:
        ws.update("J10", right_values)

    apply_handicaps_to_sheet(sh, ws, data)


def process_fixture(page, f):
    log(f"start {f['home']} vs {f['away']}")
    start = time.time()

    try:
        safe_goto(page, f["match_url"])
        ml = extract_moneyline_odds_from_current_page(page, f)
        f = {**f, **ml}

    except Exception as e:
        log(f"ML取得失敗: {f['home']} vs {f['away']} / {e}")
        f = {**f, "home_ml": "", "away_ml": ""}
        ah = empty_ah_result(f)
        return {**f, **ah}

    try:
        ah = extract_ah_odds_from_current_page(page, f)
        result = {**f, **ah}

    except Exception as e:
        log(f"AH取得失敗: {f['home']} vs {f['away']} / {e}")
        ah = empty_ah_result(f)
        result = {**f, **ah}

    log(f"done {f['home']} vs {f['away']} / {time.time() - start:.1f}s")
    return result


def run_job():
    log(f"CODE_VERSION: {CODE_VERSION}")

    results = []
    total_start = time.time()

    with sync_playwright() as p:
        browser = launch_browser(p)
        page = new_light_page(browser)

        try:
            fixtures = get_fixture_rows(page)
        finally:
            browser.close()

        for i in range(0, len(fixtures), CHUNK_SIZE):
            chunk = fixtures[i:i + CHUNK_SIZE]
            log(f"chunk start {i + 1}-{i + len(chunk)} / {len(fixtures)}")

            browser = launch_browser(p)
            page = new_light_page(browser)

            try:
                for f in chunk:
                    results.append(process_fixture(page, f))
            finally:
                browser.close()
                log("browser restarted")

    write_to_sheet(results)
    log(f"完了: {len(results)} 件 / {time.time() - total_start:.1f}s")
    return results
