from playwright.sync_api import sync_playwright, TimeoutError
from datetime import datetime, timedelta
import re
import os
import json
import time
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI

FIXTURES_URL = "https://www.betexplorer.com/baseball/usa/mlb/fixtures/"
BASE_URL = "https://www.betexplorer.com"

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1vjgGVoDYwmdEOjz8qcMFG_-7GIgg7ZISTt428ylAmmo/edit"
WORKSHEET_GID = 1434161650
HANDICAP_INPUT_GID = 1658757991

BETEXPLORER_TO_JST_HOURS = 7
HEADLESS = True
TARGET_HOURS = 48
LEAGUE_NAME = "MLB"

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
    "draftkings", "fanduel", "caesars", "pointsbet", "betrivers",
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

        # オッズ未掲載試合は対象外
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


def clean_handicap_text(raw_text):
    """
    胴元メッセージから、AIが誤解しやすい見出し・締切文などを除去する。
    チーム名、時刻、<05> などのハンデ表記を含む行を優先して残す。
    """
    if not raw_text:
        return ""

    remove_keywords = [
        "延長なし",
        "延長無し",
        "試合開始",
        "締切",
        "締め切り",
        "〆切",
        "受付",
        "Menu",
        "メニュー",
        "対象",
        "野球",
        "プロ野球",
        "本日",
        "明日",
        "以下",
        "ハンデ",
    ]

    cleaned_lines = []

    for line in raw_text.splitlines():
        line = str(line).strip()

        if not line:
            continue

        if line.startswith("http"):
            continue

        # 「MLB」「NPB」はリーグ見出しとして消す。ただしチーム名付きやハンデ付きなら残す。
        if line in ["MLB", "ＮＰＢ", "NPB", "ＭＬＢ"]:
            continue

        # 明らかな説明・見出し行を除外。ただし <...> がある行は残す。
        if any(k in line for k in remove_keywords):
            if "<" not in line or ">" not in line:
                continue

        # 記号だけの行を除外
        if re.fullmatch(r"[-_=ー－・●○◆◇■□★☆\s]+", line):
            continue

        cleaned_lines.append(line)

    return "\n".join(cleaned_lines).strip()


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


def parse_handicaps_with_openai(raw_text, fixtures):
    api_key = os.environ.get("OPENAI_API_KEY")

    if not api_key:
        log("OPENAI_API_KEY未設定のため、ハンデ自動入力をスキップ")
        return []

    cleaned_text = clean_handicap_text(raw_text)

    if not cleaned_text:
        log("ハンデ入力テキストは前処理後に空")
        return []

    log("ハンデ入力テキスト前処理後:")
    log(cleaned_text)

    match_lines = []

    for i, f in enumerate(fixtures):
        row_number = 10 + i
        match_lines.append(
            f"{row_number}: {f['start_time_jst']} | {f['home']} vs {f['away']}"
        )

    allowed_values_text = "\n".join(sorted(ALLOWED_HANDICAP_VALUES))

    prompt = f"""
あなたは日本語の胴元ハンデ情報を、Google Sheets入力用のJSONに変換するアシスタントです。

対象リーグは MLB のみです。
以下の「現在の試合一覧」に存在する試合だけを対象にしてください。
他リーグの情報、前日情報、対戦相手が一致しない情報、判断が曖昧な情報は出力しないでください。

重要:
- 出力はJSONのみ。
- "items" という配列だけを返してください。
- 各要素は {{"row": 行番号, "handicap": "home_0.5"}} の形式。
- handicap は必ず「許可されるhandicap値」から完全一致で1つ選んでください。
- 許可リストにない値は絶対に出力しないでください。
- <> が付いているチームがハンデを受けている側です。
- homeチームに <> が付いていれば home側の値。
- awayチームに <> が付いていれば away側の値。
- <05> は 0.5、<04> は 0.4、<03> は 0.3、<1.2> は 1.2 と解釈。
- <0半> は 0半、<1半3> は 1半3 のように、半を含む表記は小数に変換せず、許可リストの形式で出力してください。
- チーム名は日本語・カタカナ・略称でもよいが、必ず現在の試合一覧と照合してください。
- 対戦相手が違う場合は出力しないでください。
- 該当不明なら出力しないでください。

追加ルール:
- 前処理済みメッセージは、基本的に「チーム行」「時刻行」「チーム行」の3行セットです。
- <...> が付いていないチームにはハンデを付けないでください。
- 時刻行だけで試合を判断しないでください。
- 直前・直後にある無関係な行をチーム名として扱わないでください。
- 対戦する2チームが現在の試合一覧の home/away と一致する場合だけ出力してください。
- 片方のチーム名だけ一致しても、対戦相手が一致しなければ出力しないでください。
- MLBチーム名はカタカナ・略称で届くことがあります。例: ドジャース=Los Angeles Dodgers、ヤンキース=New York Yankees、メッツ=New York Mets、カブス=Chicago Cubs、レッドソックス=Boston Red Sox。

現在の試合一覧:
{chr(10).join(match_lines)}

許可されるhandicap値:
{allowed_values_text}

前処理済み胴元メッセージ:
{cleaned_text}
"""

    try:
        client = OpenAI(api_key=api_key)

        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "Return strict JSON only. Do not guess when uncertain."
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

        items = data.get("items", [])
        if not isinstance(items, list):
            return []

        return items

    except Exception as e:
        log(f"OpenAIハンデ解析失敗: {e}")
        return []


def apply_handicaps_to_sheet(spreadsheet, worksheet, fixtures):
    raw_text = read_handicap_raw_text(spreadsheet)

    if not raw_text:
        log("ハンデ入力テキストなし")
        return

    items = parse_handicaps_with_openai(raw_text, fixtures)

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
