from playwright.sync_api import sync_playwright, TimeoutError
from datetime import datetime, timedelta
import re
import os
import json
import time
import gspread
from google.oauth2.service_account import Credentials

FIXTURES_URL = "https://www.betexplorer.com/baseball/usa/mlb/fixtures/"
BASE_URL = "https://www.betexplorer.com"

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1vjgGVoDYwmdEOjz8qcMFG_-7GIgg7ZISTt428ylAmmo/edit"
WORKSHEET_GID = 1434161650

BETEXPLORER_TO_JST_HOURS = 7
HEADLESS = True
TARGET_HOURS = 48
LEAGUE_NAME = "MLB"

CHUNK_SIZE = 3

HANDICAP_LINES = [-3.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 3.5]

CLEAR_RANGES = [
    "A10:E70",
    "J10:AA70",
]

BLOCK_RESOURCE_TYPES = {"image", "font", "media", "stylesheet"}


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def normalize_text(text):
    return re.sub(r"[^a-z0-9]", "", text.lower())


def is_bet_in_asia(text):
    return "betinasia" in normalize_text(text)


def parse_float(value):
    try:
        return float(value)
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

    page.goto(
        url,
        wait_until="domcontentloaded",
        timeout=30000
    )

    try:
        page.wait_for_selector("table", timeout=12000)
    except TimeoutError:
        raise Exception(f"table not found: {url}")

    elapsed = time.time() - start
    log(f"loaded {elapsed:.1f}s: {url}")


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

        if not href:
            continue

        if " - " not in match_name:
            continue

        home, away = [x.strip() for x in match_name.split(" - ", 1)]

        start_time = parse_betexplorer_datetime(last_date_label, last_time_text)

        if start_time is None:
            continue

        if not is_within_target_hours(start_time):
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


def extract_moneyline_odds_from_current_page(page, fixture):
    rows = page.locator("table tr").all()
    candidates = []

    for row in rows:
        texts = get_cell_texts(row)

        if len(texts) < 6:
            continue

        bookmaker = texts[0]

        if not bookmaker:
            continue

        if "bookmakers" in bookmaker.lower():
            continue

        home_ml = parse_float(texts[4])
        away_ml = parse_float(texts[5])

        if not is_valid_decimal_odds(home_ml):
            continue

        if not is_valid_decimal_odds(away_ml):
            continue

        candidates.append({
            "bookmaker": bookmaker,
            "is_bia": is_bet_in_asia(bookmaker),
            "home_ml": home_ml,
            "away_ml": away_ml
        })

    if not candidates:
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
        "text=Asian Handicap",
        "text=AH",
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

        if len(texts) < 7:
            continue

        bookmaker = texts[0]

        if not bookmaker:
            continue

        if "bookmakers" in bookmaker.lower():
            continue

        line = parse_float(texts[4])

        if line is None:
            continue

        if line not in HANDICAP_LINES:
            continue

        home_odds = parse_float(texts[5])
        away_odds = parse_float(texts[6])

        if not is_valid_decimal_odds(home_odds):
            continue

        if not is_valid_decimal_odds(away_odds):
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

        creds = Credentials.from_service_account_info(
            credentials_info,
            scopes=scopes
        )

        return gspread.authorize(creds)

    return gspread.service_account(filename="credentials.json")


def get_worksheet_by_gid(spreadsheet, gid):
    for ws in spreadsheet.worksheets():
        if ws.id == gid:
            return ws

    raise Exception("Worksheet not found")


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


def process_fixture(page, f):
    log(f"start {f['home']} vs {f['away']}")

    start = time.time()

    try:
        safe_goto(page, f["match_url"])
        ml = extract_moneyline_odds_from_current_page(page, f)
        f = {**f, **ml}

    except Exception as e:
        log(f"ML取得失敗: {f['home']} vs {f['away']} / {e}")

        f = {
            **f,
            "home_ml": "",
            "away_ml": ""
        }

        ah = empty_ah_result(f)
        return {**f, **ah}

    try:
        ah = extract_ah_odds_from_current_page(page, f)
        result = {**f, **ah}

    except Exception as e:
        log(f"AH取得失敗: {f['home']} vs {f['away']} / {e}")
        ah = empty_ah_result(f)
        result = {**f, **ah}

    elapsed = time.time() - start
    log(f"done {f['home']} vs {f['away']} / {elapsed:.1f}s")

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

    total_elapsed = time.time() - total_start
    log(f"完了: {len(results)} 件 / {total_elapsed:.1f}s")

    return results