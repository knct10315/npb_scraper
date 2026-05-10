from fastapi import FastAPI
from playwright.sync_api import sync_playwright, TimeoutError
from datetime import datetime, timedelta
import re
import os
import json
import gspread
from google.oauth2.service_account import Credentials

app = FastAPI()

FIXTURES_URL = "https://www.betexplorer.com/baseball/japan/npb/fixtures/"
BASE_URL = "https://www.betexplorer.com"

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1vjgGVoDYwmdEOjz8qcMFG_-7GIgg7ZISTt428ylAmmo/edit"
WORKSHEET_GID = 1879745082

BETEXPLORER_TO_JST_HOURS = 7
HEADLESS = True
TARGET_HOURS = 48

HANDICAP_LINES = [-3.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 3.5]

CLEAR_RANGES = [
    "A10:E70",
    "J10:AA70",
]

BLOCK_RESOURCE_TYPES = {"image", "font", "media"}


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/run")
def run_scraping():
    results = run_job()
    return {
        "status": "completed",
        "match_count": len(results),
        "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


def normalize_text(text):
    return re.sub(r"[^a-z0-9]", "", text.lower())


def is_bet_in_asia(text):
    normalized = normalize_text(text)
    return "betinasia" in normalized


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
    page.goto(url, wait_until="domcontentloaded", timeout=60000)

    try:
        page.wait_for_selector("table", timeout=20000)
    except TimeoutError:
        raise Exception(f"tableが見つかりません: {url}")


def get_fixture_rows(page):
    safe_goto(page, FIXTURES_URL)

    rows = page.locator("table tr").all()

    fixtures = []
    last_date_label = None
    last_time_text = None

    for row in rows:
        text = row.inner_text().strip()

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

        # fixtures上にMLらしき数字が無い試合は、まだオッズ無しとしてスキップ
        decimal_numbers = re.findall(r"\d+\.\d+", text)
        if len(decimal_numbers) < 2:
            continue

        fixtures.append({
            "league": "NPB",
            "start_time_jst": start_time.strftime("%Y-%m-%d %H:%M"),
            "home": home,
            "away": away,
            "match_url": BASE_URL + href,
            "ah_url": BASE_URL + href + "#ah"
        })

    return fixtures


def extract_odds_numbers(text):
    values = []

    for n in re.findall(r"[-+]?\d+\.\d+", text):
        try:
            v = float(n)
        except ValueError:
            continue

        if 1.01 <= v <= 20:
            values.append(v)

    return values


def extract_moneyline_odds(page, fixture):
    """
    試合詳細ページから業者別MLを取得。
    BetInAsia優先、なければ一番上。
    """
    safe_goto(page, fixture["match_url"])

    rows = page.locator("table tr").all()

    candidates = []

    for row in rows:
        text = row.inner_text().strip()

        if not text:
            continue

        # AH行などをなるべく除外
        if any(token in text for token in ["+0.5", "-0.5", "+1.5", "-1.5", "+2.5", "-2.5", "+3.5", "-3.5"]):
            continue

        odds = extract_odds_numbers(text)

        if len(odds) < 2:
            continue

        candidates.append({
            "is_bia": is_bet_in_asia(text),
            "home_ml": odds[-2],
            "away_ml": odds[-1],
            "raw_text": text
        })

    if not candidates:
        raise Exception("ML候補が見つかりません")

    selected = next(
        (c for c in candidates if c["is_bia"]),
        candidates[0]
    )

    print(f"ML選択: {fixture['home']} vs {fixture['away']} / BIA={selected['is_bia']} / {selected['home_ml']} - {selected['away_ml']}")

    return {
        "home_ml": selected["home_ml"],
        "away_ml": selected["away_ml"]
    }


def extract_ah_odds(page, fixture):
    """
    AHページから業者別AHを取得。
    各ラインごとにBetInAsia優先、なければ一番上。
    """
    safe_goto(page, fixture["ah_url"])

    rows = page.locator("table tr").all()

    candidates = {line: [] for line in HANDICAP_LINES}

    for row in rows:
        text = row.inner_text().strip()

        if not text:
            continue

        numbers = re.findall(r"[-+]?\d+\.\d+", text)

        found_lines = []

        for n in numbers:
            try:
                v = float(n)
            except ValueError:
                continue

            if v in HANDICAP_LINES:
                found_lines.append(v)

        if not found_lines:
            continue

        odds = extract_odds_numbers(text)

        if len(odds) < 2:
            continue

        for line in found_lines:
            candidates[line].append({
                "is_bia": is_bet_in_asia(text),
                "home": odds[-2],
                "away": odds[-1],
                "raw_text": text
            })

    result = {}

    for line in HANDICAP_LINES:
        result[f"home_ah_{line:+.1f}"] = ""
        result[f"away_ah_{line:+.1f}"] = ""

    # ML補完
    result["home_ah_-0.5"] = fixture["home_ml"]
    result["home_ah_+0.5"] = fixture["home_ml"]
    result["away_ah_-0.5"] = fixture["away_ml"]
    result["away_ah_+0.5"] = fixture["away_ml"]

    for line in HANDICAP_LINES:
        if line in [-0.5, 0.5]:
            continue

        if not candidates[line]:
            continue

        selected = next(
            (c for c in candidates[line] if c["is_bia"]),
            candidates[line][0]
        )

        print(
            f"AH選択: {fixture['home']} vs {fixture['away']} "
            f"line={line:+.1f} / BIA={selected['is_bia']} "
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

    result["home_ah_-0.5"] = fixture["home_ml"]
    result["home_ah_+0.5"] = fixture["home_ml"]
    result["away_ah_-0.5"] = fixture["away_ml"]
    result["away_ah_+0.5"] = fixture["away_ml"]

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


def run_job():
    results = []

    with sync_playwright() as p:
        browser = launch_browser(p)
        page = new_light_page(browser)

        try:
            fixtures = get_fixture_rows(page)

            for f in fixtures:
                print(f"{f['start_time_jst']} {f['home']} vs {f['away']}")

                try:
                    ml = extract_moneyline_odds(page, f)
                    f = {**f, **ml}
                except Exception as e:
                    print(f"ML取得失敗: {f['home']} vs {f['away']} / {e}")
                    continue

                try:
                    ah = extract_ah_odds(page, f)
                    results.append({**f, **ah})

                except Exception as e:
                    print(f"AH取得失敗: {f['home']} vs {f['away']} / {e}")
                    ah = empty_ah_result(f)
                    results.append({**f, **ah})

        finally:
            browser.close()

    write_to_sheet(results)

    return results