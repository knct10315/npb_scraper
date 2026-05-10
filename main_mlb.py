from playwright.sync_api import sync_playwright, TimeoutError
from datetime import datetime, timedelta
import re
import os
import json
import gspread
from google.oauth2.service_account import Credentials

# =====================
# 設定
# =====================

FIXTURES_URL = "https://www.betexplorer.com/baseball/usa/mlb/fixtures/"
BASE_URL = "https://www.betexplorer.com"

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1vjgGVoDYwmdEOjz8qcMFG_-7GIgg7ZISTt428ylAmmo/edit"
WORKSHEET_GID = 1434161650

LEAGUE_NAME = "MLB"

BETEXPLORER_TO_JST_HOURS = 7
HEADLESS = True
TARGET_HOURS = 48

HANDICAP_LINES = [-3.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 3.5]

CLEAR_RANGES = [
    "A10:E70",
    "J10:AA70",
]

BLOCK_RESOURCE_TYPES = {"image", "font", "media"}


# =====================
# Utility
# =====================

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


# =====================
# 日時
# =====================

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

    raw_dt = datetime(
        base_date.year,
        base_date.month,
        base_date.day,
        hour,
        minute
    )

    return raw_dt + timedelta(hours=BETEXPLORER_TO_JST_HOURS)


def is_within_target_hours(dt):
    now = datetime.now()
    limit = now + timedelta(hours=TARGET_HOURS)

    return now <= dt <= limit


# =====================
# Playwright
# =====================

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


# =====================
# Navigation
# =====================

def safe_goto(page, url):
    page.goto(
        url,
        wait_until="domcontentloaded",
        timeout=60000
    )

    page.wait_for_timeout(2000)

    try:
        page.wait_for_selector("table", timeout=30000)

    except TimeoutError:
        raise Exception(f"table not found: {url}")


# =====================
# Fixtures
# =====================

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

        home, away = [
            x.strip()
            for x in match_name.split(" - ", 1)
        ]

        start_time = parse_betexplorer_datetime(
            last_date_label,
            last_time_text
        )

        if start_time is None:
            continue

        if not is_within_target_hours(start_time):
            continue

        # オッズ未掲載試合除外
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

    return fixtures


# =====================
# ML取得
# =====================

def extract_moneyline_odds_from_current_page(page, fixture):
    """
    試合詳細ページの現在表示テーブルからMLを取得。
    MLBでは ROW 0 の BOOKMAKERS 見出し行を除外する必要あり。
    想定セル:
      CELL 0: bookmaker
      CELL 4: home ML
      CELL 5: away ML
    """

    rows = page.locator("table tr").all()

    candidates = []

    for row in rows:
        cells = row.locator("th, td").all()

        if len(cells) < 6:
            continue

        texts = [
            c.inner_text().strip()
            for c in cells
        ]

        bookmaker = texts[0]

        if not bookmaker:
            continue

        # 見出し行除外
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

    selected = next(
        (c for c in candidates if c["is_bia"]),
        candidates[0]
    )

    print(
        f"ML {fixture['home']} vs {fixture['away']} "
        f"/ {selected['bookmaker']} "
        f"/ {selected['home_ml']} - {selected['away_ml']}"
    )

    return {
        "home_ml": selected["home_ml"],
        "away_ml": selected["away_ml"]
    }


# =====================
# AHタブ切替
# =====================

def ah_lines_visible(page):
    rows = page.locator("table tr").all()

    for row in rows[:40]:
        text = row.inner_text()

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


def ensure_ah_tab(page):
    page.wait_for_timeout(1000)

    if ah_lines_visible(page):
        return

    ah_selectors = [
        "text=Asian Handicap",
        "text=AH",
    ]

    for selector in ah_selectors:
        try:
            page.locator(selector).first.click(timeout=5000)
            page.wait_for_timeout(3000)

            if ah_lines_visible(page):
                return

        except Exception:
            pass

    # URLハッシュを直接変えてみる
    try:
        current_url = page.url.split("#")[0]
        page.goto(
            current_url + "#ah",
            wait_until="domcontentloaded",
            timeout=60000
        )
        page.wait_for_timeout(3000)

        if ah_lines_visible(page):
            return

    except Exception:
        pass

    raise Exception("AH tab not visible")


# =====================
# AH取得
# =====================

def extract_ah_odds_from_current_page(page, fixture):
    """
    現在ページ上でAHタブを表示してからAH取得。
    想定セル:
      CELL 0: bookmaker
      CELL 4: handicap line
      CELL 5: home odds
      CELL 6: away odds
    """

    ensure_ah_tab(page)

    rows = page.locator("table tr").all()

    candidates = {
        line: []
        for line in HANDICAP_LINES
    }

    for row in rows:
        cells = row.locator("th, td").all()

        if len(cells) < 7:
            continue

        texts = [
            c.inner_text().strip()
            for c in cells
        ]

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
            f"AH {fixture['home']} vs {fixture['away']} "
            f"/ line={line:+.1f} "
            f"/ {selected['bookmaker']} "
            f"/ {selected['home']} - {selected['away']}"
        )

        result[f"home_ah_{line:+.1f}"] = selected["home"]

        # awayは反対符号へ入れる
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


# =====================
# Google Sheets
# =====================

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

    return gspread.service_account(
        filename="credentials.json"
    )


def get_worksheet_by_gid(spreadsheet, gid):
    for ws in spreadsheet.worksheets():
        if ws.id == gid:
            return ws

    raise Exception("Worksheet not found")


def write_to_sheet(data):
    gc = get_gspread_client()

    sh = gc.open_by_url(SPREADSHEET_URL)

    ws = get_worksheet_by_gid(
        sh,
        WORKSHEET_GID
    )

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
        ws.update(range_name="A10", values=left_values)

    if right_values:
        ws.update(range_name="J10", values=right_values)


# =====================
# Main
# =====================

def run_job():
    results = []

    with sync_playwright() as p:
        browser = launch_browser(p)
        page = new_light_page(browser)

        try:
            fixtures = get_fixture_rows(page)

            for f in fixtures:
                print(
                    f"{f['start_time_jst']} "
                    f"{f['home']} vs {f['away']}"
                )

                try:
                    # 試合ページは1回だけ開く
                    safe_goto(page, f["match_url"])

                    ml = extract_moneyline_odds_from_current_page(
                        page,
                        f
                    )

                    f = {
                        **f,
                        **ml
                    }

                except Exception as e:
                    print(
                        f"ML取得失敗: "
                        f"{f['home']} vs {f['away']} "
                        f"/ {e}"
                    )
                    continue

                try:
                    # 同じページ上でAHタブに切り替えて取得
                    ah = extract_ah_odds_from_current_page(
                        page,
                        f
                    )

                    results.append({
                        **f,
                        **ah
                    })

                except Exception as e:
                    print(
                        f"AH取得失敗: "
                        f"{f['home']} vs {f['away']} "
                        f"/ {e}"
                    )

                    ah = empty_ah_result(f)

                    results.append({
                        **f,
                        **ah
                    })

        finally:
            browser.close()

    write_to_sheet(results)

    print(f"完了: {len(results)} 件")
    return results


if __name__ == "__main__":
    run_job()