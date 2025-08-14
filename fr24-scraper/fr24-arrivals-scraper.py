import re
import time
import os
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def get_driver(headless=True):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    return driver

def parse_row(tr, date_text=None):
    """Parse an arrivals flight <tr> BeautifulSoup tag."""
    tds = tr.find_all('td')
    if len(tds) < 6:
        return None

    time_txt = tds[0].get_text(strip=True)

    # FLIGHT
    flight = "-"
    flight_cell = tds[1]
    a = flight_cell.find('a')
    if a:
        flight = a.get_text(strip=True)
    else:
        flight = flight_cell.get_text(strip=True) or "-"

    # FROM (arrivals) - 3rd td
    origin = "-"
    place_div = tds[2]
    name_span = place_div.find('span', class_='hide-mobile-only')
    code_a = place_div.find('a')
    if name_span:
        name = name_span.get_text(" ", strip=True)
    else:
        name = place_div.get_text(" ", strip=True)
    code = code_a.get_text(strip=True) if code_a else ""
    origin = f"{name} {code}".strip()

    # AIRLINE
    airline = "-"
    airline_td = tds[3]
    a_air = airline_td.find('a')
    if a_air:
        airline = a_air.get_text(strip=True)
    else:
        airline = airline_td.get_text(strip=True) or "-"

    # AIRCRAFT
    aircraft_td = tds[4]
    model_span = aircraft_td.find('span', class_='notranslate ng-binding')
    reg_a = aircraft_td.find('a')
    parts = []
    if model_span:
        parts.append(model_span.get_text(strip=True))
    if reg_a:
        parts.append(reg_a.get_text(strip=True))
    if not parts:
        txt = aircraft_td.get_text(" ", strip=True)
        if txt:
            parts.append(txt)
    aircraft = " ".join(parts).strip() if parts else "-"

    # STATUS
    try:
        status_td = tds[-1]
        span = status_td.find('span')
        if span and span.get_text(strip=True):
            trailing = status_td.get_text(" ", strip=True)
            status = trailing
        else:
            status = status_td.get_text(" ", strip=True)
    except Exception:
        status = tds[-1].get_text(" ", strip=True)

    status = re.sub(r'\s+', ' ', status).strip()
    origin = re.sub(r'\s+', ' ', origin).strip()

    return {
        "DATE": date_text if date_text is not None else "-",
        "TIME": time_txt,
        "FLIGHT": flight,
        "FROM": origin,
        "AIRLINE": airline,
        "AIRCRAFT": aircraft,
        "STATUS": status
    }

def _normalize_date_str(raw, normalize_date):
    if not normalize_date or not raw:
        return raw
    try:
        this_year = datetime.now().year
        dt = datetime.strptime(raw, "%A, %b %d")
        dt = dt.replace(year=this_year)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return raw

def load_all_pages(driver, mode='arrivals', max_clicks_each=10, timeout=12, sleep_after_click=1.0):
    """
    Click 'Load earlier flights' and 'Load later flights' buttons for the given mode.
    Returns counts dict with how many clicks performed for earlier & later.
    """
    counts = {'earlier': 0, 'later': 0}
    while True:
        any_clicked = False
        buttons = driver.find_elements(By.CSS_SELECTOR, f"button.btn-flights-load[data-mode='{mode}']")
        for b in buttons:
            try:
                if not b.is_displayed():
                    continue
            except Exception:
                continue
            text = (b.get_attribute("innerText") or "").lower()
            if 'earlier' in text and counts['earlier'] < max_clicks_each:
                prev_rows = len(driver.find_elements(By.CSS_SELECTOR, "tr.ng-scope"))
                try:
                    driver.execute_script("arguments[0].scrollIntoView(true);", b)
                    time.sleep(0.25)
                    b.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", b)
                try:
                    WebDriverWait(driver, timeout).until(
                        lambda d: len(d.find_elements(By.CSS_SELECTOR, "tr.ng-scope")) > prev_rows or not b.is_displayed()
                    )
                except Exception:
                    pass
                time.sleep(sleep_after_click)
                counts['earlier'] += 1
                any_clicked = True
                break
            if 'later' in text and counts['later'] < max_clicks_each:
                prev_rows = len(driver.find_elements(By.CSS_SELECTOR, "tr.ng-scope"))
                try:
                    driver.execute_script("arguments[0].scrollIntoView(true);", b)
                    time.sleep(0.25)
                    b.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", b)
                try:
                    WebDriverWait(driver, timeout).until(
                        lambda d: len(d.find_elements(By.CSS_SELECTOR, "tr.ng-scope")) > prev_rows or not b.is_displayed()
                    )
                except Exception:
                    pass
                time.sleep(sleep_after_click)
                counts['later'] += 1
                any_clicked = True
                break
        if not any_clicked or (counts['earlier'] >= max_clicks_each and counts['later'] >= max_clicks_each):
            break
    return counts

def scrape_airport_arrivals_with_date(airport_slug=None, url=None, headless=True, timeout=20, normalize_date=False,
                                     max_clicks_each=10):
    """
    Scrape arrivals, clicking load-more buttons until exhausted/limit.
    Returns DataFrame: DATE, TIME, FLIGHT, FROM, AIRLINE, AIRCRAFT, STATUS
    """
    if url:
        target = url
    elif airport_slug:
        target = f"https://www.flightradar24.com/data/airports/{airport_slug}/arrivals"
    else:
        raise ValueError("Either 'airport_slug' or 'url' must be provided.")

    driver = get_driver(headless=headless)
    try:
        driver.get(target)
        wait = WebDriverWait(driver, timeout)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tr")))
        time.sleep(1.0)

        print("Attempting to click 'Load earlier' / 'Load later' buttons to expand table (arrivals)...")
        counts = load_all_pages(driver, mode='arrivals', max_clicks_each=max_clicks_each)
        print(f"Clicked earlier: {counts['earlier']}, later: {counts['later']}")

        html = driver.page_source
    finally:
        driver.quit()

    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("tr")
    records = []
    current_date_text = None

    for r in rows:
        classes = r.get("class") or []
        # Date separator rows
        if any("row-date-separator" in c for c in classes):
            td = r.find('td')
            if td:
                raw = td.get_text(" ", strip=True)
                current_date_text = _normalize_date_str(raw, normalize_date)
            continue

        # flight rows
        if "ng-scope" in classes or r.select_one("td.cell-flight-number") or r.get("data-date"):
            data_date_attr = r.get("data-date")
            if data_date_attr:
                row_date_to_use = _normalize_date_str(data_date_attr, normalize_date)
            else:
                row_date_to_use = current_date_text

            rec = parse_row(r, date_text=row_date_to_use)
            if rec:
                records.append(rec)

    df = pd.DataFrame(records, columns=["DATE", "TIME", "FLIGHT", "FROM", "AIRLINE", "AIRCRAFT", "STATUS"])
    return df

def run_and_save_arrivals():
    print("Flightradar24 arrivals scraper — will click load buttons and save results to CSV.")
    u = input("Full FR24 arrivals URL (leave blank to use airport slug): ").strip()
    slug = None
    if not u:
        slug = input("Airport slug (e.g. ceb, mnl): ").strip()
        if not slug:
            print("No URL or slug provided. Exiting.")
            return

    filename = input("Output CSV filename (e.g. ceb_arrivals.csv): ").strip()
    if not filename:
        print("No filename provided. Exiting.")
        return

    # ensure .csv extension
    if not filename.lower().endswith(".csv"):
        filename = filename + ".csv"

    headless_input = input("Run headless? (Y/n) [default Y]: ").strip().lower()
    headless = not (headless_input == "n")
    normalize_input = input("Normalize dates to YYYY-MM-DD? (y/N) [default N]: ").strip().lower()
    normalize_date = (normalize_input == "y")

    max_clicks_input = input("Max clicks per direction (earlier/later)? [default 10]: ").strip()
    try:
        max_clicks_each = int(max_clicks_input) if max_clicks_input else 10
    except Exception:
        max_clicks_each = 10

    print("\nScraping arrivals (this may take a while if loading many pages)...")
    try:
        df = scrape_airport_arrivals_with_date(airport_slug=slug if not u else None,
                                              url=u if u else None,
                                              headless=headless,
                                              normalize_date=normalize_date,
                                              max_clicks_each=max_clicks_each)
        out_path = os.path.abspath(filename)
        df.to_csv(out_path, index=False)
        print(f"Saved {len(df)} rows to {out_path}")
        if df.empty:
            print("Warning: no flight rows were found. Try headless=False or increase max clicks for more pages.")
    except Exception as e:
        print("An error occurred during scraping:")
        print(repr(e))

if __name__ == "__main__":
    run_and_save_arrivals()