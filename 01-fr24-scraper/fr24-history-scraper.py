#!/usr/bin/env python3
"""
scrape_fr24_history_fixed.py

Usage:
    python scrape_fr24_history_fixed.py PR2485 output.csv
    python scrape_fr24_history_fixed.py PR2485,PR100,PR200 output.csv

Requirements:
    pip install selenium beautifulsoup4 webdriver-manager python-dateutil
"""
import sys
import time
import csv
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from dateutil import parser as dateparser

# --------- Helpers for parsing ----------
def clean_text(el) -> str:
    if el is None:
        return ""
    t = el.get_text(" ", strip=True)
    return t.replace("\u2014", "—").strip()

def parse_mobile_row_fields(mobile_td):
    result = {"DATE": "", "STD": "", "ATD": "", "STA": "", "FROM": "", "TO": "", "STATUS": ""}
    if mobile_td is None:
        return result

    date_div = mobile_td.find(attrs={"data-time-format": True})
    if date_div:
        result["DATE"] = clean_text(date_div)

    status_div = mobile_td.find(attrs={"data-prefix": True})
    if status_div:
        result["STATUS"] = clean_text(status_div)

    for p in mobile_td.find_all("p"):
        lbl = p.find("label")
        val = p.find("span", class_="details")
        if lbl and val:
            lblt = lbl.get_text(strip=True).upper()
            valt = clean_text(val)
            if lblt == "STD":
                result["STD"] = valt
            elif lblt == "ATD":
                result["ATD"] = valt
            elif lblt == "STA":
                result["STA"] = valt
            elif lblt == "FROM":
                result["FROM"] = valt
            elif lblt == "TO":
                result["TO"] = valt
    return result

def parse_std_sta_to_datetime(date_text: str, time_text: str) -> Optional[datetime]:
    if not date_text or not time_text:
        return None
    if time_text.strip() in ("", "—", "-", "—"):
        return None
    try:
        dt = dateparser.parse(f"{date_text} {time_text}")
        return dt
    except Exception:
        try:
            return datetime.strptime(f"{date_text} {time_text}", "%d %b %Y %I:%M %p")
        except Exception:
            return None

def compute_flight_time(std_dt: Optional[datetime], sta_dt: Optional[datetime]) -> str:
    if not std_dt or not sta_dt:
        return ""
    if sta_dt < std_dt:
        sta_dt = sta_dt + timedelta(days=1)
    td = sta_dt - std_dt
    minutes = int(td.total_seconds() // 60)
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours:d}:{mins:02d}"

# --------- Main scraping function ----------
def scrape_history_for_flight(driver, flight_number: str, timeout: int = 12, pause_between_clicks: float = 1.0) -> List[Dict]:
    url = f"https://www.flightradar24.com/data/flights/{flight_number}"
    driver.get(url)

    # wait briefly for JS content to render
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#cnt-subpage-info h1"))
        )
    except Exception:
        # still proceed to clicking/loading attempts
        pass

    # Attempt to click "Load earlier flights" until no more added
    while True:
        try:
            # find button (if present)
            button = driver.find_element(By.CSS_SELECTOR, "button.loadButton.loadEarlierFlights")
        except Exception:
            button = None

        if not button:
            break
        try:
            cls = (button.get_attribute("class") or "").lower()
            if "disabled" in cls:
                break
            # scroll + click
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(0.2)
            try:
                button.click()
            except Exception:
                driver.execute_script("arguments[0].click();", button)
            # wait for rows to grow
            start = time.time()
            while time.time() - start < timeout:
                rows = driver.find_elements(By.CSS_SELECTOR, "tr.data-row")
                if len(rows) > 0:
                    break
                time.sleep(0.3)
            time.sleep(pause_between_clicks)
        except Exception:
            break

    # parse
    soup = BeautifulSoup(driver.page_source, "html.parser")

    header = soup.select_one("#cnt-subpage-info h1")
    airline = ""
    flightnum = flight_number
    if header:
        htxt = clean_text(header)
        m = re.search(r"Flight history for\s+(.+?)\s+flight\s+([A-Z0-9\-]+)", htxt, flags=re.I)
        if m:
            airline = m.group(1).strip()
            flightnum = m.group(2).strip()

    results = []
    for tr in soup.select("tr.data-row"):
        mobile_td = tr.select_one("td.visible-xs.visible-sm")
        mobile_vals = parse_mobile_row_fields(mobile_td)

        desktop_date_td = tr.find(attrs={"data-time-format": True})
        desktop_date = clean_text(desktop_date_td) if desktop_date_td else mobile_vals.get("DATE", "")

        from_val = mobile_vals.get("FROM") or ""
        to_val = mobile_vals.get("TO") or ""

        aircraft = ""
        hidden_tds = [td for td in tr.find_all("td") if td.get("class") and "hidden-xs" in td.get("class")]
        for td in hidden_tds:
            txt = clean_text(td)
            if txt and txt not in (desktop_date, from_val, to_val) and not re.search(r"play|btn|Scheduled|Unknown|Not available", txt, flags=re.I):
                if re.search(r"[A-Za-z0-9]{2,}", txt):
                    aircraft = txt
                    break

        std = mobile_vals.get("STD") or ""
        atd = mobile_vals.get("ATD") or ""
        sta = mobile_vals.get("STA") or ""

        status_txt = ""
        status_el = tr.find(attrs={"data-prefix": True})
        if status_el:
            status_txt = clean_text(status_el)
        else:
            status_txt = mobile_vals.get("STATUS", "")

        date_text = desktop_date or mobile_vals.get("DATE", "")

        std_dt = parse_std_sta_to_datetime(date_text, std)
        sta_dt = parse_std_sta_to_datetime(date_text, sta)
        flight_time = compute_flight_time(std_dt, sta_dt)

        row = {
            "DATE": date_text,
            "FROM": from_val,
            "TO": to_val,
            "AIRCRAFT": aircraft,
            "FLIGHT TIME": flight_time,
            "STD": std,
            "ATD": atd,
            "STA": sta,
            "STATUS": status_txt,
            "FLIGHT NUMBER": flightnum,
            "AIRLINE": airline
        }
        results.append(row)

    return results

# --------- CLI / runner ----------
def main(argv):
    if len(argv) < 3:
        print("Usage: python scrape_fr24_history_fixed.py FLIGHT1[,FLIGHT2,...] output.csv")
        sys.exit(1)

    flight_arg = argv[1]
    out_csv = argv[2]

    # parse flight list
    if "," in flight_arg:
        flights = [f.strip() for f in flight_arg.split(",") if f.strip()]
    else:
        try:
            with open(flight_arg, "r", encoding="utf-8") as fh:
                data = fh.read().strip()
                if "\n" in data:
                    flights = [line.strip() for line in data.splitlines() if line.strip()]
                else:
                    flights = [flight_arg.strip()]
        except FileNotFoundError:
            flights = [flight_arg.strip()]

    # chrome options with desktop user-agent to reduce bot detection
    chrome_opts = Options()
    #chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--window-size=1920,1080")
    desktop_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    chrome_opts.add_argument(f"--user-agent={desktop_ua}")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_opts)

    all_rows = []
    try:
        for flight in flights:
            try:
                print(f"[+] Scraping flight {flight} ...")
                rows = scrape_history_for_flight(driver, flight, timeout=12, pause_between_clicks=1.0)
                print(f"    -> parsed {len(rows)} history rows for {flight}")
                all_rows.extend(rows)
                time.sleep(1.0)  # polite pause
            except Exception as e:
                print(f"[!] Error scraping {flight}: {e}")
    finally:
        driver.quit()

    # write CSV
    if all_rows:
        fieldnames = ["DATE", "FROM", "TO", "AIRCRAFT", "FLIGHT TIME", "STD", "ATD", "STA", "STATUS", "FLIGHT NUMBER", "AIRLINE"]
        with open(out_csv, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for r in all_rows:
                writer.writerow({k: r.get(k, "") for k in fieldnames})
        print(f"[+] Wrote {len(all_rows)} rows to {out_csv}")
    else:
        print("[!] No rows extracted.")

if __name__ == "__main__":
    main(sys.argv)