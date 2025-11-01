#!/usr/bin/env python3
"""
fr24_history_with_auth.py

Full, ready-to-run scraper that:
 - logs into Flightradar24 using saved cookies, automated login via env vars, or manual interactive login
 - loads the flight-history page for each flight number you provide
 - repeatedly clicks "Load earlier flights" until all history rows are loaded
 - parses flight history rows and writes CSV with columns:
     DATE, FROM, TO, AIRCRAFT, FLIGHT TIME, STD, ATD, STA, STATUS, FLIGHT NUMBER, AIRLINE

Usage:
    python fr24_history_with_auth.py PR2485 output.csv           # single flight
    python fr24_history_with_auth.py PR2485,PR100 out.csv       # comma-separated list
    python fr24_history_with_auth.py flights.txt out.csv        # text file (one flight per line)
    python fr24_history_with_auth.py PR2485 out.csv --show     # show browser for manual login/inspection

Requirements:
    pip install selenium beautifulsoup4 webdriver-manager python-dateutil
Notes:
    - Set FR24_EMAIL and FR24_PASSWORD environment variables for automated login (if allowed).
    - The script saves cookies to `fr24_cookies.json` after manual or automated login for reuse.
    - If FR24 uses CAPTCHA/2FA, use --show and perform manual login; cookies will be saved.
    - Respect FR24's terms of service.
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# ====== HARD-CODED CREDENTIALS (for quick local testing only!) ======
FR24_EMAIL = ""
FR24_PASSWORD = ""
# ====================================================================

# -------- Configuration ----------
COOKIES_FILE = "fr24_cookies.json"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)
# ---------------------------------

# ---------- Cookie & Auth helpers ----------
def save_cookies(driver, path=COOKIES_FILE):
    try:
        cookies = driver.get_cookies()
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(cookies, fh, indent=2)
        print(f"[+] Saved {len(cookies)} cookies to {path}")
    except Exception as e:
        print(f"[!] Failed to save cookies: {e}")

def load_cookies(driver, path=COOKIES_FILE) -> bool:
    if not os.path.exists(path):
        return False
    try:
        with open(path, "r", encoding="utf-8") as fh:
            cookies = json.load(fh)
    except Exception as e:
        print(f"[!] Failed to read cookies file: {e}")
        return False
    try:
        driver.get("https://www.flightradar24.com/")
        driver.delete_all_cookies()
        for ck in cookies:
            ck2 = {}
            for k in ("name", "value", "domain", "path", "expiry", "httpOnly", "secure"):
                if k in ck:
                    ck2[k] = ck[k]
            try:
                driver.add_cookie(ck2)
            except Exception:
                pass
        print(f"[+] Loaded {len(cookies)} cookies from {path}")
        return True
    except Exception as e:
        print(f"[!] Error loading cookies into browser: {e}")
        return False

def is_logged_in_via_cookie_check(driver, test_flight="PR2485", timeout=8) -> bool:
    """Check if we can see flight history data (indicates logged in)"""
    try:
        cur = driver.current_url
        driver.get(f"https://www.flightradar24.com/data/flights/{test_flight}")
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(2.0)  # Increased wait for dynamic content
        rows = driver.find_elements(By.CSS_SELECTOR, "tr.data-row")
        try:
            driver.get(cur)
        except Exception:
            pass
        return len(rows) > 0
    except Exception:
        return False

def attempt_automated_login(driver, email: str, password: str, timeout=15) -> bool:
    """Try programmatic login (may fail with CAPTCHA/2FA)"""
    try:
        login_url = "https://www.flightradar24.com/account/login"
        driver.get(login_url)
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(2.0)
    except Exception:
        pass

    email_selectors = [
        'input[name="email"]', 
        'input[type="email"]', 
        'input#email', 
        'input[name="username"]'
    ]
    password_selectors = [
        'input[name="password"]', 
        'input[type="password"]', 
        'input#password'
    ]

    email_el = None
    pwd_el = None
    
    for sel in email_selectors:
        try:
            email_el = driver.find_element(By.CSS_SELECTOR, sel)
            break
        except Exception:
            continue
            
    for sel in password_selectors:
        try:
            pwd_el = driver.find_element(By.CSS_SELECTOR, sel)
            break
        except Exception:
            continue

    if not email_el or not pwd_el:
        print("[!] Could not find email/password inputs for automated login.")
        return False

    try:
        email_el.clear()
        email_el.send_keys(email)
        pwd_el.clear()
        pwd_el.send_keys(password)
        
        try:
            submit_btn = driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]')
            submit_btn.click()
        except Exception:
            pwd_el.send_keys(Keys.ENTER)
            
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(3.0)  # Wait for redirect
        
        ok = is_logged_in_via_cookie_check(driver, test_flight="PR2485", timeout=8)
        print(f"[+] Automated login result: {ok}")
        return ok
    except Exception as e:
        print(f"[!] Automated login exception: {e}")
        return False

def ensure_logged_in(driver, show_browser: bool = False, test_flight: str = "PR2485") -> bool:
    """Ensure authenticated session via cookies, env vars, or manual login"""
    # 1) Try loading saved cookies
    if os.path.exists(COOKIES_FILE):
        print("[*] Attempting to load saved cookies...")
        if load_cookies(driver):
            if is_logged_in_via_cookie_check(driver, test_flight=test_flight):
                print("[+] Logged in via saved cookies.")
                return True
            else:
                print("[*] Saved cookies didn't validate.")
    
    # 2) Try automated login with env vars
    email = os.environ.get("FR24_EMAIL")
    password = os.environ.get("FR24_PASSWORD")
    if email and password:
        print("[*] Attempting automated login using environment credentials...")
        ok = attempt_automated_login(driver, email, password)
        if ok:
            save_cookies(driver)
            return True
        else:
            print("[*] Automated login failed (CAPTCHA/2FA may be required).")
    
    # 3) Interactive fallback
    if show_browser:
        print("[*] Please log in manually in the browser window.")
        driver.get("https://www.flightradar24.com/account/login")
        input("After completing login (including 2FA), press Enter to continue...")
        if is_logged_in_via_cookie_check(driver, test_flight=test_flight):
            print("[+] Manual login validated. Saving cookies.")
            save_cookies(driver)
            return True
        else:
            print("[!] Manual login did not validate.")
            return False

    print("[!] No login method available. Set FR24_EMAIL/FR24_PASSWORD or use --show for manual login.")
    return False

# ---------- Parsing helpers ----------
def clean_text(el) -> str:
    """Extract and clean text from BS4 element"""
    if el is None:
        return ""
    t = el.get_text(" ", strip=True)
    return t.replace("\u2014", "—").replace("\xa0", " ").strip()

def parse_std_sta_to_datetime(date_text: str, time_text: str) -> Optional[datetime]:
    """Parse date+time strings into datetime object"""
    if not date_text or not time_text:
        return None
    if time_text.strip() in ("", "—", "-", "N/A"):
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
    """Calculate flight duration from STD and STA"""
    if not std_dt or not sta_dt:
        return ""
    if sta_dt < std_dt:
        sta_dt = sta_dt + timedelta(days=1)
    td = sta_dt - std_dt
    minutes = int(td.total_seconds() // 60)
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours:d}:{mins:02d}"

# ---------- Enhanced scraping ----------
def scrape_history_for_flight(
    driver, 
    flight_number: str, 
    timeout: int = 15, 
    debug: bool = False
) -> List[Dict]:
    """
    Scrape flight history with improved waits and error handling
    """
    url = f"https://www.flightradar24.com/data/flights/{flight_number}"
    print(f"[*] Loading {url}")
    driver.get(url)

    # Wait for page to be ready
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(2.0)  # Extra wait for JS rendering
    except Exception as e:
        print(f"[!] Page load timeout: {e}")

    # Try to wait for the flight history table
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr.data-row"))
        )
        print("[+] Flight history table loaded")
    except Exception:
        print("[!] No flight history table found - may need login or flight doesn't exist")

    # Click "Load earlier flights" until exhausted
    click_count = 0
    max_clicks = 50  # Safety limit
    
    while click_count < max_clicks:
        try:
            # Get current row count
            prev_rows = len(driver.find_elements(By.CSS_SELECTOR, "tr.data-row"))
            
            # Find the button
            button = driver.find_element(By.CSS_SELECTOR, "button.loadButton.loadEarlierFlights")
            
            # Check if disabled
            cls = (button.get_attribute("class") or "").lower()
            if "disabled" in cls:
                print(f"[*] Load button disabled after {click_count} clicks")
                break
            
            # Scroll and click
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(0.5)
            
            try:
                button.click()
            except Exception:
                driver.execute_script("arguments[0].click();", button)
            
            # Wait for new rows to appear
            try:
                WebDriverWait(driver, 10).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, "tr.data-row")) > prev_rows
                )
                print(f"[+] Loaded more rows (click {click_count + 1})")
            except Exception:
                print(f"[*] No new rows after click {click_count + 1}")
                time.sleep(1.5)
            
            click_count += 1
            time.sleep(1.0)  # Be polite to the server
            
        except Exception as e:
            if debug:
                print(f"[*] Load button not found or error: {e}")
            break

    # Save debug HTML if requested
    if debug:
        debug_file = f"debug_{flight_number}.html"
        with open(debug_file, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print(f"[DEBUG] Saved page HTML to {debug_file}")

    # Parse the fully loaded page
    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Extract airline and flight number from header
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
    rows = soup.select("tr.data-row")
    print(f"[+] Found {len(rows)} data rows to parse")

    for idx, tr in enumerate(rows):
        try:
            row_data = parse_single_row(tr, flightnum, airline, debug=debug)
            if row_data:
                results.append(row_data)
        except Exception as e:
            if debug:
                print(f"[!] Error parsing row {idx}: {e}")
            continue

    return results

def parse_single_row(tr, flightnum: str, airline: str, debug: bool = False) -> Optional[Dict]:
    """Parse a single table row with multiple fallback strategies"""
    
    # Strategy 1: Try mobile layout (stacked format)
    mobile_td = tr.select_one("td.visible-xs.visible-sm")
    mobile_data = {}
    
    if mobile_td:
        # Extract date
        date_div = mobile_td.find(attrs={"data-time-format": True})
        if date_div:
            mobile_data["DATE"] = clean_text(date_div)
        
        # Extract status
        status_div = mobile_td.find(attrs={"data-prefix": True})
        if status_div:
            mobile_data["STATUS"] = clean_text(status_div)
        
        # Extract labeled fields
        for p in mobile_td.find_all("p"):
            lbl = p.find("label")
            val = p.find("span", class_="details")
            if lbl and val:
                lblt = lbl.get_text(strip=True).upper()
                valt = clean_text(val)
                mobile_data[lblt] = valt

    # Strategy 2: Desktop layout - look for all tds
    all_tds = tr.find_all("td")
    
    # Find date (usually has data-time-format attribute)
    date_text = ""
    for td in all_tds:
        if td.has_attr("data-time-format"):
            date_text = clean_text(td)
            break
    if not date_text:
        date_text = mobile_data.get("DATE", "")
    
    # Skip header rows like "Jul 2025" or empty rows
    if not date_text or re.match(r'^[A-Za-z]{3,9}\s+\d{4}$', date_text):
        return None
    if "no flight data" in date_text.lower():
        return None

    # Extract FROM/TO
    from_val = mobile_data.get("FROM", "")
    to_val = mobile_data.get("TO", "")
    
    if not from_val or not to_val:
        # Look for tds with title attribute (usually airport names)
        title_tds = [td for td in all_tds if td.has_attr("title")]
        if len(title_tds) >= 2:
            from_val = from_val or clean_text(title_tds[0])
            to_val = to_val or clean_text(title_tds[1])
    
    # Skip if we don't have essential route info
    if not from_val or not to_val:
        return None

    # Extract aircraft type - try multiple strategies
    aircraft = ""
    
    # Strategy 2a: Look in hidden-xs columns (desktop view)
    hidden_tds = [td for td in all_tds if td.get("class") and "hidden-xs" in td.get("class")]
    for td in hidden_tds:
        txt = clean_text(td)
        # Filter out date, airports, and common non-aircraft text
        if txt and txt not in (date_text, from_val, to_val):
            if not re.search(r"play|btn|Scheduled|Unknown|Not available|^—$|^\s*$", txt, flags=re.I):
                if re.search(r"[A-Za-z0-9]{2,}", txt):
                    aircraft = txt
                    break
    
    # Strategy 2b: Look for specific aircraft-related classes or attributes
    if not aircraft:
        for td in all_tds:
            txt = clean_text(td)
            # Aircraft registrations usually have format like "A320 (RP-C4100)" or "32N"
            if re.search(r'([A-Z0-9]{2,4})\s*\([A-Z]{2}-[A-Z0-9]+\)', txt):
                aircraft = txt
                break
            elif re.search(r'^[A-Z0-9]{2,4}$', txt) and txt not in (from_val, to_val, date_text):
                aircraft = txt
                break

    # Extract times
    std = mobile_data.get("STD", "")
    atd = mobile_data.get("ATD", "")
    sta = mobile_data.get("STA", "")
    
    # Extract status
    status_txt = ""
    status_el = tr.find(attrs={"data-prefix": True})
    if status_el:
        status_txt = clean_text(status_el)
    else:
        status_txt = mobile_data.get("STATUS", "")

    # Calculate flight time
    std_dt = parse_std_sta_to_datetime(date_text, std)
    sta_dt = parse_std_sta_to_datetime(date_text, sta)
    flight_time = compute_flight_time(std_dt, sta_dt)

    return {
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

# ---------- CLI / main ----------
def main():
    parser = argparse.ArgumentParser(
        description="Scrape Flightradar24 flight history (improved version with debugging)"
    )
    parser.add_argument(
        "flights", 
        help="Flight code, comma-separated list, or path to file with one flight per line"
    )
    parser.add_argument("out", help="Output CSV path")
    parser.add_argument("--show", action="store_true", help="Show browser window")
    parser.add_argument("--pause-after", action="store_true", help="Pause after scraping for inspection")
    parser.add_argument("--debug", action="store_true", help="Enable debug output and save HTML")
    args = parser.parse_args()

    # Parse flight list
    flights_arg = args.flights
    if "," in flights_arg:
        flights = [f.strip() for f in flights_arg.split(",") if f.strip()]
    else:
        if os.path.exists(flights_arg):
            with open(flights_arg, "r", encoding="utf-8") as fh:
                flights = [line.strip() for line in fh.readlines() if line.strip()]
        else:
            flights = [flights_arg.strip()]

    print(f"[+] Will scrape {len(flights)} flight(s): {', '.join(flights)}")

    # Setup Chrome
    chrome_opts = Options()
    if not args.show:
        chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--start-maximized")
    chrome_opts.add_argument("--window-size=1920,1080")
    chrome_opts.add_argument(f"--user-agent={DEFAULT_USER_AGENT}")
    
    if args.show:
        chrome_opts.add_experimental_option("detach", True)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_opts)

    all_rows: List[Dict] = []
    
    try:
        # Ensure logged in
        test_flight = flights[0] if flights else "5J405"
        if not ensure_logged_in(driver, show_browser=args.show, test_flight=test_flight):
            print("[!] Login failed - cannot proceed.")
            print("[!] Try setting FR24_EMAIL and FR24_PASSWORD environment variables")
            print("[!] Or run with --show flag for manual login")
            return

        # Scrape each flight
        for flight in flights:
            try:
                print(f"\n{'='*60}")
                print(f"[+] Scraping flight {flight}")
                print(f"{'='*60}")
                
                rows = scrape_history_for_flight(driver, flight, timeout=15, debug=args.debug)
                print(f"[+] Successfully parsed {len(rows)} rows for {flight}")
                all_rows.extend(rows)
                
                # Be polite between flights
                time.sleep(2.0)
                
            except Exception as e:
                print(f"[!] Error scraping {flight}: {e}")
                if args.debug:
                    import traceback
                    traceback.print_exc()

        # Optional pause
        if args.pause_after and args.show:
            input("\nPaused for inspection. Press Enter to finish...")

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    # Write output CSV
    if all_rows:
        fieldnames = [
            "DATE", "FROM", "TO", "AIRCRAFT", "FLIGHT TIME", 
            "STD", "ATD", "STA", "STATUS", "FLIGHT NUMBER", "AIRLINE"
        ]
        
        with open(args.out, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for r in all_rows:
                writer.writerow({k: r.get(k, "") for k in fieldnames})
        
        print(f"\n{'='*60}")
        print(f"[+] Successfully wrote {len(all_rows)} rows to {args.out}")
        print(f"{'='*60}")
    else:
        print("\n[!] No rows extracted. Check:")
        print("    - Are you logged in? (saved cookies or FR24_EMAIL/FR24_PASSWORD)")
        print("    - Is the flight number correct?")
        print("    - Try running with --show --debug flags")

if __name__ == "__main__":
    main()