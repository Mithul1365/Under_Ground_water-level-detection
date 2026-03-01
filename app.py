from flask import Flask, render_template, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import requests
import pandas as pd
import os
import logging
import ast  # for parsing dict-like strings
import time

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

app = Flask(__name__)

# ------------------------------------------------------------------
#  Fixed location settings
# ------------------------------------------------------------------
STATE     = "Karnataka"
DISTRICT  = "Bangalore Urban"
AGENCY    = "CGWB"
STARTDATE = "2025-09-16"
PAGE      = "0"
SIZE      = "1000"
CSV_FILE  = "downloads/groundwater_auto.csv"
THRESHOLD_FILE = "downloads/threshold_values.csv"
# ------------------------------------------------------------------

# ------------------------------------------------------------------
# WhatsApp Configuration
# ------------------------------------------------------------------
RECIPIENTS = ["+91xxxxxxxxxx,+91xxxxxxxxxx,+91xxxxxxxxxx"]   # Add more numbers here
CHROMEDRIVER_PATH = r"......past your chrome.exe path....."
USER_DATA_DIR = os.path.join(os.getcwd(), "User_Data")
os.makedirs(USER_DATA_DIR, exist_ok=True)

LOG_FILE = os.path.join(os.getcwd(), "water_alert.log")
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

_driver = None  # global Chrome instance

# ------------------------------------------------------------------
# Custom datetime parser
# ------------------------------------------------------------------
def parse_custom_datetime(val):
    try:
        if isinstance(val, str) and val.strip().startswith("{"):
            d = ast.literal_eval(val)  # safely convert string → dict
            return datetime(
                int(d.get("year", 1970)),
                int(d.get("monthValue", 1)),
                int(d.get("dayOfMonth", 1)),
                int(d.get("hour", 0)),
                int(d.get("minute", 0)),
                int(d.get("second", 0)),
            )
    except Exception:
        return pd.NaT
    return pd.to_datetime(val, errors="coerce")

# ------------------------------------------------------------------
# WhatsApp Automation Helpers
# ------------------------------------------------------------------
def start_driver():
    global _driver
    if _driver is not None:
        return _driver
    options = webdriver.ChromeOptions()
    options.add_argument(f"--user-data-dir={USER_DATA_DIR}")
    options.add_argument("--start-maximized")
    service = Service(CHROMEDRIVER_PATH)
    _driver = webdriver.Chrome(service=service, options=options)
    _driver.get("https://web.whatsapp.com")
    print("📲 Scan QR code if not logged in yet...")

    # Wait for either QR code OR chat list to appear
    try:
        WebDriverWait(_driver, 60).until(
            EC.any_of(
                EC.presence_of_element_located((By.XPATH, "//canvas[@aria-label='Scan me!']")),
                EC.presence_of_element_located((By.XPATH, "//div[@aria-label='Chat list']"))
            )
        )
        print("✅ WhatsApp Web loaded.")
    except:
        print("⚠️ WhatsApp Web did not load in time.")
    return _driver

def send_alert(driver, row, threshold, RECIPIENTS):
    """
    Send alert to multiple recipients using an already open WhatsApp Web driver.
    driver      : Selenium driver (must already be logged into WhatsApp Web)
    row         : pandas row containing station info
    threshold   : threshold value
    recipients  : list of phone numbers
    """
    wait = WebDriverWait(driver, 30)

    msg = (
    f"ALERT: Water level below threshold!\n\n"
    f"Station: {row.get('stationName','')}, {row.get('district','')}\n"
    f"Level Y-Axis: {row['dataValue']} mbgl\n"
    f"Time: {row['dataTime']}\n\n"
    f"Threshold Y-axis: {round(threshold,2)} mbgl"
    )


    for number in RECIPIENTS:
        try:
            # Search for the contact/number
            search_box = wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, '//div[@contenteditable="true"][@data-tab="3"]')
                )
            )
            search_box.clear()
            search_box.send_keys(number)
            search_box.send_keys(Keys.ENTER)
            time.sleep(2)

            # Type the message
            message_box = wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, '//div[@contenteditable="true"][@data-tab="10"]')
                )
            )
            message_box.send_keys(msg)
            message_box.send_keys(Keys.ENTER)

            print(f"✅ Alert sent to {number}")
            time.sleep(2)

        except Exception as e:
            print(f"❌ Error sending alert to {number}: {e}")

# ------------------------------------------------------------------
# Check Alerts
# ------------------------------------------------------------------
def check_and_send_alerts():
    print("⚡ Running check_and_send_alerts()...")

    if not os.path.exists(CSV_FILE) or not os.path.exists(THRESHOLD_FILE):
        print("⚠️ CSV or threshold file missing, skipping alerts.")
        return

    df = pd.read_csv(CSV_FILE, dtype=str)
    print(f"📊 Loaded groundwater CSV: {len(df)} rows")

    df["stationName"] = df["stationName"].astype(str).str.strip()
    df["dataTime"] = df["dataTime"].apply(parse_custom_datetime)
    df["dataValue"] = pd.to_numeric(df["dataValue"], errors="coerce")
    df = df.dropna(subset=["stationCode", "dataTime", "dataValue"])
    print(f"📊 After cleaning: {len(df)} rows")

    df = df.sort_values("dataTime").groupby("stationCode").tail(1)
    print(f"📊 Latest per station: {len(df)} rows")

    thresholds = pd.read_csv(THRESHOLD_FILE, dtype=str)
    thresholds["stationName"] = thresholds["stationName"].astype(str).str.strip()
    thresholds["threshold"] = pd.to_numeric(thresholds["threshold"], errors="coerce")
    th_dict = dict(zip(thresholds["stationName"], thresholds["threshold"]))
    print(f"📊 Threshold dict loaded: {len(th_dict)} stations")

    # 👉 Start driver once
    driver = start_driver()

    try:
        for _, row in df.iterrows():
            st = row.get("stationName", "")
            if st in th_dict and pd.notna(row["dataValue"]):
                print(f"🔎 Checking {st}: value={row['dataValue']} vs threshold={th_dict[st]}")
                if row["dataValue"] < th_dict[st]:
                    print(f"❗ ALERT should trigger for {st}")
                    send_alert(driver, row, th_dict[st], RECIPIENTS)
                else:
                    print(f"✅ {st} OK: {row['dataValue']} >= {th_dict[st]}")
            else:
                print(f"⚠️ Skipping row: {st}, value={row['dataValue']}")
    finally:
        driver.quit()
        print("🛑 WhatsApp Web closed after sending alerts")

# ------------------------------------------------------------------
# Data Fetcher
# ------------------------------------------------------------------
def fetch_groundwater_data():
    try:
        enddate = datetime.now().strftime("%Y-%m-%d")
        url = (
            "https://indiawris.gov.in/Dataset/Ground Water Level"
            f"?stateName={STATE}"
            f"&districtName={DISTRICT}"
            f"&agencyName={AGENCY}"
            f"&startdate={STARTDATE}"
            f"&enddate={enddate}"
            f"&download=true&page={PAGE}&size={SIZE}"
        )
        headers = {"accept": "application/json"}
        resp = requests.post(url, headers=headers, data="")
        resp.raise_for_status()
        data = resp.json()

        if not data:
            print(f"[{datetime.now()}] No new data returned.")
            return

        df = pd.DataFrame(data)
        os.makedirs("downloads", exist_ok=True)

        if os.path.exists(CSV_FILE):
            existing = pd.read_csv(CSV_FILE)
            combined = pd.concat([existing, df], ignore_index=True)
            combined = combined.astype(str).drop_duplicates()
            combined.to_csv(CSV_FILE, index=False)
        else:
            df.to_csv(CSV_FILE, index=False)

        print(f"[{datetime.now()}] Data fetched & saved to {CSV_FILE}, rows added: {len(df)}")

        check_and_send_alerts()

    except Exception as e:
        print(f"[{datetime.now()}] Error fetching groundwater data: {e}")

# ------------------------------------------------------------------
# Flask Routes
# ------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/map_data")
def map_data():
    if not os.path.exists(CSV_FILE):
        return jsonify([])

    df = pd.read_csv(CSV_FILE, dtype=str)
    df["stationName"] = df["stationName"].astype(str).str.strip()
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["dataTime"] = df["dataTime"].apply(parse_custom_datetime)
    df["dataValue"] = pd.to_numeric(df["dataValue"], errors="coerce")

    df = df.sort_values("dataTime").groupby("stationCode").tail(1)

    thresholds = {}
    if os.path.exists(THRESHOLD_FILE):
        th = pd.read_csv(THRESHOLD_FILE, dtype=str)
        th["stationName"] = th["stationName"].astype(str).str.strip()
        th["threshold"] = pd.to_numeric(th["threshold"], errors="coerce")
        thresholds = dict(zip(th["stationName"], th["threshold"]))

    result = []
    for _, row in df.iterrows():
        if pd.notna(row["latitude"]) and pd.notna(row["longitude"]):
            result.append({
                "lat": float(row["latitude"]),
                "lon": float(row["longitude"]),
                "station": row.get("stationName", ""),
                "district": row.get("district", ""),
                "water_level": row.get("dataValue", ""),
                "threshold": thresholds.get(row.get("stationName", ""), None),
                "datetime": row["dataTime"].strftime("%Y-%m-%d %H:%M") if pd.notna(row["dataTime"]) else ""
            })
    return jsonify(result)

# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
if __name__ == "__main__":
    os.makedirs("downloads", exist_ok=True)
    scheduler = BackgroundScheduler()
    scheduler.add_job(fetch_groundwater_data, "interval", hours=6, next_run_time=datetime.now())
    scheduler.start()
    print("🌍 Open http://127.0.0.1:5000 in your browser")
    app.run(host="127.0.0.1", port=5000, debug=True, use_reloader=False)
