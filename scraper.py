import os
import json
import re
import unicodedata
import csv
import time
import random

from requests import Session, Response
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, HTTPError
from urllib3.util.retry import Retry
from pathlib import PurePath
from urllib.parse import quote
from typing import Callable
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

# --- CONFIGURATION ---
DEBUG_MODE = False
DEBUG_DIR = PurePath("DEBUG")

# MAPBOX API (loaded from environment / GitHub Secrets)
MAPBOX_API_SESSION_TOKEN = os.environ.get("MAPBOX_SESSION_TOKEN", "")
MAPBOX_API_ACCESS_TOKEN = os.environ.get("MAPBOX_ACCESS_TOKEN", "")
MAPBOX_API_LANGUAGE_CODE = "en"
MAPBOX_API_COUNTRY_CODE = "ca"

# Cookies - refresh token loaded from environment variable (GitHub Secret)
HTTP_COOKIES = {
    "cookieyes-consent": "consentid:b25CWTRxRWhweWk5U1VVTlZxd1VYZXVFT051UG5semQ,consent:yes,action:yes,necessary:yes,analytics:yes",
    "refreshToken": os.environ.get("ZIPPLEX_REFRESH_TOKEN", "687f9474474f32be0fa13400"),
    ".AspNetCore.Culture": "c%3Den%7Cuic%3Den",
    "jwt": ""
}

HTTP_HEADERS = {
    'accept': '*/*',
    'accept-language': 'en,tr-TR;q=0.9,tr;q=0.8,en-US;q=0.7',
    'origin': 'https://zipplex.ca',
    'referer': 'https://zipplex.ca/',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
}

# --- UTILS ---

def sanitize_filename(filename: str, replacement: str = "_", max_length: int = 255) -> str:
    if not filename:
        raise Exception("Filename cannot be None!")
    filename = unicodedata.normalize("NFKC", filename)
    invalid_chars = r'[<>:"/\\|?*\x00-\x1F]'
    filename = re.sub(invalid_chars, replacement, filename)
    filename = filename.strip(" .")
    filename = re.sub(f"{re.escape(replacement)}+", replacement, filename)
    if len(filename) > max_length:
        name, dot, ext = filename.rpartition(".")
        if dot:
            allowed = max_length - len(ext) - 1
            filename = name[:allowed] + "." + ext
        else:
            filename = filename[:max_length]
    return filename

def save_debug_file(path: str, content: str | dict | list) -> None:
    if not DEBUG_MODE:
        return
    debug_file_path = DEBUG_DIR / path
    os.makedirs(os.path.dirname(debug_file_path), exist_ok=True)
    with open(debug_file_path, "w", encoding="utf-8") as f:
        if isinstance(content, (dict, list)):
            json.dump(content, f, indent=4)
        else:
            f.write(str(content))

# --- SESSION HANDLING ---

class ZipplexSession(Session):
    def __init__(self, refresh_callback: Callable, expired_status_codes=(401,), *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.refresh_callback = refresh_callback
        self.expired_status_codes = set(expired_status_codes)

        # Setup Retry Strategy for Connection Resets and Server Hiccups
        retry_strategy = Retry(
            total=5,
            backoff_factor=1, # 1s, 2s, 4s, 8s, 16s
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "OPTIONS"],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.mount("https://", adapter)
        self.mount("http://", adapter)

    def request(self, method, url, **kwargs) -> Response:
        retried = kwargs.pop("_retried", False)

        # Human-like delay to prevent "Connection Reset by Peer" bot detection
        time.sleep(random.uniform(0.5, 1.5))

        try:
            res = super().request(method, url, **kwargs)
        except (ConnectionError, ConnectionResetError) as e:
            if not retried:
                print(f"Connection reset. Waiting 3s and retrying...")
                time.sleep(3)
                kwargs["_retried"] = True
                return self.request(method, url, **kwargs)
            raise e

        if res.status_code in self.expired_status_codes:
            if retried:
                raise Exception(f"Auth failed with {res.status_code} after retry.")

            print(f"Status {res.status_code}: Refreshing token...")
            new_headers = self.refresh_callback()
            kwargs.setdefault("headers", {}).update(new_headers)
            kwargs["_retried"] = True
            return self.request(method, url, **kwargs)

        return res

# --- CORE LOGIC ---

class Zipplex:
    def __init__(self, cookies: dict):
        self.session = ZipplexSession(refresh_callback=self.on_jwt_token_expired)
        self.session.headers.update(HTTP_HEADERS)
        self.session.cookies.update(cookies)
        self.refresh_token()
        self.validate_token()

    @property
    def jwt_token(self) -> str:
        return self.session.cookies.get("jwt", "")

    @property
    def auth_headers(self) -> dict:
        return {
            "authorization": f"Bearer {self.jwt_token}",
            'accept': 'application/json, text/plain, */*'
        }

    def on_jwt_token_expired(self):
        self.refresh_token()
        self.validate_token()
        return self.auth_headers

    def refresh_token(self):
        print("Refreshing session token...")
        url = 'https://zipplex.ca/api/SignIn/refresh-token'
        res = self.session.post(url, headers=self.auth_headers)
        res.raise_for_status()
        self.session.cookies.set("jwt", res.json()['token'])

    def validate_token(self):
        url = 'https://zipplex.ca/api/User/GetNumberOfToken'
        res = self.session.get(url, headers=self.auth_headers)
        if res.status_code != 200:
            raise Exception("Invalid JWT Token validation failed.")
        print("Session active and valid.")

    def get_research_item(self, research_id: int) -> dict:
        url = f'https://zipplex.ca/api/Research/{research_id}'
        res = self.session.get(url, headers=self.auth_headers)
        res.raise_for_status()
        data = res.json()
        save_debug_file(f"research/{research_id}.json", data)
        return data

    def search(self, keyword: str, limit: int = 5):
        params = {
            'country': MAPBOX_API_COUNTRY_CODE,
            'limit': limit,
            'proximity': 'ip',
            'types': 'region,postcode,district,place,locality,neighborhood,address',
            'language': MAPBOX_API_LANGUAGE_CODE,
            'session_token': MAPBOX_API_SESSION_TOKEN,
            'access_token': MAPBOX_API_ACCESS_TOKEN,
        }
        url = f'https://api.mapbox.com/geocoding/v5/mapbox.places/{quote(keyword)}.json'
        res = self.session.get(url, params=params)
        res.raise_for_status()
        return res.json()

    def select_item(self, feature: dict) -> int:
        json_data = {
            'address': feature['place_name'],
            'validStep': 1,
            'location': {
                'type': 'Point',
                'coordinates': feature['center'],
            },
            'typeAppartment': {},
            'features': {},
        }
        url = 'https://zipplex.ca/api/Research/Insert'
        res = self.session.post(url, json=json_data, headers=self.auth_headers)
        res.raise_for_status()
        return int(res.text)

    def select_year(self, research: dict, year: int) -> dict:
        payload = {
            **research,
            'toBeBuild': 1,
            "renovated": 0,
            "constructionYear": year,
            "validStep": 2,
        }
        url = 'https://zipplex.ca/api/Research/Insert'
        res = self.session.post(url, json=payload, headers=self.auth_headers)
        res.raise_for_status()
        return payload

    def select_building_features(self, research_item: dict, options: dict):
        default_options = {
            "electricity": 0, "heating": 0, "internet": 0, "storageLocker": 0,
            "furnished": 0, "totalElectro": 0, "totalInteriorParking": 0,
            "totalExteriorParking": 0, "elevator": 0, "airConditioning": 0
        }
        payload = {
            **research_item,
            "features": {**default_options, **options},
            "validStep": 3
        }
        url = 'https://zipplex.ca/api/Research/Insert'
        res = self.session.post(url, json=payload, headers=self.auth_headers)
        res.raise_for_status()

    def get_result_graph(self, research_id: int, nomenclature: int) -> dict:
        url = f'https://zipplex.ca/api/Result/Graph/{research_id}'
        res = self.session.get(url, params={'nomeclature': nomenclature}, headers=self.auth_headers)
        res.raise_for_status()
        return res.json()

# --- PROCESSING ---

def parse_result_graph(data: dict):
    avg = data.get('average', 0)
    percentiles = data.get('percentiles', [0, 0, 0, 0, 0, 0])
    min_val, max_val = percentiles[0], percentiles[-1]
    step = (max_val - min_val) / 5
    boundaries = [round(min_val + (i + 1) * step) for i in range(4)]

    return {
        'adjusted_average_price': avg,
        'boundaries': boundaries
    }

def find_item(zipplex: Zipplex, keyword: str, year: int, building_options: dict):
    print(f"\n--- Searching for: {keyword} ---")
    search_results = zipplex.search(keyword)

    if not search_results.get('features'):
        print("No results found on Mapbox.")
        return None

    feature = search_results['features'][0]
    print(f"Selected Address: {feature['place_name']}")

    # Steps 1-4
    research_id = zipplex.select_item(feature)
    item = zipplex.get_research_item(research_id)
    item = zipplex.select_year(item, year)
    zipplex.select_building_features(item, building_options)

    # Step 5: Data collection
    labels = ["Studio", "1 chambre", "2 chambres", "3 chambres", "4 chambres+"]
    rows_to_write = []

    for i in range(5):
        print(f"Fetching data for {labels[i]}...")
        raw_data = zipplex.get_result_graph(research_id, i)
        parsed = parse_result_graph(raw_data)

        lower_bound = int(parsed["boundaries"][1])
        upper_bound = int(parsed["boundaries"][2])
        market_avg = int((lower_bound + upper_bound) / 2)
        adjustment = parsed["adjusted_average_price"] - market_avg

        rows_to_write.append([
            labels[i], parsed["adjusted_average_price"], lower_bound,
            market_avg, upper_bound, adjustment
        ])

    # CSV Export
    csv_filename = f"{sanitize_filename(keyword)}.csv"
    with open(csv_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Zipplex", "Moy. ajustée", "Borne inf. marché", "Moy. marché", "Borne sup. marché", "Ajustement inclusion"])
        writer.writerows(rows_to_write)

    print(f"\nSuccess! File saved as: {csv_filename}")
    return {
        "keyword": keyword,
        "address": feature['place_name'],
        "rows": rows_to_write
    }

# --- GOOGLE SHEETS ---

def get_google_sheet():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")

    if not creds_json or not sheet_id:
        raise Exception("Missing GOOGLE_CREDENTIALS or GOOGLE_SHEET_ID environment variables.")

    creds_dict = json.loads(creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(credentials)
    return client.open_by_key(sheet_id)

def upload_to_google_sheets(all_results: list):
    print("\n--- Uploading to Google Sheets ---")
    spreadsheet = get_google_sheet()

    header = ["Address", "Scraped At", "Zipplex", "Moy. ajustée", "Borne inf. marché", "Moy. marché", "Borne sup. marché", "Ajustement inclusion"]
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    # Use the first worksheet, or create one named "Scraper Data"
    try:
        worksheet = spreadsheet.worksheet("Scraper Data")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title="Scraper Data", rows=1000, cols=10)

    # If sheet is empty, write the header first
    existing = worksheet.get_all_values()
    if not existing:
        worksheet.append_row(header, value_input_option="USER_ENTERED")

    # Append all scraped rows
    rows_to_append = []
    for result in all_results:
        address = result["address"]
        for row in result["rows"]:
            rows_to_append.append([address, timestamp] + row)

    if rows_to_append:
        worksheet.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"Uploaded {len(rows_to_append)} rows to Google Sheets.")
    else:
        print("No data to upload.")

# --- MAIN (NON-INTERACTIVE FOR GITHUB ACTIONS) ---

def main():
    try:
        zipplex = Zipplex(cookies=HTTP_COOKIES)
        print("\n--- ZIPPLEX DATA SCRAPER (Automated) ---")

        # Load addresses from config file
        config_path = os.path.join(os.path.dirname(__file__), "addresses.json")
        with open(config_path, "r", encoding="utf-8") as f:
            addresses = json.load(f)

        all_results = []
        for entry in addresses:
            keyword = entry["keyword"]
            year = entry.get("year", 2020)
            options = entry.get("options", {})

            result = find_item(zipplex, keyword, year, options)
            if result:
                all_results.append(result)

        # Upload to Google Sheets
        if all_results:
            upload_to_google_sheets(all_results)
        else:
            print("No results to upload.")

        print("\nDone!")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        raise

if __name__ == "__main__":
    main()
