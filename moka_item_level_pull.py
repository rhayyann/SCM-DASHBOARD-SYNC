"""
MOKA ITEM-LEVEL PULL — script SEKALI JALAN, per TAHUN, per SPREADSHEET terpisah
Dipicu manual lewat workflow_dispatch. Reusable untuk tahun manapun — tahun
dan Sheet ID tujuan diberikan lewat input workflow, bukan hardcode di kode.

KENAPA PER TAHUN, SPREADSHEET TERPISAH
---------------------------------------
Estimasi ~1.451 baris per store per bulan (level item). Untuk 1 tahun (12
bulan x 12 store) itu ~209 ribu baris -- aman jauh di bawah batas 10 juta
cell per spreadsheet punya Google Sheets. Kalau semua tahun (2021-2026)
digabung jadi satu spreadsheet, totalnya ~1,18 juta baris -- terlalu dekat
ke batas itu dan bikin Sheets berat dibuka. Makanya 1 spreadsheet = 1 tahun.

CARA PAKAI
----------
1. Buat 1 Google Sheet BARU (kosong) khusus untuk tahun yang mau ditarik.
2. Share spreadsheet itu ke email service account yang sama dipakai sync
   mingguan (cek di file JSON service account -> field "client_email", atau
   lihat siapa yang sudah di-share di spreadsheet dashboard utama Anda).
3. Ambil Sheet ID dari URL spreadsheet baru itu.
4. Jalankan workflow "Moka Item-Level Pull (One-Time, Per Tahun)" -> isi
   input "year" (misal 2021) dan "target_sheet_id" (ID dari langkah 3).

Kredensial Moka & Config/refresh-token TETAP dibaca dari spreadsheet dashboard
utama (GOOGLE_SHEET_ID, secret yang sudah ada) -- tidak perlu setup OAuth
Moka baru. Yang beda cuma KE MANA data item-nya ditulis.

Env yang dipakai:
  MOKA_CLIENT_ID, MOKA_CLIENT_SECRET, MOKA_OUTLET_MAP,
  GOOGLE_SERVICE_ACCOUNT_JSON  (sama seperti sync mingguan)
  GOOGLE_SHEET_ID              (spreadsheet dashboard utama -- sumber Config/token)
  MOKA_ITEM_PULL_YEAR          (tahun yang ditarik, misal "2021")
  MOKA_ITEM_PULL_TARGET_SHEET  (Sheet ID spreadsheet KHUSUS tahun itu, tujuan tulis)
"""

import os
import sys
import json
import calendar
from datetime import datetime, date, timedelta, timezone

import requests
import gspread
from google.oauth2.service_account import Credentials

MAKASSAR_TZ = timezone(timedelta(hours=8))

TOKEN_URL = "https://api.mokapos.com/oauth/token"
API_BASE = "https://api.mokapos.com"

SHEET_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CONFIG_SHEET_NAME = "Config"
ITEMSALES_SHEET_NAME = "ItemSales"
REFRESH_TOKEN_CELL = "B1"

MOKA_CLIENT_ID = os.environ.get("MOKA_CLIENT_ID")
MOKA_CLIENT_SECRET = os.environ.get("MOKA_CLIENT_SECRET")
MOKA_OUTLET_MAP_RAW = os.environ.get("MOKA_OUTLET_MAP", "{}")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")  # spreadsheet dashboard utama (Config/token)

PULL_YEAR_RAW = os.environ.get("MOKA_ITEM_PULL_YEAR", "").strip()
TARGET_SHEET_ID = os.environ.get("MOKA_ITEM_PULL_TARGET_SHEET", "").strip()

MONTH_ID = [
    "JANUARI", "FEBRUARI", "MARET", "APRIL", "MEI", "JUNI",
    "JULI", "AGUSTUS", "SEPTEMBER", "OKTOBER", "NOVEMBER", "DESEMBER",
]

HEADER_ROW = ["STORE", "BULAN", "TAHUN", "CATEGORY", "ITEM", "QTY TERJUAL", "NET SALES", "LAST UPDATED"]

WRITE_BATCH_SIZE = 5000  # baris per panggilan append_rows, biar aman dari limit payload API


# ---------------------------------------------------------------------------
def require_env():
    missing = [
        name for name, val in [
            ("MOKA_CLIENT_ID", MOKA_CLIENT_ID),
            ("MOKA_CLIENT_SECRET", MOKA_CLIENT_SECRET),
            ("GOOGLE_SERVICE_ACCOUNT_JSON", GOOGLE_SERVICE_ACCOUNT_JSON),
            ("GOOGLE_SHEET_ID", GOOGLE_SHEET_ID),
        ] if not val
    ]
    if missing:
        print(f"[FATAL] Environment variable belum diset: {', '.join(missing)}")
        sys.exit(1)

    if not PULL_YEAR_RAW or not PULL_YEAR_RAW.isdigit():
        print(f"[FATAL] Input 'year' wajib diisi angka, contoh 2021. Diterima: '{PULL_YEAR_RAW}'")
        sys.exit(1)

    if not TARGET_SHEET_ID:
        print("[FATAL] Input 'target_sheet_id' wajib diisi -- Sheet ID spreadsheet "
              "KHUSUS tahun ini (bukan spreadsheet dashboard utama).")
        sys.exit(1)

    try:
        outlet_map = json.loads(MOKA_OUTLET_MAP_RAW)
        if not outlet_map:
            raise ValueError("kosong")
    except (json.JSONDecodeError, ValueError):
        print("[FATAL] MOKA_OUTLET_MAP harus JSON valid.")
        sys.exit(1)

    try:
        sa_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    except json.JSONDecodeError:
        print("[FATAL] GOOGLE_SERVICE_ACCOUNT_JSON bukan JSON valid.")
        sys.exit(1)

    return outlet_map, sa_info


def open_config_sheet(gc, sa_info):
    try:
        sh = gc.open_by_key(GOOGLE_SHEET_ID)
    except gspread.exceptions.APIError as e:
        print(f"[FATAL] Tidak bisa buka spreadsheet dashboard utama: {e}")
        sys.exit(1)
    try:
        return sh.worksheet(CONFIG_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print(f"[FATAL] Tab '{CONFIG_SHEET_NAME}' tidak ditemukan di spreadsheet utama.")
        sys.exit(1)


def open_target_sheet(gc, sa_info):
    try:
        sh = gc.open_by_key(TARGET_SHEET_ID)
    except gspread.exceptions.APIError as e:
        print(f"[FATAL] Tidak bisa buka spreadsheet tujuan (target_sheet_id): {e}")
        print("Pastikan spreadsheet BARU untuk tahun ini sudah di-share ke email "
              f"service account ({sa_info.get('client_email')}) sebagai Editor.")
        sys.exit(1)

    try:
        ws = sh.worksheet(ITEMSALES_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=ITEMSALES_SHEET_NAME, rows=1000, cols=len(HEADER_ROW) + 2)
        ws.append_row(HEADER_ROW)
        print(f"[OK] Tab '{ITEMSALES_SHEET_NAME}' dibuat di spreadsheet tujuan.")
        return ws

    if not ws.row_values(1):
        ws.append_row(HEADER_ROW)
    return ws


def get_stored_refresh_token(config_ws):
    token = config_ws.acell(REFRESH_TOKEN_CELL).value
    if not token:
        print(f"[FATAL] Sel {CONFIG_SHEET_NAME}!{REFRESH_TOKEN_CELL} kosong.")
        sys.exit(1)
    return token.strip()


def save_refresh_token(config_ws, token):
    config_ws.update_acell(REFRESH_TOKEN_CELL, token)


def refresh_access_token(config_ws, refresh_token):
    resp = requests.post(
        TOKEN_URL,
        json={
            "client_id": MOKA_CLIENT_ID,
            "client_secret": MOKA_CLIENT_SECRET,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    if resp.status_code != 200:
        print(f"[FATAL] Gagal refresh access_token: {resp.status_code} {resp.text}")
        sys.exit(1)

    data = resp.json()
    access_token = data.get("access_token")
    new_refresh_token = data.get("refresh_token")
    if new_refresh_token and new_refresh_token != refresh_token:
        save_refresh_token(config_ws, new_refresh_token)
        print("[OK] refresh_token dirotasi & ditulis ulang.")

    return access_token


# ---------------------------------------------------------------------------
def months_in_year(year, now):
    months = []
    for month in range(1, 13):
        days_in_month = calendar.monthrange(year, month)[1]
        start_d = date(year, month, 1)
        if (year, month) == (now.year, now.month):
            end_d = date(year, month, min(days_in_month, now.day))
        elif (year, month) > (now.year, now.month):
            break  # jangan tarik bulan yang belum terjadi
        else:
            end_d = date(year, month, days_in_month)
        months.append((month, start_d, end_d))
    return months


def fetch_outlet_item_sales(outlet_id, access_token, start, end):
    url = f"{API_BASE}/v3/outlets/{outlet_id}/reports/item_sales"
    params = {
        "start": start.strftime("%d/%m/%Y"),
        "end": end.strftime("%d/%m/%Y"),
    }
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        params=params,
        timeout=30,
    )
    if resp.status_code != 200:
        print(f"[WARN] Outlet {outlet_id} ({start}..{end}) gagal: {resp.status_code} {resp.text}")
        return None
    return resp.json().get("data", {}).get("item_sales", [])


def get_first(item, keys, default=None):
    for k in keys:
        if k in item and item[k] not in (None, ""):
            return item[k]
    return default


# ---------------------------------------------------------------------------
def write_rows_batched(ws, rows):
    """Tulis banyak baris ke sheet dalam beberapa batch supaya aman dari
    limit ukuran payload / rate limit Google Sheets API."""
    total = len(rows)
    written = 0
    for i in range(0, total, WRITE_BATCH_SIZE):
        chunk = rows[i:i + WRITE_BATCH_SIZE]
        ws.append_rows(chunk, value_input_option="USER_ENTERED")
        written += len(chunk)
        print(f"    [WRITE] {written}/{total} baris tertulis...")
    return written


# ---------------------------------------------------------------------------
def main():
    outlet_map, sa_info = require_env()
    year = int(PULL_YEAR_RAW)

    creds = Credentials.from_service_account_info(sa_info, scopes=SHEET_SCOPES)
    gc = gspread.authorize(creds)

    config_ws = open_config_sheet(gc, sa_info)
    target_ws = open_target_sheet(gc, sa_info)

    stored_refresh_token = get_stored_refresh_token(config_ws)
    access_token = refresh_access_token(config_ws, stored_refresh_token)

    now = datetime.now(MAKASSAR_TZ)
    months = months_in_year(year, now)
    if not months:
        print(f"[FATAL] Tahun {year} berada di masa depan, tidak ada bulan untuk ditarik.")
        sys.exit(1)

    print(f"[INFO] Menarik data ITEM-LEVEL tahun {year}: {len(months)} bulan x "
          f"{len(outlet_map)} outlet.")

    now_str = now.strftime("%Y-%m-%d %H:%M WITA")
    all_rows = []
    printed_sample = False

    for (month, start_d, end_d) in months:
        bulan = MONTH_ID[month - 1]
        tahun = str(year)
        month_rows = 0

        for outlet_id, store_name in outlet_map.items():
            item_sales = fetch_outlet_item_sales(outlet_id, access_token, start_d, end_d)
            if not item_sales:
                continue

            if not printed_sample:
                # Tampilkan 1 contoh mentah di log, buat verifikasi nama field
                # (quantity/qty) sudah sesuai skema API akun Anda.
                print(f"[DEBUG] Contoh 1 item mentah dari API: {json.dumps(item_sales[0])[:500]}")
                printed_sample = True

            for item in item_sales:
                category = str(item.get("category_name") or "UNCATEGORIZED").strip().upper()
                item_name = get_first(item, ["item_name", "name", "product_name"], "UNKNOWN ITEM")
                qty = get_first(item, ["quantity", "qty", "qty_sold", "item_quantity"], 0)
                net_sales = item.get("net_sales") or 0

                all_rows.append([
                    store_name, bulan, tahun, category, item_name, qty, net_sales, now_str,
                ])
                month_rows += 1

        print(f"  [{bulan} {tahun}] {month_rows} baris item terkumpul.")

    if not all_rows:
        print("[FATAL] Tidak ada data sama sekali yang berhasil ditarik untuk tahun ini.")
        sys.exit(1)

    print(f"[INFO] Total {len(all_rows)} baris siap ditulis ke spreadsheet tujuan...")
    written = write_rows_batched(target_ws, all_rows)
    print(f"[OK] Selesai. {written} baris item-level tahun {year} berhasil ditulis.")


if __name__ == "__main__":
    main()
