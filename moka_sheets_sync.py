"""
MOKA NET SALES SYNC (versi Google Service Account — tanpa Apps Script)
Dijalankan oleh GitHub Actions cron tiap jam 07:00 WIB.

Kenapa tanpa Apps Script: deployment Apps Script sebagai Web App butuh
proses verifikasi OAuth dari Google kalau scope-nya dianggap sensitif.
Service Account tidak melalui alur consent itu sama sekali — cukup dibuat
sekali di Google Cloud Console, lalu di-invite sebagai Editor ke spreadsheet
tujuan, dan bisa langsung baca/tulis lewat Sheets API.

Struktur spreadsheet baru (2 tab):
  - "Config"   -> sel B1 menyimpan refresh_token Moka (baca & tulis ulang
                  otomatis kalau Moka merotasi token)
  - "NetSales" -> header row 1:
      STORE | BULAN | TAHUN | COMBED 24S | COMBED 30S | KIDS 24S |
      TUNIK 24S | PANJANG + RIB | REJECT | WANGKI MYNO |
      NET SALES (TOTAL) | NET SALES (WO DEFECT)

Alur:
1. Baca refresh_token dari Config!B1.
2. Tukar jadi access_token baru (POST /oauth/token, grant_type=refresh_token).
   Kalau Moka mengembalikan refresh_token baru, tulis ulang ke Config!B1.
3. Untuk tiap outlet, ambil breakdown item_sales month-to-date, jumlahkan
   net_sales per kategori target, hitung TOTAL dan WO DEFECT (TOTAL - REJECT).
4. Upsert ke tab NetSales (cocok berdasarkan STORE + BULAN + TAHUN).

Referensi API Moka: https://api.mokapos.com/docs
Kredensial diambil dari environment variable (GitHub Secrets):
  MOKA_CLIENT_ID, MOKA_CLIENT_SECRET, MOKA_OUTLET_MAP,
  GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SHEET_ID
"""

import os
import sys
import json
from datetime import datetime, timedelta, timezone

import requests
import gspread
from google.oauth2.service_account import Credentials

WIB = timezone(timedelta(hours=7))
TOKEN_URL = "https://api.mokapos.com/oauth/token"
API_BASE = "https://api.mokapos.com"

SHEET_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CONFIG_SHEET_NAME = "Config"
NETSALES_SHEET_NAME = "NetSales"
REFRESH_TOKEN_CELL = "B1"

MOKA_CLIENT_ID = os.environ.get("MOKA_CLIENT_ID")
MOKA_CLIENT_SECRET = os.environ.get("MOKA_CLIENT_SECRET")
MOKA_OUTLET_MAP_RAW = os.environ.get("MOKA_OUTLET_MAP", "{}")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")

MONTH_ID = [
    "JANUARI", "FEBRUARI", "MARET", "APRIL", "MEI", "JUNI",
    "JULI", "AGUSTUS", "SEPTEMBER", "OKTOBER", "NOVEMBER", "DESEMBER",
]

# Urutan kolom kategori di tab NetSales -- harus sama persis dengan header.
CATEGORY_COLUMNS = [
    "COMBED 24S", "COMBED 30S", "KIDS 24S", "TUNIK 24S",
    "PANJANG + RIB", "REJECT", "WANGKI MYNO",
]
DEFECT_CATEGORY = "REJECT"

HEADER_ROW = (
    ["STORE", "BULAN", "TAHUN"]
    + CATEGORY_COLUMNS
    + ["NET SALES (TOTAL)", "NET SALES (WO DEFECT)"]
)


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

    try:
        outlet_map = json.loads(MOKA_OUTLET_MAP_RAW)
        if not outlet_map:
            raise ValueError("kosong")
    except (json.JSONDecodeError, ValueError):
        print("[FATAL] MOKA_OUTLET_MAP harus JSON valid, contoh: "
              '{"442608":"TIGALAPANKAOS MAKASSAR"}')
        sys.exit(1)

    try:
        sa_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    except json.JSONDecodeError:
        print("[FATAL] GOOGLE_SERVICE_ACCOUNT_JSON bukan JSON valid.")
        sys.exit(1)

    return outlet_map, sa_info


def open_sheets(sa_info):
    creds = Credentials.from_service_account_info(sa_info, scopes=SHEET_SCOPES)
    gc = gspread.authorize(creds)
    try:
        sh = gc.open_by_key(GOOGLE_SHEET_ID)
    except gspread.exceptions.APIError as e:
        print(f"[FATAL] Tidak bisa buka spreadsheet: {e}")
        print("Pastikan spreadsheet sudah di-share ke email service account "
              f"({sa_info.get('client_email')}) sebagai Editor.")
        sys.exit(1)

    try:
        config_ws = sh.worksheet(CONFIG_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print(f"[FATAL] Tab '{CONFIG_SHEET_NAME}' tidak ditemukan di spreadsheet.")
        sys.exit(1)

    try:
        netsales_ws = sh.worksheet(NETSALES_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print(f"[FATAL] Tab '{NETSALES_SHEET_NAME}' tidak ditemukan di spreadsheet.")
        sys.exit(1)

    return config_ws, netsales_ws


def get_stored_refresh_token(config_ws):
    token = config_ws.acell(REFRESH_TOKEN_CELL).value
    if not token:
        print(f"[FATAL] Sel {CONFIG_SHEET_NAME}!{REFRESH_TOKEN_CELL} kosong. "
              "Isi manual dengan refresh_token hasil tukar authorization code dulu.")
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
        print("Kemungkinan refresh_token sudah di-revoke. Perlu ambil authorization "
              "code baru dari Back Office lalu tukar manual lewat Postman, isi ulang "
              f"ke {CONFIG_SHEET_NAME}!{REFRESH_TOKEN_CELL}.")
        sys.exit(1)

    data = resp.json()
    access_token = data.get("access_token")
    new_refresh_token = data.get("refresh_token")

    if new_refresh_token and new_refresh_token != refresh_token:
        save_refresh_token(config_ws, new_refresh_token)
        print(f"[OK] refresh_token dirotasi, sudah ditulis ulang ke "
              f"{CONFIG_SHEET_NAME}!{REFRESH_TOKEN_CELL}.")

    return access_token


def month_to_date_range():
    now = datetime.now(WIB)
    start = now.replace(day=1)
    return start, now


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
        print(f"[WARN] Outlet {outlet_id} gagal diambil: {resp.status_code} {resp.text}")
        return None
    return resp.json().get("data", {}).get("item_sales", [])


def compute_category_breakdown(item_sales):
    """Jumlahkan net_sales per kategori target -> dict {kategori: total}."""
    breakdown = {cat: 0.0 for cat in CATEGORY_COLUMNS}
    for item in item_sales:
        category = str(item.get("category_name") or "").strip().upper()
        if category in breakdown:
            breakdown[category] += item.get("net_sales") or 0
    return breakdown


def upsert_netsales(netsales_ws, rows):
    """rows: list of dict {store, bulan, tahun, <kategori>: nilai, net_total, net_wo_defect}"""
    all_values = netsales_ws.get_all_values()

    if not all_values:
        netsales_ws.append_row(HEADER_ROW)
        headers = HEADER_ROW
        existing = []
    else:
        headers = [h.strip() for h in all_values[0]]
        existing = all_values[1:]

    idx = {h: i for i, h in enumerate(headers)}
    for required in ["STORE", "BULAN", "TAHUN"] + CATEGORY_COLUMNS + ["NET SALES (TOTAL)", "NET SALES (WO DEFECT)"]:
        if required not in idx:
            print(f"[FATAL] Kolom '{required}' tidak ditemukan di header tab {NETSALES_SHEET_NAME}.")
            sys.exit(1)

    key_to_rownum = {}
    for i, r in enumerate(existing):
        def cell(col):
            return r[idx[col]] if idx[col] < len(r) else ""
        key = make_key(cell("STORE"), cell("BULAN"), cell("TAHUN"))
        key_to_rownum[key] = i + 2  # +2: header + 1-indexed

    updated, inserted = 0, 0
    new_rows = []

    for rec in rows:
        key = make_key(rec["store"], rec["bulan"], rec["tahun"])
        row_num = key_to_rownum.get(key)

        row_values = [""] * len(headers)
        row_values[idx["STORE"]] = rec["store"]
        row_values[idx["BULAN"]] = rec["bulan"]
        row_values[idx["TAHUN"]] = rec["tahun"]
        for cat in CATEGORY_COLUMNS:
            row_values[idx[cat]] = rec["breakdown"][cat]
        row_values[idx["NET SALES (TOTAL)"]] = rec["net_total"]
        row_values[idx["NET SALES (WO DEFECT)"]] = rec["net_wo_defect"]

        if row_num:
            last_col_letter = gspread.utils.rowcol_to_a1(1, len(headers)).rstrip("1")
            netsales_ws.update(f"A{row_num}:{last_col_letter}{row_num}", [row_values])
            updated += 1
        else:
            new_rows.append(row_values)
            inserted += 1

    if new_rows:
        netsales_ws.append_rows(new_rows)

    return updated, inserted


def make_key(store, bulan, tahun):
    return f"{str(store).strip().upper()}|{str(bulan).strip().upper()}|{str(tahun).strip()}"


def main():
    outlet_map, sa_info = require_env()
    config_ws, netsales_ws = open_sheets(sa_info)

    stored_refresh_token = get_stored_refresh_token(config_ws)
    access_token = refresh_access_token(config_ws, stored_refresh_token)

    start, end = month_to_date_range()
    bulan = MONTH_ID[start.month - 1]
    tahun = str(start.year)

    rows = []
    for outlet_id, store_name in outlet_map.items():
        item_sales = fetch_outlet_item_sales(outlet_id, access_token, start, end)
        if item_sales is None:
            continue

        breakdown = compute_category_breakdown(item_sales)
        net_total = sum(breakdown.values())
        net_defect = breakdown[DEFECT_CATEGORY]
        net_wo_defect = net_total - net_defect

        rows.append({
            "store": store_name,
            "bulan": bulan,
            "tahun": tahun,
            "breakdown": breakdown,
            "net_total": net_total,
            "net_wo_defect": net_wo_defect,
        })
        print(f"  {store_name} ({outlet_id}): TOTAL={net_total:,.0f} "
              f"DEFECT={net_defect:,.0f} WO_DEFECT={net_wo_defect:,.0f}")

    if not rows:
        print("[FATAL] Tidak ada data yang berhasil diambil dari outlet manapun.")
        sys.exit(1)

    updated, inserted = upsert_netsales(netsales_ws, rows)
    print(f"[OK] Sheet ter-update. Diperbarui: {updated}, baris baru: {inserted}")


if __name__ == "__main__":
    main()
