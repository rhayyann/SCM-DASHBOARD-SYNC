"""
MOKA ALL-TIME CATEGORY PULL — script SEKALI JALAN (bukan cron harian)
Dipicu manual lewat workflow_dispatch di GitHub Actions.

TUJUAN
------
Menarik histori penjualan dari awal pencatatan (default Januari 2021) sampai
bulan berjalan, per BULAN (bukan per minggu), format PANJANG (long format):
satu baris per kombinasi STORE + BULAN + TAHUN + CATEGORY. Semua kategori
yang muncul di data Moka ikut ditarik (tidak dibatasi 7 kategori yang dipakai
di tab NetSales mingguan), supaya laporan ini punya gambaran lengkap.

TIDAK MENYENTUH tab lain (NetSales, SyncLog, Config hanya dibaca untuk token).
Ditulis ke tab BARU bernama "NetSales_AllTime_ByCategory".

Pakai kredensial & secrets GitHub yang SAMA dengan sync mingguan — tidak ada
setup Moka API baru yang diperlukan.

Cara pakai: jalankan workflow "Moka All-Time Category Pull" sekali dari tab
Actions. Aman dijalankan berkali-kali (idempotent, upsert berdasarkan
STORE+BULAN+TAHUN+CATEGORY) kalau perlu di-refresh manual di kemudian hari.

Env opsional:
  MOKA_ALLTIME_START  format "YYYY-MM", default "2021-01"
  MOKA_ALLTIME_END    format "YYYY-MM", default bulan berjalan
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
ALLTIME_SHEET_NAME = "NetSales_AllTime_ByCategory"
REFRESH_TOKEN_CELL = "B1"

MOKA_CLIENT_ID = os.environ.get("MOKA_CLIENT_ID")
MOKA_CLIENT_SECRET = os.environ.get("MOKA_CLIENT_SECRET")
MOKA_OUTLET_MAP_RAW = os.environ.get("MOKA_OUTLET_MAP", "{}")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")

ALLTIME_START_RAW = os.environ.get("MOKA_ALLTIME_START", "2021-01").strip()
ALLTIME_END_RAW = os.environ.get("MOKA_ALLTIME_END", "").strip()

MONTH_ID = [
    "JANUARI", "FEBRUARI", "MARET", "APRIL", "MEI", "JUNI",
    "JULI", "AGUSTUS", "SEPTEMBER", "OKTOBER", "NOVEMBER", "DESEMBER",
]

HEADER_ROW = ["STORE", "BULAN", "TAHUN", "CATEGORY", "NET SALES", "LAST UPDATED"]


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


def open_sheets(sa_info):
    creds = Credentials.from_service_account_info(sa_info, scopes=SHEET_SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    try:
        config_ws = sh.worksheet(CONFIG_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print(f"[FATAL] Tab '{CONFIG_SHEET_NAME}' tidak ditemukan.")
        sys.exit(1)

    try:
        alltime_ws = sh.worksheet(ALLTIME_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        alltime_ws = sh.add_worksheet(title=ALLTIME_SHEET_NAME, rows=2000, cols=len(HEADER_ROW) + 2)
        alltime_ws.append_row(HEADER_ROW)
        print(f"[OK] Tab baru '{ALLTIME_SHEET_NAME}' dibuat.")

    return config_ws, alltime_ws


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
        print(f"[OK] refresh_token dirotasi & ditulis ulang.")

    return access_token


# ---------------------------------------------------------------------------
def add_month(year, month, delta):
    idx = (year * 12 + (month - 1)) + delta
    return idx // 12, idx % 12 + 1


def months_to_process():
    now = datetime.now(MAKASSAR_TZ)

    try:
        sy, sm = (int(x) for x in ALLTIME_START_RAW.split("-"))
    except ValueError:
        print(f"[FATAL] MOKA_ALLTIME_START format salah: '{ALLTIME_START_RAW}' (harus YYYY-MM)")
        sys.exit(1)

    if ALLTIME_END_RAW:
        try:
            ey, em = (int(x) for x in ALLTIME_END_RAW.split("-"))
        except ValueError:
            print(f"[FATAL] MOKA_ALLTIME_END format salah: '{ALLTIME_END_RAW}' (harus YYYY-MM)")
            sys.exit(1)
    else:
        ey, em = now.year, now.month

    months = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        days_in_month = calendar.monthrange(y, m)[1]
        start_d = date(y, m, 1)
        if (y, m) == (now.year, now.month):
            end_d = date(y, m, min(days_in_month, now.day))
        else:
            end_d = date(y, m, days_in_month)
        months.append((y, m, start_d, end_d))
        y, m = add_month(y, m, 1)

    return months


# ---------------------------------------------------------------------------
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


def compute_all_category_breakdown(item_sales):
    """Jumlahkan net_sales per kategori APAPUN yang muncul di data (tidak
    dibatasi daftar kategori tetap)."""
    breakdown = {}
    for item in item_sales:
        category = str(item.get("category_name") or "UNCATEGORIZED").strip().upper()
        breakdown[category] = breakdown.get(category, 0.0) + (item.get("net_sales") or 0)
    return breakdown


# ---------------------------------------------------------------------------
def make_key(store, bulan, tahun, category):
    return f"{str(store).strip().upper()}|{str(bulan).strip().upper()}|{str(tahun).strip()}|{str(category).strip().upper()}"


def upsert_alltime(alltime_ws, rows):
    all_values = alltime_ws.get_all_values()
    if not all_values:
        alltime_ws.append_row(HEADER_ROW)
        headers = HEADER_ROW
        existing = []
    else:
        headers = [h.strip() for h in all_values[0]]
        existing = all_values[1:]

    idx = {h: i for i, h in enumerate(headers)}
    for required in HEADER_ROW:
        if required not in idx:
            print(f"[FATAL] Kolom '{required}' tidak ditemukan di header tab {ALLTIME_SHEET_NAME}.")
            sys.exit(1)

    key_to_rownum = {}
    for i, r in enumerate(existing):
        def cell(col):
            return r[idx[col]] if idx[col] < len(r) else ""
        key = make_key(cell("STORE"), cell("BULAN"), cell("TAHUN"), cell("CATEGORY"))
        key_to_rownum[key] = i + 2

    updated, inserted = 0, 0
    new_rows = []
    now_str = datetime.now(MAKASSAR_TZ).strftime("%Y-%m-%d %H:%M WITA")

    for rec in rows:
        key = make_key(rec["store"], rec["bulan"], rec["tahun"], rec["category"])
        row_num = key_to_rownum.get(key)

        row_values = [""] * len(headers)
        row_values[idx["STORE"]] = rec["store"]
        row_values[idx["BULAN"]] = rec["bulan"]
        row_values[idx["TAHUN"]] = rec["tahun"]
        row_values[idx["CATEGORY"]] = rec["category"]
        row_values[idx["NET SALES"]] = rec["net_sales"]
        row_values[idx["LAST UPDATED"]] = now_str

        if row_num:
            last_col_letter = gspread.utils.rowcol_to_a1(1, len(headers)).rstrip("1")
            alltime_ws.update(f"A{row_num}:{last_col_letter}{row_num}", [row_values])
            updated += 1
        else:
            new_rows.append(row_values)
            inserted += 1

    if new_rows:
        alltime_ws.append_rows(new_rows)

    return updated, inserted


# ---------------------------------------------------------------------------
def main():
    outlet_map, sa_info = require_env()
    config_ws, alltime_ws = open_sheets(sa_info)

    stored_refresh_token = get_stored_refresh_token(config_ws)
    access_token = refresh_access_token(config_ws, stored_refresh_token)

    months = months_to_process()
    print(f"[INFO] Menarik {len(months)} bulan x {len(outlet_map)} outlet "
          f"({months[0][0]}-{months[0][1]:02d} s.d. {months[-1][0]}-{months[-1][1]:02d})")

    rows = []
    fetched, skipped = 0, 0

    for (year, month, start_d, end_d) in months:
        bulan = MONTH_ID[month - 1]
        tahun = str(year)

        for outlet_id, store_name in outlet_map.items():
            item_sales = fetch_outlet_item_sales(outlet_id, access_token, start_d, end_d)
            if item_sales is None:
                skipped += 1
                continue
            if not item_sales:
                continue  # tidak ada transaksi bulan itu -> lewati diam-diam

            breakdown = compute_all_category_breakdown(item_sales)
            for category, net_sales in breakdown.items():
                rows.append({
                    "store": store_name,
                    "bulan": bulan,
                    "tahun": tahun,
                    "category": category,
                    "net_sales": net_sales,
                })
            fetched += 1

        print(f"  [{bulan} {tahun}] selesai ({fetched} outlet-bulan tertarik sejauh ini, "
              f"{skipped} gagal)")

    if not rows:
        print("[FATAL] Tidak ada data sama sekali yang berhasil ditarik.")
        sys.exit(1)

    updated, inserted = upsert_alltime(alltime_ws, rows)
    print(f"[OK] Selesai. Baris diperbarui: {updated}, baris baru: {inserted}. "
          f"Total outlet-bulan gagal: {skipped}")


if __name__ == "__main__":
    main()
