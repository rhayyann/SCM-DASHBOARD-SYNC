"""
MOKA NET SALES SYNC — versi Mingguan (v2)
Dipicu oleh cron-job.org via GitHub repository_dispatch, jam 07:00 WITA
(Asia/Makassar, UTC+8) setiap hari. Ada juga jadwal cadangan (GitHub Actions
schedule) kalau-kalau cron-job.org gagal trigger.

PERUBAHAN DARI VERSI SEBELUMNYA
--------------------------------
1. Data sekarang dipecah per MINGGU dalam bulan (bukan cuma total bulanan):
     Minggu 1 = tanggal 1-7
     Minggu 2 = tanggal 8-14
     Minggu 3 = tanggal 15-21
     Minggu 4 = tanggal 22 - akhir bulan
2. Ada 2 mode jalan, diatur lewat env MOKA_SYNC_MODE:
     - "daily"    (default, dipakai cron harian): hanya memproses & meng-upsert
       minggu-minggu yang SUDAH DIMULAI di bulan BERJALAN (bulan saat script
       dijalankan). Minggu yang sedang berjalan dihitung "week-to-date" (dari
       tanggal 1 minggu itu sampai hari ini), jadi nilainya bertambah tiap hari
       tapi row-nya tidak nambah baru — hanya update value.
     - "backfill" (dijalankan manual SEKALI lewat workflow_dispatch): mengisi
       histori dari Januari 2026 sampai akhir bulan SEBELUM bulan berjalan
       (bisa dioverride lewat env MOKA_BACKFILL_END, format "YYYY-MM").
       Bulan-bulan ini dianggap sudah final/historical dan TIDAK akan disentuh
       lagi oleh mode "daily" karena mode daily hanya melihat bulan berjalan.
3. Kalau tab "NetSales" yang lama (format bulanan, tanpa kolom MINGGU)
   terdeteksi, otomatis di-rename jadi "NetSales_Old_Monthly_Archive" dan tab
   "NetSales" baru dibuat dengan header mingguan yang benar. Jadi tidak perlu
   ada penyesuaian header manual di spreadsheet.
4. Tiap kali script jalan, satu baris log ditulis ke tab "SyncLog" (dibuat
   otomatis kalau belum ada) — waktu jalan, mode, jumlah outlet sukses/gagal,
   catatan. Ini buat monitoring dari sisi spreadsheet, di luar dashboard
   cron-job.org / log GitHub Actions.

Struktur spreadsheet (3 tab):
  - "Config"    -> sel B1 menyimpan refresh_token Moka
  - "NetSales"  -> header:
      STORE | BULAN | TAHUN | MINGGU | PERIODE | COMBED 24S | COMBED 30S |
      KIDS 24S | TUNIK 24S | PANJANG + RIB | REJECT | WANGKI MYNO |
      NET SALES (TOTAL) | NET SALES (WO DEFECT) | LAST UPDATED
    Key unik per baris: STORE + BULAN + TAHUN + MINGGU
  - "SyncLog"   -> header: WAKTU | MODE | SUKSES | GAGAL | CATATAN

Referensi API Moka: https://api.mokapos.com/docs
Kredensial dari environment variable (GitHub Secrets):
  MOKA_CLIENT_ID, MOKA_CLIENT_SECRET, MOKA_OUTLET_MAP,
  GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SHEET_ID
Opsional:
  MOKA_SYNC_MODE   ("daily" default, atau "backfill")
  MOKA_BACKFILL_END (format "YYYY-MM", hanya dipakai kalau mode=backfill)
"""

import os
import sys
import json
import calendar
from datetime import datetime, date, timedelta, timezone

import requests
import gspread
from google.oauth2.service_account import Credentials

# Makassar / WITA = UTC+8 (bukan WIB/UTC+7 — koreksi dari versi sebelumnya)
MAKASSAR_TZ = timezone(timedelta(hours=8))

TOKEN_URL = "https://api.mokapos.com/oauth/token"
API_BASE = "https://api.mokapos.com"

SHEET_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CONFIG_SHEET_NAME = "Config"
NETSALES_SHEET_NAME = "NetSales"
NETSALES_ARCHIVE_NAME = "NetSales_Old_Monthly_Archive"
SYNCLOG_SHEET_NAME = "SyncLog"
REFRESH_TOKEN_CELL = "B1"

MOKA_CLIENT_ID = os.environ.get("MOKA_CLIENT_ID")
MOKA_CLIENT_SECRET = os.environ.get("MOKA_CLIENT_SECRET")
MOKA_OUTLET_MAP_RAW = os.environ.get("MOKA_OUTLET_MAP", "{}")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
SYNC_MODE = os.environ.get("MOKA_SYNC_MODE", "daily").strip().lower()
BACKFILL_END_RAW = os.environ.get("MOKA_BACKFILL_END", "").strip()

BACKFILL_START_YEAR = 2026
BACKFILL_START_MONTH = 1

MONTH_ID = [
    "JANUARI", "FEBRUARI", "MARET", "APRIL", "MEI", "JUNI",
    "JULI", "AGUSTUS", "SEPTEMBER", "OKTOBER", "NOVEMBER", "DESEMBER",
]

CATEGORY_COLUMNS = [
    "COMBED 24S", "COMBED 30S", "KIDS 24S", "TUNIK 24S",
    "PANJANG + RIB", "REJECT", "WANGKI MYNO",
]
DEFECT_CATEGORY = "REJECT"

HEADER_ROW = (
    ["STORE", "BULAN", "TAHUN", "MINGGU", "PERIODE"]
    + CATEGORY_COLUMNS
    + ["NET SALES (TOTAL)", "NET SALES (WO DEFECT)", "LAST UPDATED"]
)

SYNCLOG_HEADER = ["WAKTU", "MODE", "SUKSES", "GAGAL", "CATATAN"]


# ---------------------------------------------------------------------------
# Setup & validasi
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
        print("[FATAL] MOKA_OUTLET_MAP harus JSON valid, contoh: "
              '{"442608":"TIGALAPANKAOS MAKASSAR"}')
        sys.exit(1)

    try:
        sa_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    except json.JSONDecodeError:
        print("[FATAL] GOOGLE_SERVICE_ACCOUNT_JSON bukan JSON valid.")
        sys.exit(1)

    if SYNC_MODE not in ("daily", "backfill"):
        print(f"[FATAL] MOKA_SYNC_MODE tidak dikenal: '{SYNC_MODE}' "
              "(harus 'daily' atau 'backfill')")
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

    netsales_ws = get_or_migrate_netsales_tab(sh)
    synclog_ws = get_or_create_synclog_tab(sh)

    return config_ws, netsales_ws, synclog_ws


def get_or_migrate_netsales_tab(sh):
    """Ambil tab NetSales. Kalau formatnya masih format bulanan lama (tanpa
    kolom MINGGU), arsipkan tab lama itu dan buat tab NetSales baru dengan
    header mingguan yang benar — otomatis, tanpa perlu diedit manual."""
    try:
        ws = sh.worksheet(NETSALES_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=NETSALES_SHEET_NAME, rows=200, cols=len(HEADER_ROW) + 2)
        ws.append_row(HEADER_ROW)
        print(f"[OK] Tab '{NETSALES_SHEET_NAME}' baru dibuat dengan header mingguan.")
        return ws

    existing_header = ws.row_values(1)
    if existing_header and "MINGGU" in [h.strip().upper() for h in existing_header]:
        return ws  # sudah format baru, tidak perlu migrasi

    # Format lama terdeteksi (atau tab kosong tanpa header MINGGU) -> arsipkan
    archive_name = NETSALES_ARCHIVE_NAME
    suffix = 2
    existing_titles = [w.title for w in sh.worksheets()]
    while archive_name in existing_titles:
        archive_name = f"{NETSALES_ARCHIVE_NAME}_{suffix}"
        suffix += 1

    if existing_header:
        ws.update_title(archive_name)
        print(f"[OK] Tab NetSales format lama diarsipkan sebagai '{archive_name}'.")
        new_ws = sh.add_worksheet(title=NETSALES_SHEET_NAME, rows=200, cols=len(HEADER_ROW) + 2)
        new_ws.append_row(HEADER_ROW)
        print(f"[OK] Tab '{NETSALES_SHEET_NAME}' baru dibuat dengan header mingguan.")
        return new_ws
    else:
        ws.append_row(HEADER_ROW)
        return ws


def get_or_create_synclog_tab(sh):
    try:
        return sh.worksheet(SYNCLOG_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=SYNCLOG_SHEET_NAME, rows=1000, cols=len(SYNCLOG_HEADER) + 1)
        ws.append_row(SYNCLOG_HEADER)
        print(f"[OK] Tab '{SYNCLOG_SHEET_NAME}' dibuat untuk monitoring.")
        return ws


def log_sync_run(synclog_ws, mode, success_count, fail_count, note):
    now_str = datetime.now(MAKASSAR_TZ).strftime("%Y-%m-%d %H:%M WITA")
    synclog_ws.append_row([now_str, mode, success_count, fail_count, note])


# ---------------------------------------------------------------------------
# Moka OAuth
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Logika minggu (berbasis tanggal: 1-7 / 8-14 / 15-21 / 22-akhir bulan)
# ---------------------------------------------------------------------------

def week_ranges(year, month):
    """Kembalikan list (minggu_ke, tgl_mulai, tgl_akhir) untuk 1 bulan."""
    days_in_month = calendar.monthrange(year, month)[1]
    return [
        (1, 1, 7),
        (2, 8, 14),
        (3, 15, 21),
        (4, 22, days_in_month),
    ]


def add_month(year, month, delta):
    idx = (year * 12 + (month - 1)) + delta
    return idx // 12, idx % 12 + 1


def periods_to_process():
    """Kembalikan list (year, month, week_num, start_date, end_date) yang
    perlu diambil datanya, tergantung mode ('daily' vs 'backfill')."""
    now = datetime.now(MAKASSAR_TZ)
    periods = []

    if SYNC_MODE == "backfill":
        if BACKFILL_END_RAW:
            try:
                end_year, end_month = (int(x) for x in BACKFILL_END_RAW.split("-"))
            except ValueError:
                print(f"[FATAL] MOKA_BACKFILL_END format salah: '{BACKFILL_END_RAW}' "
                      "(harus 'YYYY-MM')")
                sys.exit(1)
        else:
            end_year, end_month = add_month(now.year, now.month, -1)

        y, m = BACKFILL_START_YEAR, BACKFILL_START_MONTH
        while (y, m) <= (end_year, end_month):
            days_in_month = calendar.monthrange(y, m)[1]
            for week_num, wstart, wend in week_ranges(y, m):
                start_d = date(y, m, wstart)
                end_d = date(y, m, min(wend, days_in_month))
                periods.append((y, m, week_num, start_d, end_d))
            y, m = add_month(y, m, 1)

    else:  # daily
        y, m, today_day = now.year, now.month, now.day
        for week_num, wstart, wend in week_ranges(y, m):
            if today_day < wstart:
                continue
            start_d = date(y, m, wstart)
            end_d = date(y, m, min(wend, today_day))
            periods.append((y, m, week_num, start_d, end_d))

    return periods


def periode_label(start_d, end_d, month_name):
    if start_d.month == end_d.month:
        return f"{start_d.day}-{end_d.day} {month_name.title()} {start_d.year}"
    return f"{start_d.day} {MONTH_ID[start_d.month-1].title()} - {end_d.day} {MONTH_ID[end_d.month-1].title()} {end_d.year}"


# ---------------------------------------------------------------------------
# Moka data fetch & aggregasi
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
        print(f"[WARN] Outlet {outlet_id} ({start}..{end}) gagal diambil: "
              f"{resp.status_code} {resp.text}")
        return None
    return resp.json().get("data", {}).get("item_sales", [])


def compute_category_breakdown(item_sales):
    breakdown = {cat: 0.0 for cat in CATEGORY_COLUMNS}
    for item in item_sales:
        category = str(item.get("category_name") or "").strip().upper()
        if category in breakdown:
            breakdown[category] += item.get("net_sales") or 0
    return breakdown


# ---------------------------------------------------------------------------
# Upsert ke NetSales
# ---------------------------------------------------------------------------

def make_key(store, bulan, tahun, minggu):
    return f"{str(store).strip().upper()}|{str(bulan).strip().upper()}|{str(tahun).strip()}|{str(minggu).strip()}"


def upsert_netsales(netsales_ws, rows):
    all_values = netsales_ws.get_all_values()

    if not all_values:
        netsales_ws.append_row(HEADER_ROW)
        headers = HEADER_ROW
        existing = []
    else:
        headers = [h.strip() for h in all_values[0]]
        existing = all_values[1:]

    idx = {h: i for i, h in enumerate(headers)}
    required_cols = (
        ["STORE", "BULAN", "TAHUN", "MINGGU", "PERIODE"]
        + CATEGORY_COLUMNS
        + ["NET SALES (TOTAL)", "NET SALES (WO DEFECT)", "LAST UPDATED"]
    )
    for required in required_cols:
        if required not in idx:
            print(f"[FATAL] Kolom '{required}' tidak ditemukan di header tab {NETSALES_SHEET_NAME}.")
            sys.exit(1)

    key_to_rownum = {}
    for i, r in enumerate(existing):
        def cell(col):
            return r[idx[col]] if idx[col] < len(r) else ""
        key = make_key(cell("STORE"), cell("BULAN"), cell("TAHUN"), cell("MINGGU"))
        key_to_rownum[key] = i + 2  # +2: header + 1-indexed

    updated, inserted = 0, 0
    new_rows = []
    now_str = datetime.now(MAKASSAR_TZ).strftime("%Y-%m-%d %H:%M WITA")

    for rec in rows:
        key = make_key(rec["store"], rec["bulan"], rec["tahun"], rec["minggu"])
        row_num = key_to_rownum.get(key)

        row_values = [""] * len(headers)
        row_values[idx["STORE"]] = rec["store"]
        row_values[idx["BULAN"]] = rec["bulan"]
        row_values[idx["TAHUN"]] = rec["tahun"]
        row_values[idx["MINGGU"]] = rec["minggu"]
        row_values[idx["PERIODE"]] = rec["periode"]
        for cat in CATEGORY_COLUMNS:
            row_values[idx[cat]] = rec["breakdown"][cat]
        row_values[idx["NET SALES (TOTAL)"]] = rec["net_total"]
        row_values[idx["NET SALES (WO DEFECT)"]] = rec["net_wo_defect"]
        row_values[idx["LAST UPDATED"]] = now_str

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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    outlet_map, sa_info = require_env()
    config_ws, netsales_ws, synclog_ws = open_sheets(sa_info)

    stored_refresh_token = get_stored_refresh_token(config_ws)
    access_token = refresh_access_token(config_ws, stored_refresh_token)

    periods = periods_to_process()
    if not periods:
        print("[INFO] Tidak ada periode yang perlu diproses hari ini.")
        log_sync_run(synclog_ws, SYNC_MODE, 0, 0, "Tidak ada periode diproses")
        return

    print(f"[INFO] Mode: {SYNC_MODE}. Jumlah periode (bulan x minggu) diproses: {len(periods)}")

    rows = []
    success_outlets, failed_outlets = 0, 0

    for (year, month, week_num, start_d, end_d) in periods:
        bulan = MONTH_ID[month - 1]
        tahun = str(year)
        periode = periode_label(start_d, end_d, bulan)

        for outlet_id, store_name in outlet_map.items():
            item_sales = fetch_outlet_item_sales(outlet_id, access_token, start_d, end_d)
            if item_sales is None:
                failed_outlets += 1
                continue

            breakdown = compute_category_breakdown(item_sales)
            net_total = sum(breakdown.values())
            net_defect = breakdown[DEFECT_CATEGORY]
            net_wo_defect = net_total - net_defect

            rows.append({
                "store": store_name,
                "bulan": bulan,
                "tahun": tahun,
                "minggu": week_num,
                "periode": periode,
                "breakdown": breakdown,
                "net_total": net_total,
                "net_wo_defect": net_wo_defect,
            })
            success_outlets += 1
            print(f"  [{bulan} {tahun} - Minggu {week_num} ({periode})] {store_name} "
                  f"({outlet_id}): TOTAL={net_total:,.0f} DEFECT={net_defect:,.0f} "
                  f"WO_DEFECT={net_wo_defect:,.0f}")

    if not rows:
        print("[FATAL] Tidak ada data yang berhasil diambil dari outlet manapun.")
        log_sync_run(synclog_ws, SYNC_MODE, success_outlets, failed_outlets, "Semua fetch gagal")
        sys.exit(1)

    updated, inserted = upsert_netsales(netsales_ws, rows)
    note = f"Diperbarui: {updated}, baris baru: {inserted}"
    print(f"[OK] Sheet ter-update. {note}")
    log_sync_run(synclog_ws, SYNC_MODE, success_outlets, failed_outlets, note)


if __name__ == "__main__":
    main()
