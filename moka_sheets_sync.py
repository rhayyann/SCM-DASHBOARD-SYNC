"""
MOKA NET SALES SYNC — versi Harian (v4)
Dipicu oleh cron-job.org via GitHub repository_dispatch, jam 07:00 WITA
(Asia/Makassar, UTC+8) setiap hari. Ada juga jadwal cadangan (GitHub Actions
schedule) kalau-kalau cron-job.org gagal trigger.

PERUBAHAN DARI VERSI SEBELUMNYA (v3 -> v4)
--------------------------------------------
1. Granularitas data diganti dari MINGGUAN menjadi HARIAN — 1 baris per
   STORE per TANGGAL (bukan per STORE per MINGGU lagi). Alasannya: supaya
   filter/agregasi BULANAN di dashboard bisa pakai KALENDER MURNI (Juli =
   tanggal 1-31 Juli, titik — tidak lagi "nyerempet" ke bulan tetangga),
   sekaligus tetap bisa hitung trend MINGGUAN (Senin-Minggu + aturan
   mayoritas hari / "Opsi B") langsung dari data harian ini, baik di
   pipeline (kolom MINGGU/PERIODE, sebagai referensi) maupun di dashboard.
2. Kolom BULAN & TAHUN sekarang murni KALENDER — diambil langsung dari
   TANGGAL baris itu sendiri, TIDAK lagi mengikuti aturan Opsi B. Kolom
   MINGGU & PERIODE tetap dihitung pakai Opsi B (Senin-Minggu, dipulangkan
   ke bulan mayoritas hari) — jadi murni informasi "hari ini termasuk
   minggu keberapa & rentang tanggal berapa", TIDAK dipakai lagi sebagai
   dasar pengelompokan bulanan.
   -> Konsekuensi: BULAN dan (bulan pemilik dari) MINGGU/PERIODE bisa beda
      untuk hari-hari di ujung bulan. Ini WAJAR & disengaja — mis. baris
      tanggal 1 Agustus akan punya BULAN=Agustus, tapi MINGGU/PERIODE-nya
      merujuk ke minggu "27 Juli - 2 Agustus" (yang pemiliknya Juli).
3. Kolom baru "TANGGAL" (tanggal spesifik baris itu, format ISO YYYY-MM-DD)
   sekarang jadi bagian resmi dari key unik baris: STORE + TANGGAL.
4. Mode "daily" sekarang cukup proses 2 hari: KEMARIN (status SELESAI,
   dianggap final) dan HARI INI (status SEDANG BERJALAN, buat jaga-jaga
   kalau ada yang trigger manual di siang/sore hari — datanya akan terus
   ke-update tiap kali script jalan sampai hari itu lewat).
5. Mode "backfill" sekarang menerima RENTANG TANGGAL eksplisit lewat env
   MOKA_BACKFILL_START & MOKA_BACKFILL_END (format "YYYY-MM-DD"), BUKAN lagi
   "YYYY-MM". Ini supaya backfill besar (misal 1 tahun penuh = ratusan hari
   x 12 outlet = ribuan API call ke Moka) bisa DIPECAH jadi beberapa run
   manual (mis. run 1: Jan-Mar, run 2: Apr-Jun, dst) — lebih aman dari sisi
   waktu proses & rate limit Moka.
6. Fetch ke Moka API sekarang PARALEL per outlet (pakai ThreadPoolExecutor,
   default 6 worker bersamaan) — karena sekarang ada JAUH lebih banyak
   panggilan API (1 hari x 1 outlet = 1 API call, vs dulu 1 minggu x 1
   outlet). Tanpa paralel, backfill 1 tahun bisa berjam-jam. Jumlah worker
   bisa diatur lewat env MOKA_FETCH_WORKERS kalau perlu diturunkan (misal
   kalau Moka mulai membatasi request beruntun).
7. Kalau tab "NetSales" yang lama (format bulanan ATAU mingguan — dua-duanya
   tidak punya kolom TANGGAL) terdeteksi, otomatis di-rename jadi
   "NetSales_Old_Weekly_Archive" (atau "_2", "_3", dst kalau nama itu sudah
   dipakai) dan tab "NetSales" baru dibuat dengan header harian yang benar.
8. Kolom "STATUS" tetap ada, artinya sekarang lebih sederhana:
     - "SEDANG BERJALAN" -> baris untuk HARI INI, masih bisa berubah kalau
       script dijalankan ulang di hari yang sama
     - "SELESAI"         -> hari itu sudah lewat, final, historical
9. Tiap kali script jalan, satu baris log ditulis ke tab "SyncLog".

Struktur spreadsheet (3 tab):
  - "Config"    -> sel B1 menyimpan refresh_token Moka
  - "NetSales"  -> header:
      STORE | BULAN | TAHUN | MINGGU | PERIODE | COMBED 24S | COMBED 30S |
      KIDS 24S | TUNIK 24S | PANJANG + RIB | REJECT | WANGKI MYNO |
      NET SALES (TOTAL) | NET SALES (WO DEFECT) | LAST UPDATED | TANGGAL |
      STATUS
    Key unik per baris: STORE + TANGGAL
  - "SyncLog"   -> header: WAKTU | MODE | SUKSES | GAGAL | CATATAN

Referensi API Moka: https://api.mokapos.com/docs
Kredensial dari environment variable (GitHub Secrets):
  MOKA_CLIENT_ID, MOKA_CLIENT_SECRET, MOKA_OUTLET_MAP,
  GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SHEET_ID
Opsional:
  MOKA_SYNC_MODE     ("daily" default, atau "backfill")
  MOKA_BACKFILL_START (format "YYYY-MM-DD", default "2026-01-01")
  MOKA_BACKFILL_END   (format "YYYY-MM-DD", default: kemarin)
  MOKA_FETCH_WORKERS  (jumlah thread paralel fetch Moka, default 6)
"""

import os
import sys
import json
import calendar
from datetime import datetime, date, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

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
NETSALES_ARCHIVE_NAME = "NetSales_Old_Weekly_Archive"
SYNCLOG_SHEET_NAME = "SyncLog"
REFRESH_TOKEN_CELL = "B1"

MOKA_CLIENT_ID = os.environ.get("MOKA_CLIENT_ID")
MOKA_CLIENT_SECRET = os.environ.get("MOKA_CLIENT_SECRET")
MOKA_OUTLET_MAP_RAW = os.environ.get("MOKA_OUTLET_MAP", "{}")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
SYNC_MODE = os.environ.get("MOKA_SYNC_MODE", "daily").strip().lower()
BACKFILL_START_RAW = os.environ.get("MOKA_BACKFILL_START", "").strip()
BACKFILL_END_RAW = os.environ.get("MOKA_BACKFILL_END", "").strip()
FETCH_WORKERS = int(os.environ.get("MOKA_FETCH_WORKERS", "6") or "6")

DEFAULT_BACKFILL_START = date(2026, 1, 1)

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
    + ["NET SALES (TOTAL)", "NET SALES (WO DEFECT)", "LAST UPDATED", "TANGGAL", "STATUS"]
)

# Nilai kolom STATUS
STATUS_ONGOING = "SEDANG BERJALAN"   # baris untuk HARI INI, masih bisa berubah
STATUS_FINAL   = "SELESAI"           # hari sudah lewat, final/historical

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


def _parse_iso_date(raw, label):
    """Parse string 'YYYY-MM-DD' -> date. Exit kalau formatnya salah."""
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        print(f"[FATAL] {label} format salah: '{raw}' (harus 'YYYY-MM-DD')")
        sys.exit(1)


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
    ensure_status_column(netsales_ws)
    synclog_ws = get_or_create_synclog_tab(sh)

    return config_ws, netsales_ws, synclog_ws


def get_or_migrate_netsales_tab(sh):
    """Ambil tab NetSales. Kalau formatnya masih format lama (bulanan ATAU
    mingguan), arsipkan tab lama itu dan buat tab NetSales baru dengan
    header harian yang benar — otomatis, tanpa perlu diedit manual.

    PENTING: deteksi "sudah format harian" TIDAK bisa cuma mengandalkan nama
    kolom "TANGGAL" ada di header — beberapa sheet lama ternyata sudah punya
    kolom "TANGGAL" peninggalan manual yang KOSONG (bukan buatan script).
    Jadi di sini kita cek juga ISI-nya: ambil beberapa baris data pertama,
    kalau kolom TANGGAL-nya kosong semua, berarti itu bukan format harian
    yang sebenarnya — tetap dianggap format lama dan diarsipkan."""
    try:
        ws = sh.worksheet(NETSALES_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=NETSALES_SHEET_NAME, rows=200, cols=len(HEADER_ROW) + 2)
        ws.append_row(HEADER_ROW)
        print(f"[OK] Tab '{NETSALES_SHEET_NAME}' baru dibuat dengan header harian.")
        return ws

    existing_header = ws.row_values(1)
    headers_upper = [h.strip().upper() for h in existing_header]
    is_new_format = False
    if existing_header and "TANGGAL" in headers_upper and "STATUS" in headers_upper:
        tanggal_idx = headers_upper.index("TANGGAL")
        sample_rows = ws.get_all_values()[1:6]  # 5 baris data pertama saja, cukup buat sampling
        has_real_dates = any(
            len(r) > tanggal_idx and r[tanggal_idx].strip()
            for r in sample_rows
        )
        is_new_format = has_real_dates

    if is_new_format:
        return ws  # sudah format harian beneran (kolom TANGGAL terisi), tidak perlu migrasi

    # Format lama terdeteksi (bulanan/mingguan, kolom TANGGAL kosong/tidak
    # ada, atau tab kosong) -> arsipkan
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
        print(f"[OK] Tab '{NETSALES_SHEET_NAME}' baru dibuat dengan header harian.")
        return new_ws
    else:
        ws.append_row(HEADER_ROW)
        return ws


def ensure_status_column(netsales_ws):
    """Tambah kolom 'STATUS' di ujung kanan tab NetSales kalau belum ada —
    non-destruktif, baris/data lama tidak disentuh, cuma nambah 1 kolom baru.
    Baris lama akan kosong di kolom STATUS sampai baris itu kena update lagi."""
    headers = netsales_ws.row_values(1)
    headers_upper = [h.strip().upper() for h in headers]
    if "STATUS" in headers_upper:
        return
    col = len(headers) + 1
    netsales_ws.update_cell(1, col, "STATUS")
    print(f"[OK] Kolom 'STATUS' ditambahkan ke tab {NETSALES_SHEET_NAME} "
          f"({gspread.utils.rowcol_to_a1(1, col)}).")


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
# Logika minggu — Senin s/d Minggu (kalender), "Opsi B":
# 1 minggu kalender selalu dicatat UTUH di 1 bulan saja, yaitu bulan yang
# memiliki HARI LEBIH BANYAK dari minggu tersebut (majority-day rule).
# Karena 1 minggu = 7 hari (ganjil), tidak akan pernah terjadi seri 50/50.
# ---------------------------------------------------------------------------

def week_start_monday(d):
    """Senin dari minggu yang memuat tanggal d."""
    return d - timedelta(days=d.weekday())  # Monday = 0


def week_owner_month(wstart, wend):
    """Tentukan (year, month) 'pemilik' minggu ini berdasarkan mayoritas hari.
    Kalau minggu itu tidak menyeberang bulan, langsung jelas bulannya."""
    if wstart.month == wend.month and wstart.year == wend.year:
        return wstart.year, wstart.month

    last_day_start_month = calendar.monthrange(wstart.year, wstart.month)[1]
    days_in_start_month = last_day_start_month - wstart.day + 1
    days_in_end_month = wend.day  # end bulan berikutnya dimulai dari tanggal 1

    if days_in_start_month >= days_in_end_month:
        return wstart.year, wstart.month
    return wend.year, wend.month


def weeks_for_month(year, month):
    """Kembalikan list (minggu_ke, tgl_mulai_senin, tgl_akhir_minggu) — urut
    kronologis — untuk semua minggu kalender (Senin-Minggu) yang PEMILIKNYA
    adalah (year, month) menurut aturan mayoritas hari di atas. Rentang
    tanggalnya sendiri bisa menyeberang ke bulan tetangga (itu wajar & memang
    disengaja — datanya tetap utuh 7 hari, cuma "dipulangkan" ke 1 bulan)."""
    first_of_month = date(year, month, 1)
    last_of_month = date(year, month, calendar.monthrange(year, month)[1])

    scan_d = week_start_monday(first_of_month) - timedelta(days=7)  # buffer
    scan_end = last_of_month + timedelta(days=7)

    owned = []
    d = scan_d
    while d <= scan_end:
        wstart = d
        wend = d + timedelta(days=6)
        oy, om = week_owner_month(wstart, wend)
        if (oy, om) == (year, month):
            owned.append((wstart, wend))
        d += timedelta(days=7)

    owned.sort(key=lambda w: w[0])
    return [(i + 1, ws, we) for i, (ws, we) in enumerate(owned)]


def add_month(year, month, delta):
    idx = (year * 12 + (month - 1)) + delta
    return idx // 12, idx % 12 + 1


def week_info_for_day(d):
    """Untuk 1 tanggal, kembalikan info minggu Senin-Minggu yang memuatnya
    (dipakai untuk mengisi kolom MINGGU & PERIODE -- REFERENSI SAJA, bukan
    dasar pengelompokan bulanan lagi). Return (week_num, wstart, wend,
    owner_year, owner_month) -- week_num adalah nomor urut minggu itu DALAM
    bulan pemiliknya (owner_year, owner_month), yang bisa beda dari bulan
    kalender tanggal d sendiri kalau d ada di ujung bulan."""
    wstart = week_start_monday(d)
    wend = wstart + timedelta(days=6)
    owner_y, owner_m = week_owner_month(wstart, wend)
    for week_num, ws, we in weeks_for_month(owner_y, owner_m):
        if ws == wstart:
            return week_num, ws, we, owner_y, owner_m
    # Fallback, seharusnya tidak pernah kejadian kalau weeks_for_month benar
    return 1, wstart, wend, owner_y, owner_m


def days_to_process():
    """Kembalikan list (tanggal, status) yang perlu diambil datanya,
    tergantung mode ('daily' vs 'backfill'). status: "SEDANG BERJALAN" untuk
    HARI INI (masih bisa berubah kalau di-run ulang hari yang sama), atau
    "SELESAI" untuk hari yang sudah lewat (final/historical)."""
    now = datetime.now(MAKASSAR_TZ)
    today = now.date()
    days = []

    if SYNC_MODE == "backfill":
        start_d = _parse_iso_date(BACKFILL_START_RAW, "MOKA_BACKFILL_START") \
            if BACKFILL_START_RAW else DEFAULT_BACKFILL_START
        end_d = _parse_iso_date(BACKFILL_END_RAW, "MOKA_BACKFILL_END") \
            if BACKFILL_END_RAW else (today - timedelta(days=1))
        end_d = min(end_d, today)  # jaga-jaga jangan sampai narik "masa depan"

        d = start_d
        while d <= end_d:
            status = STATUS_FINAL if d < today else STATUS_ONGOING
            days.append((d, status))
            d += timedelta(days=1)

    else:  # daily -- cukup kemarin (final) + hari ini (masih berjalan)
        yesterday = today - timedelta(days=1)
        days.append((yesterday, STATUS_FINAL))
        days.append((today, STATUS_ONGOING))

    return days


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

def make_key(store, tanggal_iso):
    return f"{str(store).strip().upper()}|{str(tanggal_iso).strip()}"


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
        + ["NET SALES (TOTAL)", "NET SALES (WO DEFECT)", "LAST UPDATED", "TANGGAL", "STATUS"]
    )
    for required in required_cols:
        if required not in idx:
            print(f"[FATAL] Kolom '{required}' tidak ditemukan di header tab {NETSALES_SHEET_NAME}.")
            sys.exit(1)

    key_to_rownum = {}
    for i, r in enumerate(existing):
        def cell(col):
            return r[idx[col]] if idx[col] < len(r) else ""
        key = make_key(cell("STORE"), cell("TANGGAL"))
        key_to_rownum[key] = i + 2  # +2: header + 1-indexed

    updated, inserted = 0, 0
    new_rows = []
    update_batch = []  # kumpulan {'range':..., 'values':[[...]]} — dikirim sekaligus, BUKAN 1 API call per baris
    now_str = datetime.now(MAKASSAR_TZ).strftime("%Y-%m-%d %H:%M WITA")
    last_col_letter = gspread.utils.rowcol_to_a1(1, len(headers)).rstrip("0123456789")

    for rec in rows:
        key = make_key(rec["store"], rec["tanggal_iso"])
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
        row_values[idx["TANGGAL"]] = rec["tanggal_iso"]
        row_values[idx["STATUS"]] = rec["status"]

        if row_num:
            update_batch.append({
                "range": f"A{row_num}:{last_col_letter}{row_num}",
                "values": [row_values],
            })
            updated += 1
        else:
            new_rows.append(row_values)
            inserted += 1

    # ── Kirim semua UPDATE dalam batch (bukan 1 API call per baris) ──
    # Google Sheets punya limit "write requests per menit per user"; ratusan
    # baris x 1 call masing-masing gampang kena 429. batch_update() menggabung
    # banyak range jadi 1 (atau sedikit) API call. Dipecah per chunk + retry
    # kalau tetap kena rate-limit (defensif untuk backfill besar).
    if update_batch:
        _batch_update_with_retry(netsales_ws, update_batch)

    # ── Baris baru: append_rows() sudah otomatis 1 API call untuk semua baris ──
    if new_rows:
        _retry_on_ratelimit(lambda: netsales_ws.append_rows(new_rows))

    return updated, inserted


def _retry_on_ratelimit(fn, max_retries=5, base_delay=20):
    """Jalankan fn(); kalau kena 429 (rate limit Sheets API), tunggu lalu
    coba lagi dengan backoff. Dipakai untuk membungkus semua panggilan tulis
    ke Sheets yang tidak bisa dihindari jadi 1 batch tunggal."""
    import time
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except gspread.exceptions.APIError as e:
            is_rate_limit = "429" in str(e) or "Quota exceeded" in str(e)
            if not is_rate_limit or attempt == max_retries:
                raise
            delay = base_delay * attempt
            print(f"[WARN] Kena rate limit Sheets API (percobaan {attempt}/{max_retries}), "
                  f"tunggu {delay}s lalu coba lagi...")
            time.sleep(delay)


def _batch_update_with_retry(netsales_ws, update_batch, chunk_size=300):
    """Kirim update_batch lewat batch_update(), dipecah per chunk_size range
    supaya payload tidak terlalu besar dalam 1 request. Tiap chunk dibungkus
    retry-on-429, dengan jeda singkat antar chunk untuk jaga-jaga."""
    import time
    total = len(update_batch)
    for start in range(0, total, chunk_size):
        chunk = update_batch[start:start + chunk_size]
        _retry_on_ratelimit(lambda c=chunk: netsales_ws.batch_update(c, value_input_option="USER_ENTERED"))
        print(f"[OK] Batch update terkirim: {min(start + chunk_size, total)}/{total} baris.")
        if start + chunk_size < total:
            time.sleep(2)  # jeda singkat antar chunk, jaga-jaga quota


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _fetch_one(outlet_id, store_name, access_token, day, status):
    """1 unit kerja: fetch item sales utk 1 outlet, 1 hari. Dipanggil paralel
    lewat ThreadPoolExecutor -- HTTP request murni, aman dijalankan di thread
    terpisah (tidak ada shared mutable state di sini)."""
    item_sales = fetch_outlet_item_sales(outlet_id, access_token, day, day)
    return outlet_id, store_name, day, status, item_sales


def main():
    outlet_map, sa_info = require_env()
    config_ws, netsales_ws, synclog_ws = open_sheets(sa_info)

    stored_refresh_token = get_stored_refresh_token(config_ws)
    access_token = refresh_access_token(config_ws, stored_refresh_token)

    days = days_to_process()
    if not days:
        print("[INFO] Tidak ada tanggal yang perlu diproses hari ini.")
        log_sync_run(synclog_ws, SYNC_MODE, 0, 0, "Tidak ada tanggal diproses")
        return

    total_tasks = len(days) * len(outlet_map)
    print(f"[INFO] Mode: {SYNC_MODE}. Jumlah hari: {len(days)} "
          f"({days[0][0]} s/d {days[-1][0]}), outlet: {len(outlet_map)}, "
          f"total fetch: {total_tasks} (paralel {FETCH_WORKERS} worker).")

    rows = []
    success_outlets, failed_outlets = 0, 0
    done_count = 0

    # -- Fetch ke Moka API paralel per (hari x outlet) -- jauh lebih banyak
    # panggilan dibanding versi mingguan, jadi wajib paralel biar backfill
    # besar tidak berjam-jam / kena timeout GitHub Actions. --
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as executor:
        futures = [
            executor.submit(_fetch_one, outlet_id, store_name, access_token, day, status)
            for (day, status) in days
            for outlet_id, store_name in outlet_map.items()
        ]
        for future in as_completed(futures):
            outlet_id, store_name, day, status, item_sales = future.result()
            done_count += 1
            if item_sales is None:
                failed_outlets += 1
                continue

            breakdown = compute_category_breakdown(item_sales)
            net_total = sum(breakdown.values())
            net_defect = breakdown[DEFECT_CATEGORY]
            net_wo_defect = net_total - net_defect

            bulan = MONTH_ID[day.month - 1]  # BULAN sekarang kalender murni
            tahun = str(day.year)
            week_num, wstart, wend, owner_y, owner_m = week_info_for_day(day)
            periode = periode_label(wstart, wend, MONTH_ID[owner_m - 1])
            tanggal_iso = day.isoformat()

            rows.append({
                "store": store_name,
                "bulan": bulan,
                "tahun": tahun,
                "minggu": week_num,
                "periode": periode,
                "tanggal_iso": tanggal_iso,
                "breakdown": breakdown,
                "net_total": net_total,
                "net_wo_defect": net_wo_defect,
                "status": status,
            })
            success_outlets += 1
            if done_count % 25 == 0 or done_count == total_tasks:
                print(f"  [{done_count}/{total_tasks}] {tanggal_iso} {store_name}: "
                      f"TOTAL={net_total:,.0f} DEFECT={net_defect:,.0f} "
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
