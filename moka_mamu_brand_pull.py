"""
MOKA MAMU BRAND PULL — script SEKALI JALAN (bukan cron), per BULAN, SEMUA
STORE, SEJAK AWAL PENCATATAN — khusus brand MAMU
File & workflow BARU. TIDAK mengubah/menyentuh moka_sheets_sync.py,
moka_alltime_category_pull.py, moka_item_level_pull.py, atau tab manapun di
spreadsheet dashboard utama (GOOGLE_SHEET_ID). Config/token tetap dibaca dari
sana (read-only), tapi semua tulisan pergi ke spreadsheet TUJUAN terpisah
(TARGET_SHEET_ID) -- jadi aman dijalankan berkali-kali tanpa risiko merusak
data yang sudah ada.

TUJUAN
------
Menarik histori penjualan ITEM-LEVEL brand MAMU saja, dari bulan awal
pencatatan (default 2021-01) sampai bulan berjalan, untuk SEMUA outlet di
MOKA_OUTLET_MAP. Hasilnya ditulis 1 TAB per STORE yang memang menjual brand
MAMU (store tanpa item MAMU tidak akan dapat tab -- tidak menambah tab
kosong).

Kenapa difilter di level ITEM (bukan cuma di level nama outlet):
Nama outlet ada yang sudah jelas menyebut brand (mis. "MAMU MAKASSAR"), tapi
ada juga kemungkinan 1 outlet menjual campuran TIGALAPANKAOS & MAMU dalam
satu mesin kasir. Supaya tidak salah asumsi soal skema penamaan outlet,
filter dilakukan per ITEM: kalau CATEGORY atau NAMA ITEM dari laporan Moka
mengandung kata kunci brand (default "MAMU", bisa diganti lewat input
workflow), item itu ikut ditarik -- apapun nama outlet-nya.

Kolom per tab store:
  BULAN | TAHUN | CATEGORY | ITEM | ITEMS SOLD | ITEMS REFUND | ITEM TOTAL |
  NET SALES | LAST UPDATED
  (ITEM TOTAL = ITEMS SOLD - ITEMS REFUND, sesuai diminta)
  Key unik per baris (dalam 1 tab store): BULAN + TAHUN + CATEGORY + ITEM

CATATAN SOAL FIELD REFUND
--------------------------
Skema response Moka /reports/item_sales tidak didokumentasikan secara publik
untuk field refund, jadi script ini mencoba beberapa nama field yang umum
dipakai (lihat REFUND_QTY_KEYS di bawah) -- persis seperti pola get_first()
yang sudah dipakai di moka_item_level_pull.py untuk field quantity. Kalau
setelah run pertama kolom "ITEMS REFUND" selalu 0 padahal Anda yakin ada
retur, cek log [DEBUG SAMPLE ITEM KEYS] di output workflow run -- itu akan
menampilkan nama-nama field asli yang dikirim Moka untuk 1 item contoh,
supaya nama field yang benar bisa ditambahkan ke REFUND_QTY_KEYS.

CARA PAKAI
----------
1. Buat 1 Google Sheet BARU (kosong) khusus rekap brand MAMU ini.
2. Share spreadsheet itu ke email service account yang sama dipakai sync
   yang lain (cek field "client_email" di GOOGLE_SERVICE_ACCOUNT_JSON).
3. Ambil Sheet ID dari URL spreadsheet baru itu.
4. Jalankan workflow "Moka MAMU Brand Pull (One-Time, Semua Store)" ->
   isi input "target_sheet_id" (wajib). Input lain opsional (ada default).

Env yang dipakai (sama seperti pull lain, tidak perlu secret baru):
  MOKA_CLIENT_ID, MOKA_CLIENT_SECRET, MOKA_OUTLET_MAP,
  GOOGLE_SERVICE_ACCOUNT_JSON  (sama seperti pull lain)
  GOOGLE_SHEET_ID              (spreadsheet dashboard utama -- sumber Config/token, read-only)
  MOKA_MAMU_TARGET_SHEET       (Sheet ID spreadsheet TUJUAN rekap MAMU, wajib)
  MOKA_MAMU_START              (bulan mulai, format YYYY-MM, default "2021-01")
  MOKA_MAMU_END                (bulan akhir, format YYYY-MM, default bulan berjalan)
  MOKA_MAMU_BRAND_FILTER       (kata kunci brand, default "MAMU")
"""

import os
import sys
import json
import calendar
from datetime import datetime, date, timezone, timedelta

import requests
import gspread
from google.oauth2.service_account import Credentials

MAKASSAR_TZ = timezone(timedelta(hours=8))

TOKEN_URL = "https://api.mokapos.com/oauth/token"
API_BASE = "https://api.mokapos.com"

SHEET_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CONFIG_SHEET_NAME = "Config"
REFRESH_TOKEN_CELL = "B1"

MOKA_CLIENT_ID = os.environ.get("MOKA_CLIENT_ID")
MOKA_CLIENT_SECRET = os.environ.get("MOKA_CLIENT_SECRET")
MOKA_OUTLET_MAP_RAW = os.environ.get("MOKA_OUTLET_MAP", "{}")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")  # dashboard utama -- read-only (Config/token)

TARGET_SHEET_ID = os.environ.get("MOKA_MAMU_TARGET_SHEET", "").strip()
MAMU_START_RAW = os.environ.get("MOKA_MAMU_START", "2021-01").strip()
MAMU_END_RAW = os.environ.get("MOKA_MAMU_END", "").strip()
BRAND_FILTER = os.environ.get("MOKA_MAMU_BRAND_FILTER", "MAMU").strip().upper()

MONTH_ID = [
    "JANUARI", "FEBRUARI", "MARET", "APRIL", "MEI", "JUNI",
    "JULI", "AGUSTUS", "SEPTEMBER", "OKTOBER", "NOVEMBER", "DESEMBER",
]

HEADER_ROW = [
    "BULAN", "TAHUN", "CATEGORY", "ITEM",
    "ITEMS SOLD", "ITEMS REFUND", "ITEM TOTAL", "NET SALES", "LAST UPDATED",
]

# Kandidat nama field qty terjual & qty refund di response Moka -- diambil
# yang pertama ketemu. "item_sold" & "item_refunded" adalah nama field asli
# dari dokumentasi resmi endpoint reports/item_sales (dikonfirmasi via
# GoBiz Developer Portal, integrator resmi Moka) -- taruh paling depan.
# Sisanya cadangan kalau skema akun ternyata beda.
SOLD_QTY_KEYS = ["item_sold", "quantity", "qty", "qty_sold", "item_quantity", "quantity_sold"]
REFUND_QTY_KEYS = [
    "item_refunded", "quantity_refund", "qty_refund", "refund_quantity", "refunded_quantity",
    "total_refund_quantity", "quantity_returned", "qty_return", "return_quantity",
    "quantity_void", "void_quantity",
]

MAX_TAB_NAME_LEN = 100  # batas Google Sheets untuk nama tab


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

    if not TARGET_SHEET_ID:
        print("[FATAL] Input 'target_sheet_id' wajib diisi -- Sheet ID spreadsheet "
              "TUJUAN rekap MAMU (bukan spreadsheet dashboard utama).")
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

    if not BRAND_FILTER:
        print("[FATAL] MOKA_MAMU_BRAND_FILTER tidak boleh kosong.")
        sys.exit(1)

    return outlet_map, sa_info


def open_config_ws(gc):
    try:
        sh = gc.open_by_key(GOOGLE_SHEET_ID)
    except gspread.exceptions.APIError as e:
        print(f"[FATAL] Tidak bisa buka spreadsheet dashboard utama (read-only): {e}")
        sys.exit(1)
    try:
        return sh.worksheet(CONFIG_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print(f"[FATAL] Tab '{CONFIG_SHEET_NAME}' tidak ditemukan di spreadsheet utama.")
        sys.exit(1)


def open_target_spreadsheet(gc, sa_info):
    try:
        return gc.open_by_key(TARGET_SHEET_ID)
    except gspread.exceptions.APIError as e:
        print(f"[FATAL] Tidak bisa buka spreadsheet tujuan (target_sheet_id): {e}")
        print("Pastikan spreadsheet BARU untuk rekap MAMU sudah di-share ke email "
              f"service account ({sa_info.get('client_email')}) sebagai Editor.")
        sys.exit(1)


def get_stored_refresh_token(config_ws):
    token = config_ws.acell(REFRESH_TOKEN_CELL).value
    if not token:
        print(f"[FATAL] Sel {CONFIG_SHEET_NAME}!{REFRESH_TOKEN_CELL} kosong.")
        sys.exit(1)
    return token.strip()


def save_refresh_token(config_ws, token):
    # Tidak dipakai di script ini -- token TIDAK ditulis balik ke dashboard
    # utama supaya script ini benar-benar read-only terhadap Config. Kalau
    # Moka merotasi refresh_token saat dipakai di sini, sync harian
    # (moka_sheets_sync.py) yang jalan berikutnya akan merotasinya sendiri.
    pass


def refresh_access_token(refresh_token):
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
    return resp.json().get("access_token")


# ---------------------------------------------------------------------------
def add_month(year, month, delta):
    idx = (year * 12 + (month - 1)) + delta
    return idx // 12, idx % 12 + 1


def months_to_process():
    now = datetime.now(MAKASSAR_TZ)

    try:
        sy, sm = (int(x) for x in MAMU_START_RAW.split("-"))
    except ValueError:
        print(f"[FATAL] MOKA_MAMU_START format salah: '{MAMU_START_RAW}' (harus YYYY-MM)")
        sys.exit(1)

    if MAMU_END_RAW:
        try:
            ey, em = (int(x) for x in MAMU_END_RAW.split("-"))
        except ValueError:
            print(f"[FATAL] MOKA_MAMU_END format salah: '{MAMU_END_RAW}' (harus YYYY-MM)")
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
        print(f"[WARN] Outlet {outlet_id} ({start}..{end}) gagal diambil: "
              f"{resp.status_code} {resp.text}")
        return None
    return resp.json().get("data", {}).get("item_sales", [])


def get_first(item, keys, default=0):
    for k in keys:
        if k in item and item[k] not in (None, ""):
            try:
                return float(item[k])
            except (TypeError, ValueError):
                continue
    return default


def is_mamu_item(item):
    category = str(item.get("category_name") or "").upper()
    name = str(get_first_str(item, ["item_name", "name", "product_name"], "")).upper()
    return BRAND_FILTER in category or BRAND_FILTER in name


def get_first_str(item, keys, default=""):
    for k in keys:
        if k in item and item[k] not in (None, ""):
            return item[k]
    return default


_debug_sample_printed = False


def maybe_print_debug_sample(item_sales):
    """Cetak SEKALI nama-nama field mentah dari 1 item Moka, supaya kalau
    ITEMS REFUND ternyata selalu 0, nama field aslinya bisa dicek di log."""
    global _debug_sample_printed
    if _debug_sample_printed or not item_sales:
        return
    sample = item_sales[0]
    print(f"[DEBUG SAMPLE ITEM KEYS] Contoh field mentah dari Moka (1 item): "
          f"{sorted(sample.keys())}")
    _debug_sample_printed = True


# ---------------------------------------------------------------------------
def sanitize_tab_name(name):
    # Google Sheets tidak boleh: [ ] * ? / \ : dan tidak boleh kosong/>100 char
    cleaned = "".join(c for c in str(name) if c not in "[]*?/\\:")
    cleaned = cleaned.strip() or "STORE"
    return cleaned[:MAX_TAB_NAME_LEN]


def open_or_create_store_ws(sh, tab_name):
    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=2000, cols=len(HEADER_ROW) + 2)
        ws.append_row(HEADER_ROW)
        print(f"  [OK] Tab baru dibuat: '{tab_name}'")
        return ws
    if not ws.row_values(1):
        ws.append_row(HEADER_ROW)
    return ws


def make_key(bulan, tahun, category, item_name):
    return f"{bulan.strip().upper()}|{tahun.strip()}|{category.strip().upper()}|{item_name.strip().upper()}"


def upsert_store_rows(ws, rows):
    """Upsert baris untuk 1 tab store. rows sudah final (per bulan+category+item)."""
    all_values = ws.get_all_values()
    if not all_values:
        ws.append_row(HEADER_ROW)
        headers = HEADER_ROW
        existing = []
    else:
        headers = [h.strip() for h in all_values[0]]
        existing = all_values[1:]

    idx = {h: i for i, h in enumerate(headers)}
    for required in HEADER_ROW:
        if required not in idx:
            print(f"[FATAL] Kolom '{required}' tidak ditemukan di tab '{ws.title}'.")
            sys.exit(1)

    key_to_rownum = {}
    for i, r in enumerate(existing):
        def cell(col):
            return r[idx[col]] if idx[col] < len(r) else ""
        key = make_key(cell("BULAN"), cell("TAHUN"), cell("CATEGORY"), cell("ITEM"))
        key_to_rownum[key] = i + 2

    update_batch = []
    new_rows = []
    now_str = datetime.now(MAKASSAR_TZ).strftime("%Y-%m-%d %H:%M WITA")
    last_col_letter = gspread.utils.rowcol_to_a1(1, len(headers)).rstrip("0123456789")

    for rec in rows:
        key = make_key(rec["bulan"], rec["tahun"], rec["category"], rec["item"])
        row_values = [""] * len(headers)
        row_values[idx["BULAN"]] = rec["bulan"]
        row_values[idx["TAHUN"]] = rec["tahun"]
        row_values[idx["CATEGORY"]] = rec["category"]
        row_values[idx["ITEM"]] = rec["item"]
        row_values[idx["ITEMS SOLD"]] = rec["item_sold"]
        row_values[idx["ITEMS REFUND"]] = rec["item_refund"]
        row_values[idx["ITEM TOTAL"]] = rec["item_total"]
        row_values[idx["NET SALES"]] = rec["net_sales"]
        row_values[idx["LAST UPDATED"]] = now_str

        row_num = key_to_rownum.get(key)
        if row_num:
            update_batch.append({
                "range": f"A{row_num}:{last_col_letter}{row_num}",
                "values": [row_values],
            })
        else:
            new_rows.append(row_values)

    if update_batch:
        ws.batch_update(update_batch, value_input_option="USER_ENTERED")
    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")

    return len(update_batch), len(new_rows)


# ---------------------------------------------------------------------------
def main():
    outlet_map, sa_info = require_env()

    creds = Credentials.from_service_account_info(sa_info, scopes=SHEET_SCOPES)
    gc = gspread.authorize(creds)

    config_ws = open_config_ws(gc)
    target_sh = open_target_spreadsheet(gc, sa_info)

    stored_refresh_token = get_stored_refresh_token(config_ws)
    access_token = refresh_access_token(stored_refresh_token)

    months = months_to_process()
    print(f"[INFO] Brand filter: '{BRAND_FILTER}' | {len(months)} bulan x "
          f"{len(outlet_map)} outlet ({months[0][0]}-{months[0][1]:02d} s.d. "
          f"{months[-1][0]}-{months[-1][1]:02d})")

    # rows_per_store[store_name][ (bulan,tahun,category,item) ] = {...}
    rows_per_store = {}
    fetched, skipped, mamu_items_found = 0, 0, 0

    for (year, month, start_d, end_d) in months:
        bulan = MONTH_ID[month - 1]
        tahun = str(year)

        for outlet_id, store_name in outlet_map.items():
            item_sales = fetch_outlet_item_sales(outlet_id, access_token, start_d, end_d)
            if item_sales is None:
                skipped += 1
                continue
            fetched += 1
            if not item_sales:
                continue

            maybe_print_debug_sample(item_sales)

            store_bucket = rows_per_store.setdefault(store_name, {})
            for item in item_sales:
                if not is_mamu_item(item):
                    continue

                category = str(item.get("category_name") or "UNCATEGORIZED").strip().upper()
                item_name = str(get_first_str(
                    item, ["item_name", "name", "product_name"], "UNKNOWN ITEM"
                )).strip().upper()

                item_sold = get_first(item, SOLD_QTY_KEYS, 0)
                item_refund = get_first(item, REFUND_QTY_KEYS, 0)
                net_sales = item.get("net_sales") or 0

                agg_key = (bulan, tahun, category, item_name)
                agg = store_bucket.setdefault(agg_key, {
                    "bulan": bulan, "tahun": tahun, "category": category, "item": item_name,
                    "item_sold": 0.0, "item_refund": 0.0, "net_sales": 0.0,
                })
                agg["item_sold"] += item_sold
                agg["item_refund"] += item_refund
                agg["net_sales"] += net_sales
                mamu_items_found += 1

        print(f"  [{bulan} {tahun}] selesai ({fetched} outlet-bulan tertarik, "
              f"{skipped} gagal, {mamu_items_found} baris item {BRAND_FILTER} sejauh ini)")

    if not rows_per_store:
        print(f"[FATAL] Tidak ada item brand '{BRAND_FILTER}' ditemukan sama sekali "
              f"di rentang {months[0][0]}-{months[0][1]:02d} s.d. {months[-1][0]}-{months[-1][1]:02d}.")
        sys.exit(1)

    total_updated, total_inserted = 0, 0
    for store_name, bucket in rows_per_store.items():
        rows = []
        for rec in bucket.values():
            rec["item_total"] = rec["item_sold"] - rec["item_refund"]
            rows.append(rec)

        tab_name = sanitize_tab_name(store_name)
        ws = open_or_create_store_ws(target_sh, tab_name)
        updated, inserted = upsert_store_rows(ws, rows)
        total_updated += updated
        total_inserted += inserted
        print(f"[OK] '{tab_name}': {len(rows)} baris ({updated} update, {inserted} baru)")

    print(f"[SELESAI] {len(rows_per_store)} store punya penjualan brand '{BRAND_FILTER}'. "
          f"Total baris diperbarui: {total_updated}, baris baru: {total_inserted}. "
          f"Outlet-bulan gagal ditarik: {skipped}")


if __name__ == "__main__":
    main()
