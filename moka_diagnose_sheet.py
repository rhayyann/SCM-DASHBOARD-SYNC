"""
DIAGNOSA GOOGLE_SHEET_ID — script kecil SEKALI PAKAI, cuma untuk cari tahu
spreadsheet mana yang ditunjuk oleh secret GOOGLE_SHEET_ID (dashboard utama).

TIDAK menulis apapun ke spreadsheet manapun -- murni baca judul, link, dan
daftar nama tab, lalu print ke log workflow. Aman dijalankan kapan saja,
tidak berhubungan dengan pull data Moka manapun.

Kenapa dibutuhkan: banyak spreadsheet di Google Drive yang mirip-mirip
(Lose Sales, Critical Stock, dashboard utama, dll), dan GitHub Secrets tidak
bisa dibaca ulang nilainya setelah disimpan -- jadi cara paling pasti untuk
tahu spreadsheet mana yang dipakai adalah tanya langsung ke API pakai
kredensial yang sama dengan yang dipakai script-script sync.

Env yang dipakai: GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SHEET_ID (sama seperti
yang lain, tidak perlu secret baru).
"""

import os
import sys
import json

import gspread
from google.oauth2.service_account import Credentials

SHEET_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")


def main():
    if not GOOGLE_SERVICE_ACCOUNT_JSON or not GOOGLE_SHEET_ID:
        print("[FATAL] GOOGLE_SERVICE_ACCOUNT_JSON atau GOOGLE_SHEET_ID belum diset.")
        sys.exit(1)

    try:
        sa_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    except json.JSONDecodeError:
        print("[FATAL] GOOGLE_SERVICE_ACCOUNT_JSON bukan JSON valid.")
        sys.exit(1)

    creds = Credentials.from_service_account_info(sa_info, scopes=SHEET_SCOPES)
    gc = gspread.authorize(creds)

    print(f"[INFO] Service account: {sa_info.get('client_email')}")
    print(f"[INFO] GOOGLE_SHEET_ID (dari secret): {GOOGLE_SHEET_ID}")
    print("-" * 70)

    try:
        sh = gc.open_by_key(GOOGLE_SHEET_ID)
    except gspread.exceptions.APIError as e:
        print(f"[FATAL] Tidak bisa buka spreadsheet: {e}")
        sys.exit(1)

    print(f"JUDUL SPREADSHEET : {sh.title}")
    print(f"LINK              : {sh.url}")
    print("DAFTAR TAB        :")
    for ws in sh.worksheets():
        print(f"  - {ws.title}")

    # Cek khusus tab Config -- ini yang jadi sumber refresh_token
    try:
        config_ws = sh.worksheet("Config")
        b1 = config_ws.acell("B1").value
        masked = (b1[:6] + "..." + b1[-4:]) if b1 and len(b1) > 12 else "(kosong / terlalu pendek)"
        print("-" * 70)
        print(f"[OK] Tab 'Config' ditemukan. Isi B1 (disamarkan): {masked}")
    except gspread.exceptions.WorksheetNotFound:
        print("-" * 70)
        print("[WARN] Tab 'Config' TIDAK ditemukan di spreadsheet ini.")


if __name__ == "__main__":
    main()
