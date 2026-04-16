import time
import requests
import pandas as pd
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Pengaturan Performa
MAX_WORKERS = 10  # Jumlah thread (halaman yang ditarik sekaligus)
TIMEOUT_SECONDS = 10
VERBOSE = True

def log(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs)

BASE_URL = "https://api-pddikti.kemdiktisaintek.go.id/v2/pt/search/filter"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Origin": "https://pddikti.kemdiktisaintek.go.id",
    "Referer": "https://pddikti.kemdiktisaintek.go.id/",
}

provinsi_list = [
    # --- PULAU SUMATERA ---
    "Prov. Aceh",
    "Prov. Sumatera Utara",
    "Prov. Sumatera Barat",
    "Prov. Riau",
    "Prov. Kepulauan Riau",
    "Prov. Jambi",
    "Prov. Sumatera Selatan",
    "Prov. Kepulauan Bangka Belitung",
    "Prov. Bengkulu",
    "Prov. Lampung",

    # --- PULAU JAWA ---
    "Prov. Banten",
    "Prov. Jawa Barat",
    "Prov. Jawa Tengah",
    "Prov. D.I. Yogyakarta",
    "Prov. Jawa Timur"
]

def safe_filename(text: str) -> str:
    text = text.strip().lower().replace("prov. ", "").replace(" ", "_")
    return re.sub(r"[^a-z0-9_\.]", "", text)

def check_api_status(session) -> bool:
    print("⚡ Mengecek kesehatan API...")
    try:
        r = session.get(BASE_URL, headers=HEADERS, params={"page": 1}, timeout=5)
        if r.status_code == 200:
            print("✅ API Aktif. Memulai akselerasi...\n")
            return True
        return False
    except:
        return False

def fetch_page_worker(page, provinsi, session):
    """Fungsi pekerja untuk mengambil satu halaman spesifik."""
    params = {
        "page": page,
        "akreditasi": "",
        "jenis": "",
        "provinsi": provinsi,
        "status": "",
    }
    
    # Retry loop lokal untuk tiap worker
    for attempt in range(1, 4):
        try:
            r = session.get(BASE_URL, headers=HEADERS, params=params, timeout=TIMEOUT_SECONDS)
            if r.status_code == 200:
                data = r.json().get("data") or []
                return data
            elif r.status_code == 429: # Rate limited
                time.sleep(2 * attempt)
        except Exception:
            pass
    return []

def scrape_province_fast(session, provinsi):
    # 1. Ambil info awal (Total Halaman)
    first_payload = fetch_page_worker(1, provinsi, session)
    
    # Hitung total halaman dari request manual pertama
    # Kita perlu hit halaman 1 sekali lagi untuk dapat totalPages
    r_init = session.get(BASE_URL, headers=HEADERS, params={"page": 1, "provinsi": provinsi}, timeout=TIMEOUT_SECONDS)
    meta = r_init.json()
    total_pages = meta.get("totalPages", 0)
    total_items = meta.get("totalItems", 0)
    
    print(f"🚀 {provinsi}: Menarik {total_items} data dari {total_pages} halaman secara paralel...")
    
    all_rows = []
    all_rows.extend(first_payload)

    # 2. Tarik sisa halaman secara paralel (Multithreading)
    if total_pages > 1:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Daftarkan semua tugas dari halaman 2 ke atas
            future_to_page = {executor.submit(fetch_page_worker, p, provinsi, session): p for p in range(2, total_pages + 1)}
            
            for future in as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    data = future.result()
                    all_rows.extend(data)
                    if VERBOSE:
                        print(f"  ∟ Halaman {page_num} selesai ditarik.")
                except Exception as e:
                    print(f"  ❌ Halaman {page_num} gagal total: {e}")

    return pd.DataFrame(all_rows)

if __name__ == "__main__":
    main_session = requests.Session()
    # Mengatur adapter untuk koneksi pool yang lebih besar
    adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    main_session.mount('https://', adapter)

    if not check_api_status(main_session):
        print("❌ API tidak merespons. Coba lagi nanti.")
        sys.exit()

    gabungan_list = []
    for prov in provinsi_list:
        start_time = time.time()
        df_prov = scrape_province_fast(main_session, prov)
        
        if not df_prov.empty:
            duration = time.time() - start_time
            print(f"✨ Selesai dalam {duration:.2f} detik.")
            
            gabungan_list.append(df_prov)
            df_prov.to_csv(f"pddikti_{safe_filename(prov)}_raw.csv", index=False, encoding="utf-8-sig")

    if gabungan_list:
        df_all = pd.concat(gabungan_list, ignore_index=True)
        df_all.to_csv("pddikti_all_pt_raw.csv", index=False, encoding="utf-8-sig")
        df_all.to_excel("pddikti_all_pt_raw.xlsx", index=False)
        print(f"\n✅ BERHASIL: {len(df_all)} data terkumpul.")