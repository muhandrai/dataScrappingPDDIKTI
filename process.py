import time
import requests
import pandas as pd
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Pengaturan Performa
MAX_WORKERS = 10
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

import time
import requests
import pandas as pd
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# PENGATURAN TAHAP 2
# =========================
MAX_WORKERS = 10
TIMEOUT_SECONDS = 15
FILE_INPUT = "./pddikti_all_pt_raw.csv"


DEFAULT_SEMESTERS = ["20251"]

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Origin": "https://pddikti.kemdiktisaintek.go.id",
}

def fetch_prodi_worker(id_sp, nama_pt, session):
    """
    Worker untuk mencoba mengambil data prodi sebuah kampus.
    Akan mencoba beberapa semester secara berurutan hingga mendapat data.
    """
    headers = HEADERS.copy()
    headers["Referer"] = f"https://pddikti.kemdiktisaintek.go.id/detail-pt/{id_sp}"

    hasil_prodi = []
    semester_valid = None
    status_detail = "KOSONG/GAGAL"

    for semester in DEFAULT_SEMESTERS:
        url = f"https://api-pddikti.kemdiktisaintek.go.id/pt/prodi/{id_sp}/{semester}"

        # Coba maksimal 2 kali per semester jika terjadi error jaringan (bukan error 404/kosong)
        for attempt in range(1, 3):
            try:
                r = session.get(url, headers=headers, timeout=TIMEOUT_SECONDS)

                # Jika terlalu banyak request (Rate Limit)
                if r.status_code == 429:
                    time.sleep(2 * attempt)
                    continue

                if r.status_code == 200:
                    data = r.json()
                    # Pastikan data yang kembali adalah list dan tidak kosong
                    if isinstance(data, list) and len(data) > 0:
                        hasil_prodi = data
                        semester_valid = semester
                        status_detail = "OK"
                        break # Berhenti mencari di semester sebelumnya karena sudah dapat

            except requests.exceptions.RequestException:
                time.sleep(1) # Jeda sejenak sebelum retry
                continue

        # Jika hasil_prodi sudah terisi, keluar dari loop semester
        if hasil_prodi:
            break

    # Format data untuk dikembalikan
    formatted_data = []
    for prodi in hasil_prodi:
        prodi["id_sp"] = id_sp
        prodi["nama_pt"] = nama_pt
        prodi["semester_diambil"] = semester_valid
        formatted_data.append(prodi)

    log_info = {
        "id_sp": id_sp,
        "nama_pt": nama_pt,
        "semester_valid": semester_valid,
        "jumlah_prodi": len(formatted_data),
        "status_detail": status_detail
    }

    return formatted_data, log_info

def run_tahap_2():
    print(f"📁 Membaca file {FILE_INPUT}...")

    if not os.path.exists(FILE_INPUT):
        print(f"❌ ERROR: File '{FILE_INPUT}' tidak ditemukan.")
        print("Pastikan Anda sudah menjalankan skrip Tahap 1 terlebih dahulu di folder yang sama.")
        sys.exit()

    # Baca data Tahap 1
    df_all = pd.read_csv(FILE_INPUT)

    # Ambil kolom ID dan Nama Kampus, buang data ganda
    if 'id_sp' not in df_all.columns or 'nama_pt' not in df_all.columns:
        print("❌ ERROR: Kolom 'id_sp' atau 'nama_pt' tidak ditemukan di file CSV.")
        sys.exit()

    df_source = df_all[['id_sp', 'nama_pt']].dropna().drop_duplicates()
    total_kampus = len(df_source)

    print(f"🎯 Ditemukan {total_kampus} Perguruan Tinggi unik.")
    print(f"🚀 Memulai pengambilan data Program Studi dengan {MAX_WORKERS} workers...\n")

    # Siapkan Session
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount('https://', adapter)

    detail_list = []
    log_list = []

    start_time = time.time()
    counter = 0

    # Eksekusi Multithreading
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Daftarkan semua tugas
        futures = {
            executor.submit(fetch_prodi_worker, row.id_sp, row.nama_pt, session): row.nama_pt
            for row in df_source.itertuples(index=False)
        }

        # Proses hasil saat ada tugas yang selesai
        for future in as_completed(futures):
            nama_kampus = futures[future]
            counter += 1
            try:
                data_prodi, info_log = future.result()

                if data_prodi:
                    detail_list.extend(data_prodi)
                log_list.append(info_log)

                # Print progress sederhana
                if counter % 50 == 0 or counter == total_kampus:
                    progress = (counter / total_kampus) * 100
                    print(f"⏳ Progress: {counter}/{total_kampus} ({progress:.1f}%) | Terakhir diproses: {nama_kampus[:30]}...")

            except Exception as e:
                print(f"❌ Error pada kampus {nama_kampus}: {e}")

    # Menggabungkan hasil
    print("\n" + "="*50)
    print("Menyimpan hasil Tahap 2...")

    if detail_list:
        df_detail = pd.DataFrame(detail_list)

        # Merapikan urutan kolom (id_sp dan nama_pt di depan)
        front_cols = ['id_sp', 'nama_pt', 'semester_diambil']
        other_cols = [c for c in df_detail.columns if c not in front_cols]
        df_detail = df_detail[front_cols + other_cols]

        df_detail.to_csv("pddikti_detail_prodi.csv", index=False, encoding="utf-8-sig")
        df_detail.to_excel("pddikti_detail_prodi.xlsx", index=False)
        print(f"✅ BERHASIL: {len(df_detail)} Program Studi disimpan ke 'pddikti_detail_prodi.csv'")
    else:
        print("⚠️ Peringatan: Tidak ada data program studi yang berhasil diambil.")

    df_log = pd.DataFrame(log_list)
    df_log.to_csv("pddikti_detail_prodi_log.csv", index=False, encoding="utf-8-sig")
    print(f"📑 Log scraping disimpan ke 'pddikti_detail_prodi_log.csv'")

    duration = time.time() - start_time
    print(f"⏱️ Waktu total eksekusi Tahap 2: {duration/60:.2f} menit")
    print("="*50)

if __name__ == "__main__":
    run_tahap_2()