import time
import re
import sys
import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# PENGATURAN TAHAP 1
# =========================
MAX_WORKERS = 10
TIMEOUT_SECONDS = 10
VERBOSE = True

BASE_URL = "https://api-pddikti.kemdiktisaintek.go.id/v2/pt/search/filter"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Origin": "https://pddikti.kemdiktisaintek.go.id",
    "Referer": "https://pddikti.kemdiktisaintek.go.id/",
}

PROVINSI_LIST = [
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
    "Prov. Jawa Timur",
]


def log(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs)


def safe_filename(text: str) -> str:
    text = text.strip().lower().replace("prov. ", "").replace(" ", "_")
    return re.sub(r"[^a-z0-9_\.]", "", text)


def csv_path_for(provinsi: str) -> str:
    """Kembalikan path CSV output untuk satu provinsi."""
    return f"pddikti_{safe_filename(provinsi)}_raw.csv"


def check_api_status(session: requests.Session) -> bool:
    print("⚡ Mengecek kesehatan API...")
    try:
        r = session.get(BASE_URL, headers=HEADERS, params={"page": 1}, timeout=5)
        if r.status_code == 200:
            print("✅ API Aktif. Memulai akselerasi...\n")
            return True
        return False
    except Exception:
        return False


def fetch_page_worker(page: int, provinsi: str, session: requests.Session) -> list:
    """Worker untuk mengambil satu halaman data PT dari satu provinsi."""
    params = {
        "page": page,
        "akreditasi": "",
        "jenis": "",
        "provinsi": provinsi,
        "status": "",
    }

    for attempt in range(1, 4):
        try:
            r = session.get(BASE_URL, headers=HEADERS, params=params, timeout=TIMEOUT_SECONDS)
            if r.status_code == 200:
                return r.json().get("data") or []
            elif r.status_code == 429:  # Rate limited
                time.sleep(2 * attempt)
        except Exception:
            time.sleep(1)

    return []


def scrape_province_fast(session: requests.Session, provinsi: str) -> pd.DataFrame:
    """Scrape semua data PT dalam satu provinsi secara paralel."""

    # Ambil halaman 1 sekaligus dapatkan metadata (totalPages, totalItems)
    params_init = {
        "page": 1,
        "akreditasi": "",
        "jenis": "",
        "provinsi": provinsi,
        "status": "",
    }
    r_init = session.get(BASE_URL, headers=HEADERS, params=params_init, timeout=TIMEOUT_SECONDS)
    meta = r_init.json()

    total_pages = meta.get("totalPages", 0)
    total_items = meta.get("totalItems", 0)
    first_payload = meta.get("data") or []

    print(f"🚀 {provinsi}: Menarik {total_items} data dari {total_pages} halaman secara paralel...")

    all_rows = list(first_payload)

    # Tarik sisa halaman (2 ke atas) secara paralel
    if total_pages > 1:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_page = {
                executor.submit(fetch_page_worker, p, provinsi, session): p
                for p in range(2, total_pages + 1)
            }

            for future in as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    data = future.result()
                    all_rows.extend(data)
                    log(f"  ∟ Halaman {page_num} selesai ditarik.")
                except Exception as e:
                    print(f"  ❌ Halaman {page_num} gagal total: {e}")

    return pd.DataFrame(all_rows)


def get_completed_provinces() -> list[str]:
    """Kembalikan daftar provinsi yang sudah memiliki file CSV output."""
    done = []
    for prov in PROVINSI_LIST:
        if os.path.exists(csv_path_for(prov)):
            done.append(prov)
    return done


def rebuild_combined_csv(gabungan_list: list[pd.DataFrame]) -> pd.DataFrame | None:
    """Gabungkan semua DataFrame provinsi dan simpan ke file master."""
    if not gabungan_list:
        return None
    df_all = pd.concat(gabungan_list, ignore_index=True)
    df_all.to_csv("pddikti_all_pt_raw.csv", index=False, encoding="utf-8-sig")
    df_all.to_excel("pddikti_all_pt_raw.xlsx", index=False)
    return df_all


def run_tahap_1() -> bool:
    """
    Entry point Tahap 1 dengan dukungan resume.

    - Provinsi yang CSV-nya sudah ada akan dilewati (skip).
    - Di akhir, semua CSV provinsi (lama + baru) digabung ulang ke file master.
    - Return True jika berhasil, False jika gagal.
    """
    # --- Deteksi resume ---
    completed = get_completed_provinces()
    remaining = [p for p in PROVINSI_LIST if p not in completed]

    if completed:
        print(f"♻️  Resume terdeteksi: {len(completed)} provinsi sudah selesai, "
              f"{len(remaining)} provinsi akan dilanjutkan.\n")
        for p in completed:
            print(f"  ✅ Skip (sudah ada): {p}")
        print()
    else:
        print("🆕 Memulai dari awal...\n")

    if not remaining:
        print("🎉 Semua provinsi sudah selesai diproses sebelumnya!")
        # Tetap rebuild file master dari CSV yang ada
        all_dfs = [pd.read_csv(csv_path_for(p)) for p in PROVINSI_LIST if os.path.exists(csv_path_for(p))]
        df_all = rebuild_combined_csv(all_dfs)
        if df_all is not None:
            print(f"✅ File master diperbarui: {len(df_all)} data PT.")
        return True

    # --- Setup session ---
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS
    )
    session.mount("https://", adapter)

    if not check_api_status(session):
        print("❌ API tidak merespons. Coba lagi nanti.")
        return False

    # --- Scrape provinsi yang belum selesai ---
    for prov in remaining:
        start_time = time.time()
        try:
            df_prov = scrape_province_fast(session, prov)
        except Exception as e:
            print(f"❌ Gagal scrape {prov}: {e}")
            continue

        if not df_prov.empty:
            duration = time.time() - start_time
            print(f"✨ {prov} selesai dalam {duration:.2f} detik. ({len(df_prov)} PT)")
            # Simpan langsung → jadi checkpoint otomatis
            df_prov.to_csv(csv_path_for(prov), index=False, encoding="utf-8-sig")
        else:
            print(f"⚠️ {prov}: Tidak ada data yang ditemukan.")

    # --- Gabungkan semua CSV provinsi (lama + baru) ke file master ---
    print("\n🔗 Menggabungkan semua data provinsi...")
    all_dfs = [pd.read_csv(csv_path_for(p)) for p in PROVINSI_LIST if os.path.exists(csv_path_for(p))]

    if all_dfs:
        df_all = rebuild_combined_csv(all_dfs)
        print(f"\n✅ TAHAP 1 SELESAI: {len(df_all)} data PT terkumpul dari {len(all_dfs)} provinsi.")
        return True
    else:
        print("⚠️ Tidak ada data yang berhasil dikumpulkan.")
        return False


if __name__ == "__main__":
    success = run_tahap_1()
    sys.exit(0 if success else 1)
