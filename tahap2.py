import time
import sys
import os
import json
import threading
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# PENGATURAN TAHAP 2
# =========================
MAX_WORKERS = 10
TIMEOUT_SECONDS = 15
FILE_INPUT = "./pddikti_all_pt_raw.csv"
FILE_OUTPUT_CSV = "./pddikti_detail_prodi.csv"
FILE_OUTPUT_LOG = "./pddikti_detail_prodi_log.csv"
FILE_CHECKPOINT = "./pddikti_checkpoint_tahap2.json"

# Semester yang dicoba secara berurutan (fallback dari yang terbaru)
DEFAULT_SEMESTERS = ["20251", "20242", "20241"]

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Origin": "https://pddikti.kemdiktisaintek.go.id",
}

# Lock untuk penulisan file secara aman dari banyak thread
_write_lock = threading.Lock()


# =========================
# CHECKPOINT
# =========================

def load_checkpoint() -> set[str]:
    """
    Muat daftar id_sp yang sudah selesai diproses dari file checkpoint.
    Juga memeriksa file output CSV jika checkpoint tidak ada.
    """
    completed: set[str] = set()

    # Prioritas 1: file checkpoint JSON
    if os.path.exists(FILE_CHECKPOINT):
        try:
            with open(FILE_CHECKPOINT, "r", encoding="utf-8") as f:
                data = json.load(f)
                completed = set(str(x) for x in data.get("completed_ids", []))
            print(f"♻️  Checkpoint ditemukan: {len(completed)} kampus sudah selesai.")
        except (json.JSONDecodeError, KeyError):
            print("⚠️  File checkpoint rusak, akan dibuat ulang.")

    # Prioritas 2: baca dari CSV output yang sudah ada
    elif os.path.exists(FILE_OUTPUT_CSV):
        try:
            df_existing = pd.read_csv(FILE_OUTPUT_CSV, usecols=["id_sp"])
            completed = set(str(x) for x in df_existing["id_sp"].dropna().unique())
            print(f"♻️  Resume dari CSV output: {len(completed)} kampus sudah ada di file hasil.")
            # Buat checkpoint dari data CSV yang ada
            save_checkpoint(completed)
        except Exception:
            print("⚠️  Gagal membaca CSV output untuk resume, memulai dari awal.")

    return completed


def save_checkpoint(completed_ids: set[str]) -> None:
    """Simpan daftar id_sp yang sudah selesai ke file checkpoint JSON."""
    try:
        with open(FILE_CHECKPOINT, "w", encoding="utf-8") as f:
            json.dump({"completed_ids": list(completed_ids)}, f)
    except Exception as e:
        print(f"⚠️  Gagal menyimpan checkpoint: {e}")


def append_results_to_csv(data_prodi: list[dict], log_info: dict, write_header: bool) -> None:
    """
    Tulis satu batch hasil ke CSV secara thread-safe.
    Menggunakan append mode agar data sebelumnya tidak tertimpa.
    """
    with _write_lock:
        if data_prodi:
            df_batch = pd.DataFrame(data_prodi)
            # Pastikan urutan kolom konsisten
            front_cols = ["id_sp", "nama_pt", "semester_diambil"]
            other_cols = [c for c in df_batch.columns if c not in front_cols]
            df_batch = df_batch[front_cols + other_cols]
            df_batch.to_csv(
                FILE_OUTPUT_CSV,
                mode="a",
                header=write_header,
                index=False,
                encoding="utf-8-sig",
            )

        df_log = pd.DataFrame([log_info])
        df_log.to_csv(
            FILE_OUTPUT_LOG,
            mode="a",
            header=write_header,
            index=False,
            encoding="utf-8-sig",
        )


# =========================
# WORKER
# =========================

def fetch_prodi_worker(
    id_sp: str, nama_pt: str, session: requests.Session
) -> tuple[list, dict]:
    """
    Worker untuk mengambil data program studi sebuah kampus.
    Mencoba beberapa semester secara berurutan hingga mendapat data.
    """
    headers = HEADERS.copy()
    headers["Referer"] = f"https://pddikti.kemdiktisaintek.go.id/detail-pt/{id_sp}"

    hasil_prodi = []
    semester_valid = None
    status_detail = "KOSONG/GAGAL"

    for semester in DEFAULT_SEMESTERS:
        url = f"https://api-pddikti.kemdiktisaintek.go.id/pt/prodi/{id_sp}/{semester}"

        for attempt in range(1, 3):  # Coba maks 2x per semester
            try:
                r = session.get(url, headers=headers, timeout=TIMEOUT_SECONDS)

                if r.status_code == 429:  # Rate limited
                    time.sleep(2 * attempt)
                    continue

                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, list) and len(data) > 0:
                        hasil_prodi = data
                        semester_valid = semester
                        status_detail = "OK"
                        break  # Data ditemukan, stop retry

            except requests.exceptions.RequestException:
                time.sleep(1)
                continue

        if hasil_prodi:
            break  # Data ditemukan, stop coba semester lain

    # Tambahkan metadata ke setiap prodi
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
        "status_detail": status_detail,
    }

    return formatted_data, log_info


# =========================
# ENTRY POINT
# =========================

def run_tahap_2() -> bool:
    """
    Entry point Tahap 2 dengan dukungan resume.

    - id_sp yang sudah ada di checkpoint/CSV output akan dilewati (skip).
    - Hasil ditulis secara incremental (append) per kampus selesai.
    - Checkpoint diperbarui setiap SAVE_INTERVAL kampus selesai.
    - Return True jika berhasil, False jika gagal.
    """
    SAVE_INTERVAL = 25  # Simpan checkpoint setiap N kampus selesai

    print(f"📁 Membaca file {FILE_INPUT}...")

    if not os.path.exists(FILE_INPUT):
        print(f"❌ ERROR: File '{FILE_INPUT}' tidak ditemukan.")
        print("Pastikan Anda sudah menjalankan Tahap 1 terlebih dahulu di folder yang sama.")
        return False

    df_all = pd.read_csv(FILE_INPUT)

    if "id_sp" not in df_all.columns or "nama_pt" not in df_all.columns:
        print("❌ ERROR: Kolom 'id_sp' atau 'nama_pt' tidak ditemukan di file CSV.")
        return False

    df_source = df_all[["id_sp", "nama_pt"]].dropna().drop_duplicates()
    total_kampus_asal = len(df_source)

    # --- Load checkpoint ---
    completed_ids = load_checkpoint()

    # Filter hanya kampus yang belum diproses
    df_todo = df_source[~df_source["id_sp"].astype(str).isin(completed_ids)]
    total_todo = len(df_todo)
    total_skip = total_kampus_asal - total_todo

    if total_skip > 0:
        print(f"  ↩️  {total_skip} kampus dilewati (sudah selesai sebelumnya).")

    if total_todo == 0:
        print("🎉 Semua kampus sudah diproses sebelumnya. Tidak ada yang perlu dilakukan.")
        return True

    print(f"🎯 Sisa {total_todo} dari {total_kampus_asal} Perguruan Tinggi yang akan diproses.")
    print(f"🚀 Memulai dengan {MAX_WORKERS} workers...\n")

    # Header CSV hanya ditulis jika file belum ada (fresh start)
    write_header = not os.path.exists(FILE_OUTPUT_CSV)

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS
    )
    session.mount("https://", adapter)

    counter = 0
    success_count = 0
    newly_completed: set[str] = set()
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_prodi_worker, row.id_sp, row.nama_pt, session): (
                str(row.id_sp),
                row.nama_pt,
            )
            for row in df_todo.itertuples(index=False)
        }

        for future in as_completed(futures):
            id_sp_done, nama_kampus = futures[future]
            counter += 1

            try:
                data_prodi, info_log = future.result()

                # Tulis hasil langsung ke CSV (incremental)
                append_results_to_csv(data_prodi, info_log, write_header=write_header)
                write_header = False  # Header hanya ditulis sekali

                newly_completed.add(id_sp_done)
                if data_prodi:
                    success_count += 1

                # Simpan checkpoint secara berkala
                if len(newly_completed) % SAVE_INTERVAL == 0:
                    all_done = completed_ids | newly_completed
                    save_checkpoint(all_done)

                # Print progress
                if counter % 50 == 0 or counter == total_todo:
                    progress = (counter / total_todo) * 100
                    nama_display = (
                        nama_kampus[:30] + "..." if len(nama_kampus) > 30 else nama_kampus
                    )
                    print(
                        f"⏳ Progress: {counter}/{total_todo} ({progress:.1f}%) "
                        f"| Terakhir: {nama_display}"
                    )

            except Exception as e:
                print(f"❌ Error pada kampus {nama_kampus}: {e}")

    # Simpan checkpoint final
    final_completed = completed_ids | newly_completed
    save_checkpoint(final_completed)

    # Ringkasan akhir
    print("\n" + "=" * 55)
    duration = time.time() - start_time
    print(f"✅ Selesai memproses {counter} kampus dalam sesi ini ({success_count} berhasil dapat data).")
    print(f"📊 Total sudah diproses: {len(final_completed)}/{total_kampus_asal} kampus.")
    print(f"📄 Hasil disimpan di '{FILE_OUTPUT_CSV}'")
    print(f"📑 Log disimpan di '{FILE_OUTPUT_LOG}'")
    print(f"⏱️  Waktu eksekusi sesi ini: {duration / 60:.2f} menit")
    print("=" * 55)

    # Jika semua kampus sudah selesai, generate Excel final
    if len(final_completed) == total_kampus_asal and os.path.exists(FILE_OUTPUT_CSV):
        print("\n📦 Membuat file Excel final...")
        try:
            df_final = pd.read_csv(FILE_OUTPUT_CSV)
            df_final.to_excel("pddikti_detail_prodi.xlsx", index=False)
            print(f"✅ Excel final disimpan: {len(df_final)} baris Program Studi.")
        except Exception as e:
            print(f"⚠️  Gagal membuat Excel: {e}")

    return success_count > 0


if __name__ == "__main__":
    success = run_tahap_2()
    sys.exit(0 if success else 1)
