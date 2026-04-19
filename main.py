"""
main.py — Entry point utama PDDIKTI Scrapper
Jalankan file ini untuk menjalankan Tahap 1 dan Tahap 2 secara berurutan.

Atau jalankan masing-masing tahap secara terpisah:
  python tahap1.py   -> Scraping daftar Perguruan Tinggi
  python tahap2.py   -> Scraping detail Program Studi
"""

import sys
from tahap1 import run_tahap_1
from tahap2 import run_tahap_2


def main():
    print("=" * 55)
    print("  🎓 PDDIKTI SCRAPPER — Mulai Proses Lengkap")
    print("=" * 55 + "\n")

    # ---- TAHAP 1 ----
    print("📌 [TAHAP 1] Scraping daftar Perguruan Tinggi...\n")
    tahap1_ok = run_tahap_1()

    if not tahap1_ok:
        print("\n🛑 Tahap 1 gagal. Proses dihentikan.")
        sys.exit(1)

    # ---- TAHAP 2 ----
    print("\n" + "=" * 55)
    print("📌 [TAHAP 2] Scraping detail Program Studi...\n")
    tahap2_ok = run_tahap_2()

    if not tahap2_ok:
        print("\n🛑 Tahap 2 gagal atau tidak ada data prodi yang ditemukan.")
        sys.exit(1)

    print("\n🎉 Semua proses selesai dengan sukses!")


if __name__ == "__main__":
    main()
