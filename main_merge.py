import pandas as pd
import glob
import os
from datetime import datetime
import argparse # 1. Import library untuk command-line argument

# ==============================================================================
# KONFIGURASI PATH (Kunci Merge dipindah ke command-line)
# ==============================================================================
# Tentukan tipe merge: 'inner', 'left', 'right', 'outer'
MERGE_TYPE = 'inner'

# Konfigurasi path folder
path_source_a = 'files/inputs/source-a/'
path_source_b = 'files/inputs/source-b/'
path_temp = 'files/temp/'       # Folder untuk menyimpan hasil konsolidasi sementara
path_output = 'files/outputs/'
# ==============================================================================


def consolidate_csvs_in_folder(input_path, output_file):
    """Fungsi ini sama seperti sebelumnya, untuk mengkonsolidasi banyak CSV dalam satu folder."""
    try:
        if not os.path.isdir(input_path):
            print(f"⚠️  Folder input '{input_path}' tidak ditemukan.")
            return False

        all_files = glob.glob(os.path.join(input_path, "*.csv"))
        if not all_files:
            print(f"⚠️  Tidak ada file CSV di '{input_path}'.")
            return False

        all_columns = set()
        for f in all_files:
            try:
                df_header = pd.read_csv(f, nrows=0)
                all_columns.update(df_header.columns)
            except Exception:
                continue
        
        final_columns = sorted(list(all_columns))
        pd.DataFrame(columns=final_columns).to_csv(output_file, index=False, encoding='utf-8')

        for f in all_files:
            df = pd.read_csv(f)
            df_reindexed = df.reindex(columns=final_columns)
            df_reindexed.to_csv(output_file, mode='a', header=False, index=False, encoding='utf-8')
        
        print(f"✅  Konsolidasi '{input_path}' berhasil. Disimpan di '{output_file}'")
        return True
    except Exception as e:
        print(f"❌ Gagal saat konsolidasi '{input_path}': {e}")
        return False

def main(merge_key):
    """Fungsi utama untuk mengatur alur kerja konsolidasi dan merge."""
    print("--- Memulai Proses Penggabungan Data ---")
    
    # Pastikan folder temp dan output ada
    os.makedirs(path_temp, exist_ok=True)
    os.makedirs(path_output, exist_ok=True)

    # Definisikan nama file sementara
    temp_a_file = os.path.join(path_temp, 'consolidated_a.csv')
    temp_b_file = os.path.join(path_temp, 'consolidated_b.csv')

    # --- TAHAP 1: KONSOLIDASI ---
    print("\n--- Tahap 1: Konsolidasi Masing-Masing Sumber ---")
    success_a = consolidate_csvs_in_folder(path_source_a, temp_a_file)
    success_b = consolidate_csvs_in_folder(path_source_b, temp_b_file)

    if not (success_a and success_b):
        print("\n❌ Proses dihentikan karena salah satu tahap konsolidasi gagal.")
        return

    # --- TAHAP 2: PENGGABUNGAN (MERGE) ---
    print("\n--- Tahap 2: Penggabungan (Merge) Berdasarkan Kunci ---")
    try:
        df_a = pd.read_csv(temp_a_file)
        df_b = pd.read_csv(temp_b_file)

        # Validasi: Cek apakah kolom kunci ada di kedua file
        if merge_key not in df_a.columns or merge_key not in df_b.columns:
            print(f"❌ Error: Kolom kunci '{merge_key}' tidak ditemukan di salah satu file hasil konsolidasi.")
            print(f"Kolom di Source A: {list(df_a.columns)}")
            print(f"Kolom di Source B: {list(df_b.columns)}")
            return
            
        print(f"Menggabungkan data menggunakan kunci '{merge_key}' dengan metode '{MERGE_TYPE}'...")
        
        # Lakukan merge
        final_df = pd.merge(df_a, df_b, on=merge_key, how=MERGE_TYPE)

        # Simpan hasil akhir
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        final_output_file = os.path.join(path_output, f"{timestamp}_final_merge.csv")
        final_df.to_csv(final_output_file, index=False, encoding='utf-8')

        print("\n🎉  Sukses! Proses merge selesai.")
        print(f"Hasil disimpan di: '{final_output_file}'")
        print(f"Total baris hasil merge: {len(final_df)}")

    except Exception as e:
        print(f"❌ Gagal saat melakukan merge: {e}")


if __name__ == "__main__":
    # 2. Setup Argumen Parser
    parser = argparse.ArgumentParser(description="Tools untuk konsolidasi dan merge dua sumber data CSV.")
    
    # 3. Tambahkan Opsi untuk Kunci Merge
    parser.add_argument(
        '-k', '--key',
        dest='merge_key',
        default='id', # Nilai default jika tidak ada input
        help="Kolom yang akan digunakan sebagai kunci merge. Default: 'id'"
    )

    args = parser.parse_args()
    
    # 4. Jalankan fungsi main dengan kunci dari argumen
    main(args.merge_key)