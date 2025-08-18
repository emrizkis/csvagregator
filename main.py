import pandas as pd
import glob
import os
from datetime import datetime

# --- Konfigurasi ---
# Path folder input
folder_path = 'files/inputs/' 

# Path output diubah ke 'files/outputs/'
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
nama_file_output = f"files/outputs/{timestamp}_merge_file.csv"


def gabungkan_csv_hemat_memori(path, file_output):
    """
    Menggabungkan file-file CSV dengan skema kolom yang berbeda secara efisien.
    
    Args:
        path (str): Path ke folder yang berisi file CSV.
        file_output (str): Nama file CSV untuk menyimpan hasil gabungan.
    """
    try:
        # Cek apakah folder input ada
        if not os.path.isdir(path):
            print(f"⚠️  Error: Folder input '{path}' tidak ditemukan.")
            return

        semua_file = glob.glob(os.path.join(path, "*.csv"))

        if not semua_file:
            print(f"⚠️  Tidak ada file .csv yang ditemukan di dalam folder: '{path}'")
            return

        # Langkah 1: Kumpulkan semua header unik
        print("Membaca headers dari semua file...")
        semua_kolom = set()
        for f in semua_file:
            try:
                df_header = pd.read_csv(f, nrows=0) 
                semua_kolom.update(df_header.columns)
            except Exception as e:
                print(f"❌ Gagal membaca header dari {os.path.basename(f)}: {e}")
        
        kolom_final = sorted(list(semua_kolom))
        print(f"\n✅  Semua kolom unik ditemukan: {len(kolom_final)} kolom.")

        # Langkah 2: Buat folder output jika belum ada
        output_dir = os.path.dirname(file_output)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            print(f"Folder output '{output_dir}' telah disiapkan.")

        # Langkah 3: Buat file output dan tulis headernya
        pd.DataFrame(columns=kolom_final).to_csv(file_output, index=False, encoding='utf-8')
        
        # Langkah 4: Proses setiap file dan tambahkan ke file output
        print("\nMemulai proses penggabungan data...")
        total_baris = 0
        for f in semua_file:
            try:
                df = pd.read_csv(f)
                df_reindexed = df.reindex(columns=kolom_final)
                df_reindexed.to_csv(file_output, mode='a', header=False, index=False, encoding='utf-8')
                
                print(f"✅  Memproses dan menambahkan {len(df)} baris dari: {os.path.basename(f)}")
                total_baris += len(df)

            except Exception as e:
                print(f"❌ Gagal memproses file {os.path.basename(f)}: {e}")

        print(f"\n🎉  Sukses! Proses selesai.")
        print(f"Hasil disimpan di: '{file_output}'")
        print(f"Total baris digabungkan: {total_baris}")

    except Exception as e:
        print(f"\nTerjadi kesalahan fatal: {e}")

# --- Panggil Fungsi Utama ---
if __name__ == "__main__":
    gabungkan_csv_hemat_memori(folder_path, nama_file_output)