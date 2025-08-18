# Dynamic CSV Merger & Dashboard

## Deskripsi 📜

Proyek ini adalah seperangkat _tools_ Python untuk mengotomatisasi proses penggabungan (_merge_) data dari berbagai file CSV yang memiliki skema atau kolom berbeda. _Tool_ ini terdiri dari dua komponen utama:

1.  **Script Pemrosesan Data (`main_merge.py`)**: Sebuah _script command-line_ yang kuat untuk menggabungkan data dari dua sumber folder (`source-a` dan `source-b`). Script ini pertama-tama mengkonsolidasi semua file di dalam masing-masing sumber, lalu menggabungkan kedua hasil tersebut berdasarkan kolom kunci (seperti _foreign key_ di database).
2.  **Dashboard Interaktif (`dashboard.py`)**: Sebuah aplikasi web berbasis Streamlit yang secara otomatis menampilkan hasil dari script pemrosesan. _Dashboard_ ini memungkinkan pengguna untuk memilih laporan mana yang ingin dilihat, menampilkan ringkasan data, dan menyajikan visualisasi interaktif.

Proyek ini sangat ideal untuk kasus di mana Anda perlu menggabungkan data transaksional dengan data referensi, seperti menggabungkan data penjualan dengan data pembayaran berdasarkan ID transaksi.

---

## Fitur Utama ✨

- **Konsolidasi CSV Dinamis**: Secara otomatis menggabungkan banyak file CSV menjadi satu, bahkan jika setiap file memiliki set kolom yang berbeda.
- **Penggabungan Berbasis Kunci (Merge)**: Melakukan _merge_ (mirip `SQL JOIN`) antara dua set data berdasarkan kolom kunci yang dapat dikonfigurasi.
- **Opsi Command-Line Fleksibel**: Kunci untuk _merge_ dapat diatur melalui _flag_ saat menjalankan _script_ (misalnya `--key id_transaksi`), dengan nilai _default_ 'id'.
- **Output Berbasis Timestamp**: Setiap file hasil diberi nama dengan _timestamp_ (`YYYYMMDDHHIISS`), sehingga tidak ada data yang tertimpa dan riwayat pekerjaan tersimpan.
- **Dashboard Visual**: Laporan tidak hanya dalam bentuk tabel, tetapi juga dashboard web yang interaktif.
- **Pemilihan Laporan**: Pengguna dapat memilih laporan mana yang akan dianalisis melalui _dropdown menu_ di _dashboard_.
- **Hemat Memori**: Proses penggabungan dirancang untuk menangani file besar tanpa membebani RAM secara berlebihan.

---
