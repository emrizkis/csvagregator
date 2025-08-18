import streamlit as st
import pandas as pd
import plotly.express as px
import glob
import os

# Konfigurasi Halaman Dashboard
st.set_page_config(
    page_title="Dashboard Laporan Merge",
    page_icon="📊",
    layout="wide"
)

# Judul Dashboard
st.title("📊 Dashboard Laporan Hasil Merge")

# --- PEMILIHAN FILE ---
st.markdown("### Pilih Laporan untuk Ditampilkan")

# Fungsi untuk mendapatkan daftar semua file laporan
def get_report_files(folder_path):
    """Mendapatkan daftar file CSV di folder, diurutkan dari yang terbaru."""
    try:
        list_of_files = glob.glob(os.path.join(folder_path, '*.csv'))
        if not list_of_files:
            return []
        # Urutkan file berdasarkan waktu modifikasi (terbaru di atas)
        list_of_files.sort(key=os.path.getctime, reverse=True)
        return list_of_files
    except Exception:
        return []

# Fungsi untuk memuat data dari path file yang spesifik
@st.cache_data
def load_data(file_path):
    """Memuat data dari file path yang dipilih."""
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        st.error(f"Gagal memuat file {os.path.basename(file_path)}: {e}")
        return None

# Dapatkan semua file laporan
report_files = get_report_files('files/outputs/')

# Cek jika ada file laporan
if report_files:
    # Buat dropdown menu dengan nama file (tanpa path) sebagai pilihan
    # `format_func` digunakan untuk menampilkan nama file yang lebih ramah
    selected_file = st.selectbox(
        "Pilih file laporan:",
        options=report_files,
        format_func=lambda x: os.path.basename(x)
    )

    # Muat data dari file yang dipilih
    df = load_data(selected_file)

    if df is not None:
        st.success(f"Menampilkan data dari: **{os.path.basename(selected_file)}**")

        # --- Tampilkan Metrik Utama ---
        st.markdown("## Ringkasan Data")
        col1, col2, col3 = st.columns(3)
        col1.metric("Jumlah Baris", f"{df.shape[0]:,}")
        col2.metric("Jumlah Kolom", f"{df.shape[1]:,}")
        missing_cells = df.isnull().sum().sum()
        col3.metric("Sel Kosong (NaN)", f"{missing_cells:,}")

        # --- Tampilkan Data Mentah ---
        with st.expander("Lihat Data Mentah (Raw Data)"):
            st.dataframe(df)

        # --- Visualisasi Interaktif ---
        st.markdown("## Analisis Visual")
        
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()

        viz_col1, viz_col2 = st.columns(2)

        with viz_col1:
            st.subheader("Distribusi Data (Histogram)")
            hist_col = st.selectbox("Pilih kolom untuk melihat distribusi:", options=numeric_cols, key="hist")
            if hist_col:
                fig_hist = px.histogram(df, x=hist_col, title=f"Distribusi Kolom {hist_col}")
                st.plotly_chart(fig_hist, use_container_width=True)

        with viz_col2:
            st.subheader("Perbandingan Kategori (Bar Chart)")
            bar_cat_col = st.selectbox("Pilih kolom kategori (Sumbu X):", options=categorical_cols, key="bar_cat")
            bar_num_col = st.selectbox("Pilih kolom numerik (Sumbu Y):", options=numeric_cols, key="bar_num")
            if bar_cat_col and bar_num_col:
                agg_df = df.groupby(bar_cat_col)[bar_num_col].sum().reset_index()
                fig_bar = px.bar(agg_df, x=bar_cat_col, y=bar_num_col, title=f"{bar_num_col} berdasarkan {bar_cat_col}")
                st.plotly_chart(fig_bar, use_container_width=True)
else:
    st.warning("Tidak ada file laporan yang ditemukan di folder 'files/outputs/'.")
    st.info("Silakan jalankan script `main_merge.py` terlebih dahulu untuk menghasilkan laporan.")