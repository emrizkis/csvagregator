import sys
import os
import glob
from datetime import datetime
import pandas as pd
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QLineEdit, QFileDialog, QTextEdit, QComboBox,
    QHeaderView, QTableWidget, QTableWidgetItem, QMessageBox
)
from PyQt6.QtCore import QThread, pyqtSignal, QObject
from PyQt6.QtGui import QFont, QIcon

# ==============================================================================
# Helper Function to get correct Base Path (for App Icon)
# ==============================================================================
def get_base_path():
    """
    Gets the correct base path for the application's internal assets like icons.
    Handles running as a script or as a frozen PyInstaller bundle.
    """
    if getattr(sys, 'frozen', False):
        if sys.platform == 'darwin':
            # Path when bundled on macOS
            return os.path.abspath(os.path.join(os.path.dirname(sys.executable), '..', '..', '..'))
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

# ==============================================================================
# Worker Thread for Merging Process
# ==============================================================================
class MergeWorker(QObject):
    finished = pyqtSignal()
    log = pyqtSignal(str)
    error = pyqtSignal(str)
    progress = pyqtSignal(str)

    def __init__(self, source_a, source_b, merge_key, output_dir):
        super().__init__()
        self.source_a = source_a
        self.source_b = source_b
        self.merge_key = merge_key
        self.output_dir = output_dir # Directory chosen by the user

    def run(self):
        try:
            self.log.emit("--- Memulai Proses Penggabungan Data ---")

            # --- DYNAMIC FOLDER LOGIC ---
            # Use the user-provided directory as the base for outputs and temp files.
            base_output_path = self.output_dir
            path_temp = os.path.join(base_output_path, 'temp')
            path_output = os.path.join(base_output_path, 'outputs')

            # Create these directories in the user-selected location
            os.makedirs(path_temp, exist_ok=True)
            self.log.emit(f"Folder sementara disiapkan di: {path_temp}")
            os.makedirs(path_output, exist_ok=True)
            self.log.emit(f"Folder output disiapkan di: {path_output}")
            # --- END OF CHANGE ---

            temp_a_file = os.path.join(path_temp, 'consolidated_a.csv')
            temp_b_file = os.path.join(path_temp, 'consolidated_b.csv')

            # --- TAHAP 1: KONSOLIDASI ---
            self.log.emit("\n--- Tahap 1: Konsolidasi Masing-Masing Sumber ---")
            self.progress.emit("Mengonsolidasi Source A...")
            success_a = self.consolidate_csvs(self.source_a, temp_a_file)
            QThread.msleep(100)
            self.progress.emit("Mengonsolidasi Source B...")
            success_b = self.consolidate_csvs(self.source_b, temp_b_file)
            QThread.msleep(100)

            if not (success_a and success_b):
                self.error.emit("Proses dihentikan karena salah satu tahap konsolidasi gagal.")
                self.finished.emit()
                return

            # --- TAHAP 2: PENGGABUNGAN (MERGE) ---
            self.log.emit("\n--- Tahap 2: Penggabungan (Merge) Berdasarkan Kunci ---")
            self.progress.emit(f"Menggabungkan data dengan kunci: '{self.merge_key}'...")
            
            df_a = pd.read_csv(temp_a_file)
            df_b = pd.read_csv(temp_b_file)

            if self.merge_key not in df_a.columns or self.merge_key not in df_b.columns:
                err_msg = (f"Error: Kolom kunci '{self.merge_key}' tidak ditemukan di salah satu sumber.\n"
                           f"Kolom di Source A: {list(df_a.columns)}\n"
                           f"Kolom di Source B: {list(df_b.columns)}")
                self.error.emit(err_msg)
                self.finished.emit()
                return

            final_df = pd.merge(df_a, df_b, on=self.merge_key, how='inner')

            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            final_output_file = os.path.join(path_output, f"{timestamp}_final_merge.csv")
            final_df.to_csv(final_output_file, index=False, encoding='utf-8')

            self.log.emit("\n🎉  Sukses! Proses merge selesai.")
            self.log.emit(f"Hasil disimpan di: '{final_output_file}'")
            self.log.emit(f"Total baris hasil merge: {len(final_df)}")
            self.progress.emit("Selesai!")

        except Exception as e:
            self.error.emit(f"Terjadi kesalahan: {e}")
        finally:
            self.finished.emit()

    def consolidate_csvs(self, input_path, output_file):
        try:
            if not os.path.isdir(input_path):
                self.log.emit(f"⚠️  Folder input '{input_path}' tidak ditemukan.")
                return False

            all_files = glob.glob(os.path.join(input_path, "*.csv"))
            if not all_files:
                self.log.emit(f"⚠️  Tidak ada file CSV di '{input_path}'.")
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
                self.log.emit(f"  -> Memproses: {os.path.basename(f)}")
            
            self.log.emit(f"✅  Konsolidasi '{input_path}' berhasil.")
            return True
        except Exception as e:
            self.error.emit(f"Gagal saat konsolidasi '{input_path}': {e}")
            return False

# ==============================================================================
# Main Application Window
# ==============================================================================
class App(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Dynamic CSV Merger & Dashboard")
        self.setGeometry(100, 100, 1200, 800)
        
        # Set App Icon
        base_path = get_base_path()
        if sys.platform == 'darwin':
            icon_path = os.path.join(base_path, 'assets', 'icon.icns')
        else:
            icon_path = os.path.join(base_path, 'assets', 'icon.ico')

        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))

        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.main_layout = QHBoxLayout(self.central_widget)

        self.left_panel = QWidget()
        self.left_layout = QVBoxLayout(self.left_panel)
        self.left_panel.setFixedWidth(350)

        self.right_panel = QWidget()
        self.right_layout = QVBoxLayout(self.right_panel)

        self.main_layout.addWidget(self.left_panel)
        self.main_layout.addWidget(self.right_panel)

        self.init_ui_controls()
        self.init_ui_dashboard()

    def init_ui_controls(self):
        title_label = QLabel("Pengaturan Proses Merge")
        title_label.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        self.left_layout.addWidget(title_label)

        # --- NEW: Output Directory Selection ---
        self.output_dir_label = QLabel("1. Pilih Folder Output/Kerja:")
        self.output_dir_path = QLineEdit()
        self.output_dir_path.setPlaceholderText("Hasil merge akan disimpan di sini")
        self.output_dir_btn = QPushButton("Browse...")
        self.output_dir_btn.clicked.connect(lambda: self.browse_folder(self.output_dir_path))
        output_dir_layout = QHBoxLayout()
        output_dir_layout.addWidget(self.output_dir_path)
        output_dir_layout.addWidget(self.output_dir_btn)
        self.left_layout.addWidget(self.output_dir_label)
        self.left_layout.addLayout(output_dir_layout)
        self.left_layout.addSpacing(15)
        
        # Source A
        self.source_a_label = QLabel("2. Pilih Folder Source A:")
        self.source_a_path = QLineEdit()
        self.source_a_path.setPlaceholderText("Path ke folder source A")
        self.source_a_btn = QPushButton("Browse...")
        self.source_a_btn.clicked.connect(lambda: self.browse_folder(self.source_a_path))
        source_a_layout = QHBoxLayout()
        source_a_layout.addWidget(self.source_a_path)
        source_a_layout.addWidget(self.source_a_btn)
        self.left_layout.addWidget(self.source_a_label)
        self.left_layout.addLayout(source_a_layout)

        # Source B
        self.source_b_label = QLabel("3. Pilih Folder Source B:")
        self.source_b_path = QLineEdit()
        self.source_b_path.setPlaceholderText("Path ke folder source B")
        self.source_b_btn = QPushButton("Browse...")
        self.source_b_btn.clicked.connect(lambda: self.browse_folder(self.source_b_path))
        source_b_layout = QHBoxLayout()
        source_b_layout.addWidget(self.source_b_path)
        source_b_layout.addWidget(self.source_b_btn)
        self.left_layout.addWidget(self.source_b_label)
        self.left_layout.addLayout(source_b_layout)

        # Merge Key
        self.merge_key_label = QLabel("4. Masukkan Foreign Key untuk Merge:")
        self.merge_key_input = QLineEdit()
        self.merge_key_input.setPlaceholderText("Contoh: id_transaksi")
        self.left_layout.addWidget(self.merge_key_label)
        self.left_layout.addWidget(self.merge_key_input)

        # Run Button
        self.run_button = QPushButton("Jalankan Proses Merge")
        self.run_button.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        self.run_button.setStyleSheet("background-color: #4CAF50; color: white; padding: 10px;")
        self.run_button.clicked.connect(self.run_merge_process)
        self.left_layout.addWidget(self.run_button)

        self.progress_label = QLabel("Status: Idle")
        self.left_layout.addWidget(self.progress_label)
        
        self.log_label = QLabel("Log Proses:")
        self.log_area = QTextEdit()
        self.log_area.setReadOnly(True)
        self.log_area.setFont(QFont("Courier New", 9))
        self.log_area.setStyleSheet("background-color: #222; color: #0f0;")
        self.left_layout.addWidget(self.log_label)
        self.left_layout.addWidget(self.log_area)

        self.left_layout.addStretch()

    def init_ui_dashboard(self):
        dash_title = QLabel("Dashboard Hasil Laporan")
        dash_title.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        self.right_layout.addWidget(dash_title)

        selector_layout = QHBoxLayout()
        self.report_selector_label = QLabel("Pilih Laporan:")
        self.report_selector = QComboBox()
        self.refresh_button = QPushButton("Refresh Laporan")
        
        selector_layout.addWidget(self.report_selector_label)
        selector_layout.addWidget(self.report_selector, 1)
        selector_layout.addWidget(self.refresh_button)
        self.right_layout.addLayout(selector_layout)

        self.table_widget = QTableWidget()
        self.right_layout.addWidget(self.table_widget)

        self.refresh_button.clicked.connect(self.populate_report_selector)
        self.report_selector.currentIndexChanged.connect(self.display_report)

        self.populate_report_selector()

    def browse_folder(self, line_edit):
        folder = QFileDialog.getExistingDirectory(self, "Pilih Folder")
        if folder:
            line_edit.setText(folder)
    
    def run_merge_process(self):
        output_dir = self.output_dir_path.text()
        source_a = self.source_a_path.text()
        source_b = self.source_b_path.text()
        merge_key = self.merge_key_input.text()

        if not all([output_dir, source_a, source_b, merge_key]):
            self.show_error_message("Harap isi semua field (Folder Output, Source A, B, dan Foreign Key).")
            return

        self.run_button.setEnabled(False)
        self.run_button.setText("Sedang Memproses...")
        self.log_area.clear()

        self.thread = QThread()
        self.worker = MergeWorker(source_a, source_b, merge_key, output_dir)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        
        self.worker.log.connect(self.log_area.append)
        self.worker.error.connect(self.on_merge_error)
        self.worker.progress.connect(self.progress_label.setText)
        self.thread.finished.connect(self.on_merge_finished)
        
        self.thread.start()

    def on_merge_finished(self):
        self.run_button.setEnabled(True)
        self.run_button.setText("Jalankan Proses Merge")
        self.progress_label.setText("Status: Idle")
        self.populate_report_selector()
        QMessageBox.information(self, "Selesai", "Proses penggabungan data telah selesai!")

    def on_merge_error(self, message):
        self.log_area.append(f"❌ ERROR: {message}")
        self.show_error_message(message)

    def show_error_message(self, message):
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Icon.Critical)
        msg.setText("Terjadi Kesalahan")
        msg.setInformativeText(message)
        msg.setWindowTitle("Error")
        msg.exec()

    def populate_report_selector(self):
        self.report_selector.clear()
        output_dir_path = self.output_dir_path.text()
        if not output_dir_path:
            self.report_selector.addItem("Pilih Folder Output terlebih dahulu")
            return

        output_dir = os.path.join(output_dir_path, 'outputs')
        if not os.path.isdir(output_dir):
            self.report_selector.addItem("Folder 'outputs' tidak ditemukan")
            return
            
        try:
            files = glob.glob(os.path.join(output_dir, "*.csv"))
            files.sort(key=os.path.getmtime, reverse=True)
            if files:
                self.report_selector.addItems([os.path.basename(f) for f in files])
            else:
                self.report_selector.addItem("Tidak ada laporan ditemukan")
        except Exception as e:
            self.show_error_message(f"Gagal memuat laporan: {e}")

    def display_report(self, index):
        if index < 0: return
        output_dir_path = self.output_dir_path.text()
        if not output_dir_path: return

        file_name = self.report_selector.itemText(index)
        output_dir = os.path.join(output_dir_path, 'outputs')
        file_path = os.path.join(output_dir, file_name)

        if not os.path.exists(file_path):
            self.table_widget.clear()
            self.table_widget.setRowCount(0)
            self.table_widget.setColumnCount(0)
            return
        
        try:
            df = pd.read_csv(file_path)
            self.table_widget.setRowCount(df.shape[0])
            self.table_widget.setColumnCount(df.shape[1])
            self.table_widget.setHorizontalHeaderLabels(df.columns)

            for i, row in df.iterrows():
                for j, val in enumerate(row):
                    self.table_widget.setItem(i, j, QTableWidgetItem(str(val)))
            
            self.table_widget.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        except Exception as e:
            self.show_error_message(f"Gagal menampilkan file {file_name}: {e}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = App()
    window.show()
    sys.exit(app.exec())

