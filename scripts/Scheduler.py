import schedule
import time
import sys
import os
from datetime import datetime
from db_connector import get_connection

# --- CẤU HÌNH HỆ THỐNG LOGGING (Tự động ghi ra file) ---
# Đường dẫn file log: D:\LaptopDW\scheduler_internal.log
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE_PATH = os.path.join(BASE_DIR, "scheduler_internal.log")

class Logger(object):
    """Lớp này giúp in ra màn hình đồng thời ghi vào file log"""
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open(LOG_FILE_PATH, "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message) # Hiện lên màn hình đen
        self.log.write(message)      # Ghi vào file
        self.log.flush()             # Lưu ngay lập tức (không chờ đệm)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

# Chuyển hướng toàn bộ lệnh print và lệnh báo lỗi vào file log
sys.stdout = Logger()
sys.stderr = sys.stdout

# --- IMPORT CÁC QUY TRÌNH ETL ---
try:
    from CrawData import crawl_data_from_source
    from LoadDataToStaging import run_staging_process
    from LoadDataToDW import run_dw_process
    from LoadDataToDataMart import run_datamart_process
except ImportError as e:
    print(f"❌ Lỗi Import thư viện: {e}")
    print("👉 Hãy chắc chắn bạn đang chạy trong môi trường ảo (venv)")

# --- ĐỊNH NGHĨA CÔNG VIỆC (JOB) ---
def job():
    """
    Quy trình ETL toàn diện: Crawl -> Staging -> DW -> Data Mart
    """
    print(f"\n========== BẮT ĐẦU JOB LÚC: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ==========")
    
    try:
        # BƯỚC 0: CRAWL DATA
        print("\n--- BƯỚC 0: CRAWL DATA (Thu thập dữ liệu) ---")
        crawl_data_from_source()

        # BƯỚC 1: STAGING
        print("\n--- BƯỚC 1: STAGING (Nạp vùng đệm) ---")
        run_staging_process()
        
        # BƯỚC 2: DATA WAREHOUSE
        print("\n--- BƯỚC 2: DATA WAREHOUSE (Làm sạch & Lưu kho) ---")
        run_dw_process()
        
        # BƯỚC 3: DATA MART
        print("\n--- BƯỚC 3: DATA MART (Tổng hợp báo cáo) ---")
        run_datamart_process()
        
        print(f"\n✅ [Job] HOÀN TẤT TOÀN BỘ QUY TRÌNH LÚC: {datetime.now().strftime('%H:%M:%S')}")
        print("==============================================================\n")
        
    except Exception as e:
        print(f"\n🔥 LỖI NGHIÊM TRỌNG TRONG QUÁ TRÌNH CHẠY JOB: {e}")

def get_schedule_time():
    """Lấy giờ chạy từ Database"""
    default_time = "10:00"
    try:
        conn = get_connection('control')
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ConfigValue FROM Etl_Config WHERE ConfigKey = 'daily_scrape_time'")
            result = cursor.fetchone()
            conn.close()
            if result:
                return result[0]
    except Exception as e:
        print(f"⚠️ Không lấy được giờ từ DB ({e}), dùng mặc định {default_time}")
    return default_time

# --- MAIN (CHẠY CHƯƠNG TRÌNH) ---
if __name__ == "__main__":
    print(f"\n🚀 HỆ THỐNG KHỞI ĐỘNG LẠI VÀO LÚC: {datetime.now()}")
    print(f"📂 File log được lưu tại: {LOG_FILE_PATH}")

    # ==============================================================================
    # 👇👇👇 KHU VỰC CẤU HÌNH GIỜ CHẠY ĐỂ TEST (SỬA Ở ĐÂY) 👇👇👇
    # ==============================================================================
    
    # Nếu muốn test ngay, điền giờ tương lai gần (VD: "16:30")
    # Nếu muốn chạy thật theo DB, để là: TEST_TIME = None
    TEST_TIME = "19:45"  
    
    # ==============================================================================

    if TEST_TIME:
        run_time = TEST_TIME
        print(f"🧪 Đang chạy chế độ TEST. Giờ kích hoạt: {run_time}")
    else:
        run_time = get_schedule_time()
        print(f"⚙️ Đang chạy chế độ PRODUCTION (Lấy giờ từ DB). Giờ kích hoạt: {run_time}")
    
    # Lên lịch
    schedule.every().day.at(run_time).do(job)
    
    print(f"⏳ Đang chờ đến {run_time} để chạy...")
    
    # Vòng lặp kiểm tra
    while True:
        try:
            schedule.run_pending()
            time.sleep(1) # Kiểm tra mỗi giây để bắt giờ chính xác
        except KeyboardInterrupt:
            print("\n🛑 Đã dừng chương trình thủ công.")
            break
        except Exception as e:
            print(f"❌ Lỗi vòng lặp Scheduler: {e}")