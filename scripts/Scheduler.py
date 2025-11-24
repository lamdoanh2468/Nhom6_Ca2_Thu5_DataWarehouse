import schedule
import time
from db_connector import get_connection

# Import cả 3 quy trình ETL
from LoadDataToStaging import run_staging_process
from LoadDataToDW import run_dw_process
from LoadDataToDataMart import run_datamart_process

def job():
    """
    Hàm này sẽ chạy toàn bộ quy trình ETL theo thứ tự
    """
    print(f"\n⏰ [Job] Bắt đầu chạy toàn bộ quy trình ETL lúc: {time.strftime('%H:%M:%S')}")
    
    # Bước 1: Crawl & Staging (Lấy hàng về kho tạm)
    print("\n--- BƯỚC 1: STAGING ---")
    run_staging_process()
    
    # Bước 2: Load DW (Sơ chế và xếp lên kệ kho chính)
    print("\n--- BƯỚC 2: DATA WAREHOUSE ---")
    run_dw_process()
    
    # Bước 3: Load Data Mart (Nấu món ăn dọn lên bàn cho sếp)
    print("\n--- BƯỚC 3: DATA MART ---")
    run_datamart_process()
    
    print(f"\n🏁 [Job] Hoàn tất toàn bộ quy trình lúc: {time.strftime('%H:%M:%S')}\n")

def get_schedule_time():
    """Lấy giờ chạy từ Database cấu hình"""
    default_time = "12:26"
    try:
        conn = get_connection('control')
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ConfigValue FROM Etl_Config WHERE ConfigKey = 'daily_scrape_time'")
            result = cursor.fetchone()
            conn.close()
            if result:
                return result[0] # Trả về giờ trong DB (VD: "10:00")
    except Exception as e:
        print(f"⚠️ Không lấy được giờ chạy từ DB, dùng mặc định {default_time}. Lỗi: {e}")
    return default_time

# --- Cấu hình lịch chạy ---
if __name__ == "__main__":
    run_time = get_schedule_time()
    
    # Lên lịch chạy hàng ngày
    schedule.every().day.at(run_time).do(job)
    
    print(f"⏳ Hệ thống Scheduler đang chạy...")
    print(f"📅 Đã lên lịch ETL vào lúc: {run_time} hàng ngày.")
    print("👉 Nhấn Ctrl + C để dừng chương trình.")

    # Vòng lặp vô tận để duy trì script chạy ngầm
    while True:
        schedule.run_pending()
        time.sleep(60) # Kiểm tra mỗi phút