from LoadDataToStaging import run_staging_process
from LoadDataToDW import run_dw_process
from LoadDataToDataMart import run_datamart_process
import time

def run_full_pipeline():
    print("🚀 BẮT ĐẦU CHẠY TOÀN BỘ QUY TRÌNH ETL NGAY LẬP TỨC...")
    start_total = time.time()

    # BƯỚC 1: STAGING
    print("\n-----------------------------------")
    print("1️⃣  ĐANG CHẠY STAGING (Cào & Lưu tạm)...")
    run_staging_process()

    # BƯỚC 2: DATA WAREHOUSE
    print("\n-----------------------------------")
    print("2️⃣  ĐANG CHẠY DW (Làm sạch & Lưu kho chính)...")
    run_dw_process()

    # BƯỚC 3: DATA MART
    print("\n-----------------------------------")
    print("3️⃣  ĐANG CHẠY DATA MART (Tổng hợp báo cáo)...")
    run_datamart_process()

    end_total = time.time()
    duration = end_total - start_total
    print("\n===================================")
    print(f"✅ HOÀN TẤT TOÀN BỘ! Tổng thời gian: {duration:.2f} giây.")
    print("===================================")

if __name__ == "__main__":
    run_full_pipeline()