import pandas as pd
from db_connector import get_connection
from datetime import datetime

def populate_dim_date(start_year, end_year):
    """
    Hàm tạo dữ liệu ngày tháng tự động cho bảng Dim_Date
    """
    print(f"📅 Đang tạo dữ liệu thời gian từ năm {start_year} đến {end_year}...")
    
    # 1. Tạo danh sách ngày liên tục
    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"
    
    try:
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    except Exception as e:
        print(f"❌ Lỗi tạo khoảng thời gian: {e}")
        return
    
    # 2. Kết nối Database DW
    conn = get_connection('dw')
    if not conn:
        print("❌ Không thể kết nối tới DW.")
        return
    
    cursor = conn.cursor()
    
    # 3. Chuẩn bị câu lệnh Insert
    # Cấu trúc bảng: DateKey, FullDate, DayOfMonth, Month, MonthName, Year
    sql = """
        INSERT IGNORE INTO Dim_Date 
        (DateKey, FullDate, DayOfMonth, Month, MonthName, Year) 
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    val_list = []
    for single_date in date_range:
        # Tạo DateKey dạng số nguyên YYYYMMDD (ví dụ: 20251112)
        date_key = int(single_date.strftime('%Y%m%d'))
        
        # Các thuộc tính khác
        full_date = single_date.strftime('%Y-%m-%d')
        day = single_date.day
        month = single_date.month
        month_name = single_date.strftime('%B') # Tên tháng (January, February...)
        year = single_date.year
        
        val_list.append((date_key, full_date, day, month, month_name, year))
        
    # 4. Thực thi Insert
    try:
        if val_list:
            print(f"⏳ Đang nạp {len(val_list)} dòng vào Dim_Date...")
            cursor.executemany(sql, val_list)
            conn.commit()
            print(f"✅ THÀNH CÔNG! Đã nạp lịch từ {start_year} đến {end_year}.")
        else:
            print("⚠️ Không có dữ liệu ngày tháng nào được tạo.")
            
    except Exception as e:
        print(f"❌ Lỗi khi Insert vào DB: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# --- CẤU HÌNH NĂM CẦN TẠO ---
if __name__ == "__main__":
    # 👇👇👇 SỬA NĂM MUỐN TẠO Ở ĐÂY 👇👇👇
    START_YEAR = 2023
    END_YEAR = 2030
    # ---------------------------------------
    
    populate_dim_date(START_YEAR, END_YEAR)