import mysql.connector
import json
import os
from datetime import datetime

# --- 1. CẤU HÌNH ĐƯỜNG DẪN ---
# Đi từ thư mục scripts ra ngoài project -> vào folder config
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'db_config.json')

# --- 2. HÀM TẠO KẾT NỐI ---
def get_connection(db_type):
    """
    Tạo kết nối đến database dựa trên db_type.
    db_type: 'control', 'staging', hoặc 'dw'
    """
    try:
        # Đọc file config
        with open(CONFIG_PATH, 'r') as f:
            config = json.load(f)
        
        # Kiểm tra xem db_type có tồn tại trong config không
        if db_type not in config:
            raise ValueError(f"Không tìm thấy cấu hình cho loại database: {db_type}")

        db_config = config[db_type]
        
        # Tạo kết nối
        conn = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            port=db_config['port']
        )
        return conn
    except Exception as e:
        print(f"❌ Lỗi kết nối Database ({db_type}): {e}")
        return None

# --- 3. HÀM GHI LOG (Sử dụng get_connection ở trên) ---
def log_etl(process_name, status, message, rows_affected=0):
    """
    Hàm ghi log vào bảng Etl_Log trong database Control.
    Status: 'Running', 'Success', 'Failed'
    """
    try:
        # Kết nối tới DB Control để ghi log
        conn = get_connection('control')
        
        if conn:
            cursor = conn.cursor()
            query = """
                INSERT INTO Etl_Log (ProcessName, StartTime, EndTime, Status, Message, RowsAffected)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            now = datetime.now()
            val = (process_name, now, now, status, message, rows_affected)
            
            cursor.execute(query, val)
            conn.commit()
            cursor.close()
            conn.close()
            
            # In ra màn hình console để dễ theo dõi
            icon = "✅" if status == "Success" else "❌" if status == "Failed" else "ℹ️"
            print(f"{icon} [{status}] {process_name}: {message}")
            
    except Exception as e:
        print(f"⚠️ Không thể ghi log hệ thống: {e}")

# --- 4. CHẠY THỬ (MAIN) ---
if __name__ == "__main__":
    print("--- Kiểm tra kết nối ---")
    test_conn = get_connection('staging')
    if test_conn:
        print("Kết nối Staging thành công!")
        test_conn.close()
    
    print("--- Kiểm tra ghi Log ---")
    log_etl("Test Connection", "Success", "Kiểm tra kết nối từ db_connector.py")