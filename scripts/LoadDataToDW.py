import pandas as pd
from db_connector import get_connection, log_etl
from datetime import datetime
import warnings
import re
import shutil 
import glob
import os
# Tắt cảnh báo không cần thiết
warnings.filterwarnings('ignore')

def extract_number(text):
    """Hàm phụ trợ để lấy số từ chuỗi (VD: '16GB' -> 16.0)"""
    if pd.isna(text): return 0.0
    matches = re.findall(r"(\d+)", str(text))
    if matches:
        return float(matches[0])
    return 0.0

def transform_data(df_staging):
    """
    Hàm làm sạch và chuẩn hóa dữ liệu từ Staging
    """
    print("🔄 [Transform] Đang xử lý và làm sạch dữ liệu...")

    # 1. Xử lý GIÁ (Price): "18.990.000đ" -> 18990000.0
    df_staging['clean_price'] = df_staging['Price'].astype(str).str.replace('.', '').str.replace('đ', '').str.strip()
    df_staging['clean_price'] = pd.to_numeric(df_staging['clean_price'], errors='coerce').fillna(0)

    # 2. Xử lý RAM & Storage thành số (cho Fact Table)
    df_staging['clean_ram'] = df_staging['Ram'].apply(extract_number)
    df_staging['clean_storage'] = df_staging['Storage'].apply(extract_number)

    # 3. Tách THƯƠNG HIỆU (Brand) từ Tên sản phẩm
    # Giả sử tên là "Laptop ASUS...", lấy chữ ASUS. Nếu không có thì lấy 'Unknown'
    df_staging['clean_brand'] = df_staging['Name'].apply(lambda x: x.split()[1] if len(x.split()) > 1 else "Unknown")

    # 4. Xử lý CPU
    df_staging['clean_cpu_name'] = df_staging['CpuType'].fillna("Unknown")

    # Loại bỏ các dòng rác (Giá = 0)
    df_clean = df_staging[df_staging['clean_price'] > 0].copy()
    
    print(f"   -> Dữ liệu sạch: {len(df_clean)} dòng (Đã loại {len(df_staging) - len(df_clean)} dòng rác)")
    return df_clean

def load_dim_brand(cursor, brand_name):
    """Nạp Brand và trả về brand_id"""
    cursor.execute("SELECT brand_id FROM Dim_Brand WHERE brand_name = %s", (brand_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("INSERT INTO Dim_Brand (brand_name) VALUES (%s)", (brand_name,))
        return cursor.lastrowid

def load_dim_cpu(cursor, cpu_type):
    """Nạp CPU và trả về cpu_id"""
    cursor.execute("SELECT cpu_id FROM Dim_CPU WHERE cpu_type = %s", (cpu_type,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("INSERT INTO Dim_CPU (cpu_type) VALUES (%s)", (cpu_type,))
        return cursor.lastrowid

def load_dim_laptop(cursor, row):
    """Nạp Dimension Laptop (SCD Type 1 - Update thông tin nếu đã tồn tại)"""
    # Kiểm tra xem laptop này (dựa trên Link) đã có trong DW chưa
    cursor.execute("SELECT laptop_id FROM Dim_Laptop WHERE link = %s", (row['links_href'],))
    result = cursor.fetchone()
    
    if result:
        # Nếu tồn tại -> Cập nhật thông tin mới nhất (SCD Type 1)
        laptop_id = result[0]
        sql_update = """
            UPDATE Dim_Laptop 
            SET name=%s, ram_storage=%s, storage_capacity=%s, display=%s, 
                resolution=%s, os=%s, battery=%s, gpu=%s
            WHERE laptop_id=%s
        """
        val_update = (
            row['Name'], row['Ram'], row['Storage'], row['Display'],
            row.get('Resolution', ''), row.get('OSystem', ''), row.get('Battery', ''), row['GPU'],
            laptop_id
        )
        cursor.execute(sql_update, val_update)
        return laptop_id
    else:
        # Nếu chưa tồn tại -> Thêm mới
        sql_insert = """
            INSERT INTO Dim_Laptop (name, ram_storage, storage_capacity, display, resolution, os, battery, gpu, link)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        val_insert = (
            row['Name'], row['Ram'], row['Storage'], row['Display'],
            row.get('Resolution', ''), row.get('OSystem', ''), row.get('Battery', ''), row['GPU'],
            row['links_href']
        )
        cursor.execute(sql_insert, val_insert)
        return cursor.lastrowid

def run_dw_process():
    process_name = "ETL_DW_Daily"
    log_etl(process_name, "Running", "Bắt đầu nạp dữ liệu vào DW...")
    
    conn_dw = None
    conn_staging = None
    
    try:
        # 1. Thiết lập đường dẫn
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        processed_path = os.path.join(base_dir, 'data', 'processed')
        
        archive_path = os.path.join(base_dir, 'data', 'archive')
        if not os.path.exists(archive_path):
            os.makedirs(archive_path)
        # ---------------------------------------

        csv_files = glob.glob(os.path.join(processed_path, "*.csv"))
        # 1. Lấy dữ liệu từ Staging
        conn_staging = get_connection('staging')
        if not conn_staging: return
        
        print("📥 Đang đọc dữ liệu từ Staging...")
        df_staging = pd.read_sql("SELECT * FROM stg_laptops", conn_staging)
        conn_staging.close() # Đóng kết nối staging sớm cho nhẹ
        
        if df_staging.empty:
            print("⚠️ Staging trống!")
            log_etl(process_name, "Warning", "Staging trống.")
            return

        # 2. Transform
        df_clean = transform_data(df_staging)
        
        # 3. Load vào DW
        conn_dw = get_connection('dw')
        if not conn_dw: return
        cursor_dw = conn_dw.cursor()
        
        fact_rows = []
        current_date_key = int(datetime.now().strftime('%Y%m%d'))
        
        print("🚀 Đang nạp vào Dimensions & Fact...")
        
        # Dùng Transaction để đảm bảo toàn vẹn dữ liệu
        conn_dw.start_transaction()

        for index, row in df_clean.iterrows():
            try:
                # --- Load Dimensions (Lookup & Insert) ---
                brand_id = load_dim_brand(cursor_dw, row['clean_brand'])
                cpu_id = load_dim_cpu(cursor_dw, row['clean_cpu_name'])
                laptop_id = load_dim_laptop(cursor_dw, row)
                
                # --- Chuẩn bị dữ liệu Fact ---
                fact_rows.append((
                    current_date_key,
                    laptop_id,
                    brand_id,
                    cpu_id,
                    row['clean_price'],
                    row['clean_ram'],
                    row['clean_storage']
                ))
            except Exception as e:
                print(f"⚠️ Lỗi xử lý dòng {index}: {e}")
                continue
            
        # 4. Insert Batch vào Fact Table
        if fact_rows:
            # Xóa dữ liệu Fact cũ của ngày hôm nay (nếu chạy lại nhiều lần trong ngày) để tránh duplicate
            cursor_dw.execute("DELETE FROM Fact_Laptop WHERE DateKey = %s", (current_date_key,))
            
            sql_fact = """
                INSERT INTO Fact_Laptop (DateKey, laptop_id, brand_id, cpu_id, price, ram_storage_gb, storage_capacity_gb)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor_dw.executemany(sql_fact, fact_rows)
            conn_dw.commit()
            
            print(f"✅ THÀNH CÔNG: Đã nạp {len(fact_rows)} dòng vào Fact_Laptop.")
            print("📦 Đang di chuyển file đã nạp sang 'data/archive'...")
            for file in csv_files:
                file_name = os.path.basename(file)
                try:
                    # Di chuyển file từ processed -> archive
                    shutil.move(file, os.path.join(archive_path, file_name))
                    print(f"   -> Đã lưu kho: {file_name}")
                except Exception as e_move:
                    print(f"   ⚠️ Không thể di chuyển file {file_name}: {e_move}")
            log_etl(process_name, "Success", f"Nạp DW thành công {len(fact_rows)} dòng.", len(fact_rows))
        else:
            print("⚠️ Không có dữ liệu để nạp vào Fact.")
        
        cursor_dw.close()
        conn_dw.close()

    except Exception as e:
        if conn_dw: conn_dw.rollback()
        print(f"❌ LỖI NGHIÊM TRỌNG: {e}")
        log_etl(process_name, "Failed", str(e))

if __name__ == "__main__":
    run_dw_process()