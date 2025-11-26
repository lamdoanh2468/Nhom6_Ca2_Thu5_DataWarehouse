import pandas as pd
import glob  
from db_connector import get_connection, log_etl
from datetime import datetime
import warnings
import re

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

    # 1. Xử lý GIÁ (Price)
    df_staging['clean_price'] = df_staging['Price'].astype(str).str.replace('.', '').str.replace('đ', '').str.strip()
    df_staging['clean_price'] = pd.to_numeric(df_staging['clean_price'], errors='coerce').fillna(0)

    # 2. Xử lý RAM & Storage
    df_staging['clean_ram'] = df_staging['Ram'].apply(extract_number)
    df_staging['clean_storage'] = df_staging['Storage'].apply(extract_number)

    # 3. Tách THƯƠNG HIỆU (Brand)
    df_staging['clean_brand'] = df_staging['Name'].apply(lambda x: str(x).split()[1] if len(str(x).split()) > 1 else "Unknown")

    # 4. Xử lý CPU
    df_staging['clean_cpu_name'] = df_staging['CpuType'].fillna("Unknown")
    
    # 5. [QUAN TRỌNG] Xử lý lỗi 'nan' trong SQL
    # Thay thế tất cả giá trị NaN (Not a Number) thành None (để MySQL hiểu là NULL)
    # Hoặc thành chuỗi rỗng "" với các cột văn bản
    cols_to_fix = ['Resolution', 'OSystem', 'Battery', 'Display', 'GPU']
    for col in cols_to_fix:
        if col in df_staging.columns:
            df_staging[col] = df_staging[col].fillna("") # Điền chuỗi rỗng nếu thiếu

    # Loại bỏ các dòng rác (Giá = 0)
    df_clean = df_staging[df_staging['clean_price'] > 0].copy()
    
    # Bước chốt chặn cuối cùng: Replace toàn bộ NaN còn sót lại thành None
    df_clean = df_clean.where(pd.notnull(df_clean), None)

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

import pandas as pd
import shutil
import os
from datetime import datetime
from db_connector import get_connection, log_etl

def run_dw_process():
    process_name = "ETL_DW_Daily"
    log_etl(process_name, "Running", "Bắt đầu nạp dữ liệu vào DW...")

    conn_dw = None
    
    # 1. Tìm tất cả các file CSV trong folder processed
    # Dùng glob để bắt pattern *.csv (bất kể tên file là gì)
    processed_dir = "data/processed"
    csv_files = glob.glob(os.path.join(processed_dir, "*.csv"))

    if not csv_files:
        print("⚠️ Không tìm thấy file .csv nào trong data/processed!")
        log_etl(process_name, "Warning", "Không có file csv để ETL.")
        return

    print(f"📂 Tìm thấy {len(csv_files)} file cần xử lý: {csv_files}")

    # Mở kết nối DB (Mở 1 lần dùng cho vòng lặp)
    conn_dw = get_connection('dw')
    if not conn_dw: 
        return
    
    cursor_dw = conn_dw.cursor()

    # ================================
    # VÒNG LẶP XỬ LÝ TỪNG FILE
    # ================================
    for processed_file in csv_files:
        try:
            print(f"\n🔄 Đang xử lý file: {processed_file}")
            
            # --- ĐỌC DỮ LIỆU ---
            df_staging = pd.read_csv(processed_file)
            
            if df_staging.empty:
                print(f"⚠️ File {processed_file} trống! Bỏ qua.")
                continue

            # --- TRANSFORM ---
            df_clean = transform_data(df_staging)

            # --- LOAD VÀO DW ---
            fact_rows = []
            current_date_key = int(datetime.now().strftime('%Y%m%d'))

            conn_dw.start_transaction()

            for index, row in df_clean.iterrows():
                try:
                    brand_id = load_dim_brand(cursor_dw, row['clean_brand'])
                    cpu_id = load_dim_cpu(cursor_dw, row['clean_cpu_name'])
                    laptop_id = load_dim_laptop(cursor_dw, row)

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

            # Insert Fact
            if fact_rows:
                # Xóa dữ liệu cũ của ngày hôm nay (để tránh double dữ liệu nếu chạy lại)
                # Lưu ý: Nếu muốn cộng dồn thì bỏ dòng DELETE này đi
                cursor_dw.execute("DELETE FROM Fact_Laptop WHERE DateKey = %s", (current_date_key,))
                
                sql_fact = """
                    INSERT INTO Fact_Laptop (DateKey, laptop_id, brand_id, cpu_id, price, ram_storage_gb, storage_capacity_gb)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor_dw.executemany(sql_fact, fact_rows)
                conn_dw.commit()

                print(f"✅ THÀNH CÔNG: Đã nạp {len(fact_rows)} dòng từ {os.path.basename(processed_file)}.")
                log_etl(process_name, "Success", f"Nạp thành công {len(fact_rows)} dòng.", len(fact_rows))
            
            # --- ARCHIVE FILE (Di chuyển sau khi xử lý xong) ---
            # Tạo thư mục archived nếu chưa có
            if not os.path.exists("data/archived"):
                os.makedirs("data/archived")

            file_name = os.path.basename(processed_file) # Lấy tên file gốc (vd: laptop_2025....csv)
            # Thêm timestamp lúc archive để tránh trùng tên nếu chạy nhiều lần
            archive_name = f"{os.path.splitext(file_name)[0]}_archived_{datetime.now().strftime('%H%M%S')}.csv"
            archived_path = os.path.join("data/archived", archive_name)

            shutil.move(processed_file, archived_path)
            print(f"📁 Đã chuyển file → {archived_path}")

        except Exception as e:
            conn_dw.rollback()
            print(f"❌ LỖI KHI XỬ LÝ FILE {processed_file}: {e}")
            log_etl(process_name, "Failed", str(e))

    # Đóng kết nối sau khi xử lý hết các file
    cursor_dw.close()
    conn_dw.close()
    print("\n🏁 Hoàn tất quy trình ETL.")

if __name__ == "__main__":
    run_dw_process()