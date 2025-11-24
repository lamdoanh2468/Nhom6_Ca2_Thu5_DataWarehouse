import os
import glob
import shutil
import pandas as pd
from db_connector import get_connection, log_etl

def run_staging_process():
    process_name = "Staging_Load_Process"
    log_etl(process_name, "Running", "Bắt đầu quét file CSV để nạp Staging...")

    # 1. Thiết lập đường dẫn thư mục
    # Lấy thư mục cha của scripts (D:/LaptopDW)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    raw_path = os.path.join(base_dir, 'data', 'raw')
    processed_path = os.path.join(base_dir, 'data', 'processed')

    # Tạo thư mục processed nếu chưa có (để lưu bản sao file sau khi xong)
    if not os.path.exists(processed_path):
        os.makedirs(processed_path)

    # 2. Tìm tất cả file .csv trong thư mục data/raw
    csv_files = glob.glob(os.path.join(raw_path, "*.csv"))

    if not csv_files:
        print("⚠️ Không tìm thấy file dữ liệu mới nào trong 'data/raw'.")
        # Ghi log Success để báo hệ thống biết là chạy xong (dù không có việc gì làm)
        log_etl(process_name, "Success", "Không có file mới để nạp.", 0)
        return

    # 3. Kết nối Database Staging
    conn = get_connection('staging')
    if not conn:
        print("❌ Không kết nối được database Staging.")
        return
    
    cursor = conn.cursor()
    total_files = 0
    total_rows = 0

    try:
        # 4. Làm sạch bảng Staging (Full Load Strategy)
        # Vì Staging chỉ là vùng đệm chứa dữ liệu mới nhất, nên xóa cũ nạp mới.
        print("🧹 Đang làm sạch bảng stg_laptops...")
        cursor.execute("TRUNCATE TABLE stg_laptops")
        
        # 5. Duyệt qua từng file CSV tìm được
        for file_path in csv_files:
            file_name = os.path.basename(file_path)
            print(f"📂 Đang xử lý file: {file_name}")

            try:
                # Đọc file CSV vào DataFrame
                df = pd.read_csv(file_path)
                
                # Kiểm tra nếu file rỗng
                if df.empty:
                    print(f"   ⚠️ File {file_name} rỗng, bỏ qua.")
                    # Copy sang processed để lưu vết thay vì move
                    shutil.copy(file_path, os.path.join(processed_path, file_name))
                    continue

                # Chuẩn bị câu lệnh Insert
                # Lưu ý: Số lượng %s phải khớp với số cột trong VALUES
                sql = """
                    INSERT INTO stg_laptops 
                    (Name, Price, links_href, CpuType, Ram, Storage, Display, GPU, OSystem, Battery, Resolution, ScrapeTimestamp) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """
                
                # Chuyển đổi dữ liệu DataFrame thành List of Tuples để nạp batch
                val_list = []
                for _, row in df.iterrows():
                    # Sử dụng .get('', '') để tránh lỗi nếu file CSV thiếu cột
                    # Ép kiểu str() để đảm bảo không lỗi dữ liệu
                    val_list.append((
                        str(row.get('Name', '')), 
                        str(row.get('Price', '0')), 
                        str(row.get('links_href', '')), 
                        str(row.get('CpuType', '')), 
                        str(row.get('Ram', '')), 
                        str(row.get('Storage', '')), 
                        str(row.get('Display', '')), 
                        str(row.get('GPU', '')), 
                        str(row.get('OSystem', '')),
                        str(row.get('Battery', '')), 
                        str(row.get('Resolution', ''))
                    ))

                # Thực thi nạp hàng loạt (Bulk Insert) -> Tốc độ cao
                cursor.executemany(sql, val_list)
                
                rows_in_file = len(val_list)
                total_rows += rows_in_file
                total_files += 1
                
                print(f"   ✅ Đã nạp {rows_in_file} dòng.")

                # 6. Copy file đã nạp xong sang thư mục 'processed'
                # SỬA ĐỔI: Dùng shutil.copy thay vì shutil.move để giữ nguyên file gốc ở data/raw
                shutil.copy(file_path, os.path.join(processed_path, file_name))
                print(f"   📦 Đã SAO CHÉP file vào 'data/processed' (file gốc vẫn còn).")

            except Exception as e_file:
                print(f"   ❌ Lỗi khi xử lý file {file_name}: {e_file}")
                # Nếu lỗi file này, log lại và tiếp tục file khác (không dừng chương trình)
                log_etl(process_name, "Warning", f"Lỗi file {file_name}: {str(e_file)}")

        # 7. Commit giao dịch (Lưu vĩnh viễn vào DB)
        conn.commit()
        
        msg = f"Hoàn tất Staging. Xử lý {total_files} file, nạp tổng cộng {total_rows} dòng."
        print(f"🎉 {msg}")
        log_etl(process_name, "Success", msg, total_rows)

    except Exception as e:
        conn.rollback() # Hoàn tác nếu có lỗi nghiêm trọng
        err_msg = f"Lỗi hệ thống Staging: {str(e)}"
        print(f"🔥 {err_msg}")
        log_etl(process_name, "Failed", err_msg)
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_staging_process()