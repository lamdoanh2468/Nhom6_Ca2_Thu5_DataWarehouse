import pandas as pd
from db_connector import get_connection, log_etl
import warnings

# Tắt cảnh báo không cần thiết
warnings.filterwarnings('ignore')

def run_datamart_process():
    # 5.1.2.1 Khởi tạo process_name, kết nối data warehouse và data mart (chuẩn bị)
    process_name = "ETL_DataMart_Daily"
    conn_dw = None
    conn_dm = None
    # 5.1.2.2 log_etl(process_name, "Running", "Bắt đầu tổng hợp dữ liệu sang bảng Agg_LaptopSummary")
    log_etl(process_name, "Running", "Bắt đầu tổng hợp dữ liệu sang bảng Agg_LaptopSummary...")
    try:
        # 5.1.2.3 Kiểm tra/kết nối data warehouse
        conn_dw = get_connection('dw')
        if not conn_dw:
            print("❌ Không kết nối được Data Warehouse.")
            return
        
        # 5.1.2.4 Thực thi câu lệnh SQL SELECT lấy dữ liệu tổng hợp giữa bảng fact_laptop và dim_brand
        sql_extract = """
            SELECT 
                f.DateKey,
                b.brand_name,
                COUNT(f.laptop_id) as TotalProducts,
                AVG(f.price) as AvgPrice,
                MIN(f.price) as MinPrice,
                MAX(f.price) as MaxPrice,
                AVG(f.ram_storage_gb) as AvgRAM,
                AVG(f.storage_capacity_gb) as AvgStorage
            FROM Fact_Laptop f
            JOIN Dim_Brand b ON f.brand_id = b.brand_id
            GROUP BY f.DateKey, b.brand_name;
        """
        
        print("📊 Đang tính toán các chỉ số tổng hợp từ DW...")
        df_analysis = pd.read_sql(sql_extract, conn_dw)

        # 5.1.2.5 Đóng kết nối database data warehouse
        conn_dw.close()
        
        # 5.1.2.6 Kiểm tra dữ liệu trong fact_laptop sau khi tổng hợp
        if df_analysis.empty:
            print("⚠️ Không có dữ liệu trong Fact để tổng hợp.")
            return

        # 5.1.2.7 Kết nối với database Data Mart
        conn_dm = get_connection('datamart') 
        
        if not conn_dm: 
            print("❌ Không kết nối được Data Mart.")
            return
            
        # 5.1.2.8 Khởi tạo cursor cho Data Mart
        cursor_dm = conn_dm.cursor()
        
        print(f"🚀 Đang nạp {len(df_analysis)} dòng vào bảng Agg_LaptopSummary...")
        
        # 5.1.2.9 Thực thi câu lệnh SQL chèn dữ liệu vào bảng Agg_LaptopSummary
        sql_load = """
            INSERT INTO Agg_LaptopSummary 
            (DateKey, BrandName, TotalProducts, AvgPrice, MinPrice, MaxPrice, AvgRAM, AvgStorage)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                TotalProducts = VALUES(TotalProducts),
                AvgPrice = VALUES(AvgPrice),
                MinPrice = VALUES(MinPrice),
                MaxPrice = VALUES(MaxPrice),
                AvgRAM = VALUES(AvgRAM),
                AvgStorage = VALUES(AvgStorage);
        """
        
        # 5.1.2.10 Chuyển DataFrame df_analysis sang danh sách tuple
        data_tuples = [tuple(x) for x in df_analysis.to_numpy()]
        
        # 5.1.2.11 Sử dụng cursor trong Data Mart thực thi nhiều câu lệnh SQL
        cursor_dm.executemany(sql_load, data_tuples)

        # 5.1.2.12 Xác nhận thay đổi xuống database Data Mart
        conn_dm.commit()
        
        print(f"✅ THÀNH CÔNG: Đã cập nhật bảng Agg_LaptopSummary cho {len(data_tuples)} nhãn hàng.")
        # 5.1.2.13 log_etl("Success", "Đã tổng hợp n dòng vào Agg_Summary", n)
        log_etl(process_name, "Success", f"Đã tổng hợp {len(data_tuples)} dòng vào Agg_LaptopSummary.", len(data_tuples))
        
        # 5.1.2.14 Đóng cursor và đóng kết nối với database Data Mart
        cursor_dm.close()
        conn_dm.close()

    except Exception as e:
        print(f"❌ LỖI: {e}")
        log_etl(process_name, "Failed", str(e))

if __name__ == "__main__":
    run_datamart_process()