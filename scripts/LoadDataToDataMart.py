import pandas as pd
from db_connector import get_connection, log_etl
import warnings

# Tắt cảnh báo không cần thiết
warnings.filterwarnings('ignore')

def run_datamart_process():
    process_name = "ETL_DataMart_Daily"
    log_etl(process_name, "Running", "Bắt đầu tổng hợp dữ liệu sang bảng Agg_LaptopSummary...")
    
    conn_dw = None
    conn_dm = None
    
    try:
        # --- 1. Lấy dữ liệu từ Data Warehouse (Fact Table) ---
        conn_dw = get_connection('dw')
        if not conn_dw: return
        
        # Query tổng hợp dữ liệu (Aggregation)
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
        conn_dw.close()
        
        if df_analysis.empty:
            print("⚠️ Không có dữ liệu trong Fact để tổng hợp.")
            return

        # --- 2. Nạp vào Data Mart (Bảng Agg_LaptopSummary) ---
        # Lưu ý: Nếu bạn dùng chung DB thì sửa 'datamart' thành 'dw'
        conn_dm = get_connection('datamart') 
        
        if not conn_dm: 
            print("❌ Không kết nối được Data Mart.")
            return
            
        cursor_dm = conn_dm.cursor()
        
        print(f"🚀 Đang nạp {len(df_analysis)} dòng vào bảng Agg_LaptopSummary...")
        
        # Câu lệnh UPSERT vào bảng tên mới
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
        
        data_tuples = [tuple(x) for x in df_analysis.to_numpy()]
        
        cursor_dm.executemany(sql_load, data_tuples)
        conn_dm.commit()
        
        print(f"✅ THÀNH CÔNG: Đã cập nhật bảng Agg_LaptopSummary cho {len(data_tuples)} nhãn hàng.")
        log_etl(process_name, "Success", f"Đã tổng hợp {len(data_tuples)} dòng vào Agg_LaptopSummary.", len(data_tuples))
        
        cursor_dm.close()
        conn_dm.close()

    except Exception as e:
        print(f"❌ LỖI: {e}")
        log_etl(process_name, "Failed", str(e))

if __name__ == "__main__":
    run_datamart_process()