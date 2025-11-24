from db_connector import get_connection, log_etl

def test_system():
    print("--- 1. KIỂM TRA KẾT NỐI DATABASE ---")
    
    # Kiểm tra Control (Lưu Log)
    conn_ctrl = get_connection('control')
    if conn_ctrl:
        print(f"✅ Control DB: OK (Database: {conn_ctrl.database})")
        conn_ctrl.close()
    else:
        print("❌ Control DB: Thất bại")

    # Kiểm tra Staging (Lưu dữ liệu cào)
    conn_stg = get_connection('staging')
    if conn_stg:
        print(f"✅ Staging DB: OK (Database: {conn_stg.database})")
        conn_stg.close()
    else:
        print("❌ Staging DB: Thất bại")

    print("\n--- 2. KIỂM TRA GHI LOG ---")
    log_etl("Test_Connection_New", "Success", "Test ghi log vào database Control mới")
    print("👉 Hãy mở bảng 'Etl_Log' trong database 'control' để xem dòng log vừa tạo.")

if __name__ == "__main__":
    test_system()