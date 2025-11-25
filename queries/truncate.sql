/* ------------------------------------------------------
   PHẦN 1: LÀM SẠCH STAGING (Dữ liệu thô)
   ------------------------------------------------------ */
USE staging;
TRUNCATE TABLE stg_laptops;


/* ------------------------------------------------------
   PHẦN 2: LÀM SẠCH DATA WAREHOUSE (Kho dữ liệu chính)
   ------------------------------------------------------ */
USE dw_laptop;

-- Tắt kiểm tra khóa ngoại (Để xóa được các bảng cha-con)
SET FOREIGN_KEY_CHECKS = 0; 

-- 1. Xóa bảng Fact
TRUNCATE TABLE Fact_Laptop;

-- 2. Xóa các bảng Dimension (Trừ Dim_Date)
TRUNCATE TABLE Dim_Laptop;
TRUNCATE TABLE Dim_Brand;
TRUNCATE TABLE Dim_CPU;

-- LƯU Ý: 
-- Bảng Dim_Date được giữ nguyên vì chứa lịch cố định.
-- Bảng Etl_Log và Etl_Config ĐÃ CHUYỂN sang DB Control nên không xóa ở đây nữa.

-- Bật lại kiểm tra khóa ngoại
SET FOREIGN_KEY_CHECKS = 1;


/* ------------------------------------------------------
   PHẦN 3: LÀM SẠCH DATA MART (Báo cáo)
   ------------------------------------------------------ */
USE datamart;
TRUNCATE TABLE Agg_LaptopSummary;


/* ------------------------------------------------------
   PHẦN 4: LÀM SẠCH CONTROL (Log & Cấu hình) - MỚI
   ------------------------------------------------------ */
USE control;

-- 1. Xóa sạch lịch sử chạy (Log)
TRUNCATE TABLE Etl_Log;

-- 2. Reset cấu hình (Config)
TRUNCATE TABLE Etl_Config;

-- QUAN TRỌNG: Sau khi xóa Config, phải nạp lại dòng mặc định ngay
-- Nếu không có dòng này, Scheduler sẽ bị lỗi vì không tìm thấy giờ chạy
INSERT INTO Etl_Config (ConfigKey, ConfigValue, Description) 
VALUES ('daily_scrape_time', '10:00', 'Thời gian chạy cào dữ liệu hàng ngày');