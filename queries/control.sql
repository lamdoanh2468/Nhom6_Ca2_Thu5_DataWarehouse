USE control;

-- 2. Tạo bảng lưu cấu hình (Etl_Config)
CREATE TABLE IF NOT EXISTS Etl_Config (
    ConfigKey VARCHAR(50) PRIMARY KEY,
    ConfigValue VARCHAR(255) NOT NULL,
    Description TEXT,
    LastUpdated DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 3. Tạo bảng lưu nhật ký (Etl_Log)
CREATE TABLE IF NOT EXISTS Etl_Log (
    LogID INT AUTO_INCREMENT PRIMARY KEY,
    ProcessName VARCHAR(100) NOT NULL,
    StartTime DATETIME,
    EndTime DATETIME,
    Status VARCHAR(20) NOT NULL, -- 'Running', 'Success', 'Failed'
    Message TEXT,
    RowsAffected INT DEFAULT 0
);

-- 4. Nạp giờ chạy mặc định (10:00 sáng)
INSERT INTO Etl_Config (ConfigKey, ConfigValue, Description) 
VALUES ('daily_scrape_time', '10:00', 'Thời gian chạy cào dữ liệu hàng ngày')
ON DUPLICATE KEY UPDATE ConfigValue = '10:00';