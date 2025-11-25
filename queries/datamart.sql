USE datamart;

CREATE TABLE IF NOT EXISTS Agg_LaptopSummary (
    SummaryID INT AUTO_INCREMENT PRIMARY KEY,
    DateKey INT,               
    BrandName VARCHAR(100),
    TotalProducts INT,
    AvgPrice DECIMAL(18, 2),
    MinPrice DECIMAL(18, 2),
    MaxPrice DECIMAL(18, 2),
    AvgRAM FLOAT,
    AvgStorage FLOAT,
    
    LastRefreshed DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Khóa duy nhất để lệnh UPSERT hoạt động
    UNIQUE KEY agg_key (DateKey, BrandName)
);