USE staging;

-- Xóa bảng cũ chứa cột rác
DROP TABLE IF EXISTS stg_laptops;

-- Tạo bảng mới CHỈ CHỨA dữ liệu Python cào về
CREATE TABLE stg_laptops (
    StagingID INT AUTO_INCREMENT PRIMARY KEY,
    Name TEXT,
    Price VARCHAR(255),
    links_href TEXT,       -- Link sản phẩm (Key định danh)
    CpuType VARCHAR(255),
    Ram VARCHAR(255),
    Storage VARCHAR(255),
    Display TEXT,
    GPU TEXT,
    OSystem VARCHAR(255),
    Battery VARCHAR(255),
    Resolution VARCHAR(255),
    ScrapeTimestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);