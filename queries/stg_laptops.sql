USE staging;

CREATE TABLE IF NOT EXISTS  stg_laptops (
    StagingID INT AUTO_INCREMENT PRIMARY KEY,
    Name TEXT,
    Price VARCHAR(255),
    links_href TEXT,       
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