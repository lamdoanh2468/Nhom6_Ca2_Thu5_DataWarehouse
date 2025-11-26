/*TẠO STORED PROCEDURE sp_Load_Agg_LaptopSummary*/
USE datamart;

DELIMITER //
CREATE PROCEDURE sp_Load_Agg_LaptopSummary(
    IN p_DateKey INT,
    IN p_BrandName VARCHAR(100),
    IN p_TotalProducts INT,
    IN p_AvgPrice DECIMAL(15,2),
    IN p_MinPrice DECIMAL(15,2),
    IN p_MaxPrice DECIMAL(15,2),
    IN p_AvgRAM DECIMAL(15,2),
    IN p_AvgStorage DECIMAL(15,2)
)
BEGIN
    INSERT INTO Agg_LaptopSummary (
        DateKey, BrandName, TotalProducts, AvgPrice,
        MinPrice, MaxPrice, AvgRAM, AvgStorage
    )
    VALUES (
        p_DateKey, p_BrandName, p_TotalProducts, p_AvgPrice,
        p_MinPrice, p_MaxPrice, p_AvgRAM, p_AvgStorage
    )
    ON DUPLICATE KEY UPDATE
        TotalProducts = VALUES(TotalProducts),
        AvgPrice = VALUES(AvgPrice),
        MinPrice = VALUES(MinPrice),
        MaxPrice = VALUES(MaxPrice),
        AvgRAM = VALUES(AvgRAM),
        AvgStorage = VALUES(AvgStorage);
END //
DELIMITER ;

/* TẠO VIEW View_Agg_Laptop_Daily*/
USE dw_laptop;

CREATE OR REPLACE VIEW View_Agg_Laptop_Daily AS
SELECT 
    f.DateKey,
    b.brand_name AS BrandName, 
    COUNT(f.laptop_id) as TotalProducts,
    AVG(f.price) as AvgPrice,
    MIN(f.price) as MinPrice,
    MAX(f.price) as MaxPrice,
    AVG(f.ram_storage_gb) as AvgRAM,
    AVG(f.storage_capacity_gb) as AvgStorage
FROM fact_laptop f
JOIN dim_brand b ON f.brand_id = b.brand_id
GROUP BY f.DateKey, b.brand_name;
