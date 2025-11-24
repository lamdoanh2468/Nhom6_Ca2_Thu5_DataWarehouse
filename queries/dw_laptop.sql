USE dw_laptop;

/* 1. Bảng Dim_Date */
CREATE TABLE IF NOT EXISTS Dim_Date (
    DateKey INT PRIMARY KEY, 
    FullDate DATE NOT NULL,
    DayOfMonth INT NOT NULL,
    Month INT NOT NULL,
    MonthName VARCHAR(20) NOT NULL,
    Year INT NOT NULL
);

/* 2. Bảng Dim_Brand */
CREATE TABLE IF NOT EXISTS Dim_Brand (
    brand_id INT AUTO_INCREMENT PRIMARY KEY,
    brand_name VARCHAR(100) NOT NULL UNIQUE
);

/* 3. Bảng Dim_CPU */
CREATE TABLE IF NOT EXISTS Dim_CPU (
    cpu_id INT AUTO_INCREMENT PRIMARY KEY,
    cpu_type TEXT
);

/* 4. Bảng Dim_Laptop (Đã chuẩn hóa khớp với Python LoadDataToDW.py) */
CREATE TABLE IF NOT EXISTS Dim_Laptop (
    laptop_id INT AUTO_INCREMENT PRIMARY KEY,
    name TEXT NOT NULL,
    ram_storage VARCHAR(50),       -- Khớp với Python
    storage_capacity VARCHAR(50),  -- Khớp với Python
    display VARCHAR(255),
    resolution VARCHAR(255),
    os VARCHAR(255),
    battery VARCHAR(255),
    gpu TEXT,
    link VARCHAR(255),             -- Dùng VARCHAR để đánh index được
    UNIQUE KEY unique_link (link)  -- Đảm bảo không trùng sản phẩm
);

/* 5. Bảng Fact_Laptop (Đã mở comment và chỉnh sửa kiểu dữ liệu) */
CREATE TABLE IF NOT EXISTS Fact_Laptop (
    FactID INT AUTO_INCREMENT PRIMARY KEY,
    DateKey INT NOT NULL,
    laptop_id INT NOT NULL,
    brand_id INT NOT NULL,
    cpu_id INT NOT NULL,
    
    -- Các chỉ số (Measures)
    price DECIMAL(18, 2),
    ram_storage_gb FLOAT,      -- Float để lưu dung lượng chính xác hơn
    storage_capacity_gb FLOAT, 
    
    -- Khóa ngoại
    FOREIGN KEY (DateKey) REFERENCES Dim_Date(DateKey),
    FOREIGN KEY (laptop_id) REFERENCES Dim_Laptop(laptop_id),
    FOREIGN KEY (brand_id) REFERENCES Dim_Brand(brand_id),
    FOREIGN KEY (cpu_id) REFERENCES Dim_CPU(cpu_id)
);