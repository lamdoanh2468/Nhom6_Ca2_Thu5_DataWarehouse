USE dw_laptop;

CREATE TABLE IF NOT EXISTS Dim_Date (
    DateKey INT PRIMARY KEY, 
    FullDate DATE NOT NULL,
    DayOfMonth INT NOT NULL,
    Month INT NOT NULL,
    MonthName VARCHAR(20) NOT NULL,
    Year INT NOT NULL
);

CREATE TABLE IF NOT EXISTS Dim_Brand (
    brand_id INT AUTO_INCREMENT PRIMARY KEY,
    brand_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS Dim_CPU (
    cpu_id INT AUTO_INCREMENT PRIMARY KEY,
    cpu_type TEXT
);

CREATE TABLE IF NOT EXISTS Dim_Laptop (
    laptop_id INT AUTO_INCREMENT PRIMARY KEY,
    name TEXT NOT NULL,
    ram_storage VARCHAR(50),      
    storage_capacity VARCHAR(50),  
    display VARCHAR(255),
    resolution VARCHAR(255),
    os VARCHAR(255),
    battery VARCHAR(255),
    gpu TEXT,
    link VARCHAR(255),             
    UNIQUE KEY unique_link (link)  
);

CREATE TABLE IF NOT EXISTS Fact_Laptop (
    FactID INT AUTO_INCREMENT PRIMARY KEY,
    DateKey INT NOT NULL,
    laptop_id INT NOT NULL,
    brand_id INT NOT NULL,
    cpu_id INT NOT NULL,
    price DECIMAL(18, 2),
    ram_storage_gb FLOAT,      
    storage_capacity_gb FLOAT, 
    
    FOREIGN KEY (DateKey) REFERENCES Dim_Date(DateKey),
    FOREIGN KEY (laptop_id) REFERENCES Dim_Laptop(laptop_id),
    FOREIGN KEY (brand_id) REFERENCES Dim_Brand(brand_id),
    FOREIGN KEY (cpu_id) REFERENCES Dim_CPU(cpu_id)
);