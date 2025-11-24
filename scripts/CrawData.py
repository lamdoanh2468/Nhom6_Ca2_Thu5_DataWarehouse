import os
import time
import random
import pandas as pd
from datetime import datetime
from playwright.sync_api import sync_playwright
from db_connector import log_etl  # Hàm ghi log chung

def crawl_data_from_source():
    process_name = "Crawl_Data_Process"
    log_etl(process_name, "Running", "Bắt đầu khởi động trình duyệt để cào dữ liệu...")
    
    data_list = []
    
    try:
        with sync_playwright() as p:
            # 1. Mở trình duyệt (headless=True để chạy ngầm, False để hiện lên xem)
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            url = "https://cellphones.com.vn/laptop.html"
            print(f"🕷️ [Crawl] Đang truy cập: {url}")
            page.goto(url, timeout=60000)
            
            # 2. Cuộn trang để tải thêm sản phẩm (Lazy load)
            for _ in range(5):  # Cuộn 5 lần, tăng lên nếu muốn lấy nhiều hơn
                page.mouse.wheel(0, 5000)
                time.sleep(2)
            
            # 3. Lấy danh sách thẻ sản phẩm
            product_items = page.locator(".product-info-container")
            count = product_items.count()
            print(f"🔎 [Crawl] Tìm thấy {count} sản phẩm trên trang danh sách.")
            
            if count == 0:
                log_etl(process_name, "Warning", "Không tìm thấy sản phẩm nào trên trang web.")
                browser.close()
                return

            # 4. Duyệt qua từng sản phẩm để lấy thông tin sơ bộ & Link
            links_to_scrape = []
            for i in range(min(count, 5)):  # Lấy thử 20 sản phẩm đầu tiên để test
                try:
                    item = product_items.nth(i)
                    name = item.locator(".product__name h3").inner_text()
                    
                    # Xử lý giá (có thể có khuyến mãi hoặc không)
                    price_locator = item.locator(".product__price--show")
                    if price_locator.count() > 0:
                        price = price_locator.inner_text().replace('₫', '').replace('.', '').strip()
                    else:
                        price = "0"
                        
                    link_href = item.locator("a").get_attribute("href")
                    
                    if link_href:
                        links_to_scrape.append({
                            "Name": name,
                            "Price": price,
                            "links_href": link_href
                        })
                except Exception as e:
                    print(f"⚠️ Lỗi lấy item {i}: {e}")
                    continue
            
            print(f"🚀 [Crawl] Bắt đầu vào chi tiết {len(links_to_scrape)} sản phẩm...")

            # 5. Vào từng trang chi tiết để lấy thông số kỹ thuật
            for product in links_to_scrape:
                try:
                    print(f"   -> Đang xem: {product['Name']}...")
                    page.goto(product['links_href'], timeout=60000)
                    time.sleep(1) # Nghỉ xíu để tránh bị chặn
                    
                    # Lấy bảng thông số kỹ thuật (Technical Specs)
                    # Lưu ý: Selector này có thể thay đổi tùy giao diện web thực tế
                    # Đây là ví dụ logic, bạn cần F12 trên web để check selector chính xác
                    
                    # Hàm phụ trợ lấy text an toàn
                    def get_text(selector):
                        if page.locator(selector).count() > 0:
                            return page.locator(selector).first.inner_text().strip()
                        return ""

                    # Map dữ liệu (Selector mẫu - Cần điều chỉnh theo thực tế CellphoneS)
                    product["CpuType"] = get_text("text=Loại CPU >> xpath=../following-sibling::div")
                    product["Ram"] = get_text("text=Dung lượng RAM >> xpath=../following-sibling::div")
                    product["Storage"] = get_text("text=Ổ cứng >> xpath=../following-sibling::div")
                    product["Display"] = get_text("text=Kích thước màn hình >> xpath=../following-sibling::div")
                    product["GPU"] = get_text("text=Card đồ họa >> xpath=../following-sibling::div")
                    product["OSystem"] = get_text("text=Hệ điều hành >> xpath=../following-sibling::div")
                    product["Battery"] = get_text("text=Pin >> xpath=../following-sibling::div")
                    product["Resolution"] = get_text("text=Độ phân giải màn hình >> xpath=../following-sibling::div")
                    
                    data_list.append(product)
                    
                except Exception as e:
                    print(f"❌ Lỗi chi tiết sản phẩm: {e}")
            
            browser.close()

        # 6. Lưu dữ liệu ra File CSV
        if not data_list:
            log_etl(process_name, "Warning", "Kết thúc nhưng không thu thập được dữ liệu nào.")
            return

        df = pd.DataFrame(data_list)
        
        # Tạo đường dẫn: D:/LaptopDW/data/raw/
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        raw_path = os.path.join(base_dir, 'data', 'raw')
        
        if not os.path.exists(raw_path):
            os.makedirs(raw_path)

        # Tên file: laptop_YYYYMMDD_HHMMSS.csv
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"laptop_{timestamp}.csv"
        full_path = os.path.join(raw_path, filename)

        df.to_csv(full_path, index=False, encoding='utf-8-sig')
        
        msg = f"Đã lưu {len(df)} dòng vào file: {filename}"
        print(f"✅ {msg}")
        log_etl(process_name, "Success", msg, len(df))

    except Exception as e:
        print(f"🔥 Lỗi Fatal Crawl: {e}")
        log_etl(process_name, "Failed", str(e))

if __name__ == "__main__":
    crawl_data_from_source()