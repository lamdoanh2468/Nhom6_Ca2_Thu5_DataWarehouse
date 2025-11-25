import os
import time
import pandas as pd
from datetime import datetime
from playwright.sync_api import sync_playwright
# 👇 1. THÊM get_connection VÀO ĐÂY
from db_connector import log_etl, get_connection 

def crawl_data_from_source():
    process_name = "Crawl_Data_Process"
    
    # 👇 2. THÊM ĐOẠN CHECK KẾT NỐI NÀY
    print("🔌 Đang kiểm tra kết nối Database trước khi cào...")
    conn_check = get_connection('control')
    if not conn_check:
        print("❌ Lỗi: Không thể kết nối Database Control. Hủy bỏ việc cào dữ liệu.")
        return # Dừng ngay, không mở trình duyệt
    else:
        print("✅ Kết nối Database ổn định. Tiếp tục...")
        conn_check.close() # Đóng kết nối kiểm tra
    # ---------------------------------------------------------

    log_etl(process_name, "Running", "Bắt đầu khởi động trình duyệt để cào dữ liệu...")
    
    data_list = []
    
    try:
        with sync_playwright() as p:
            # 1. Mở trình duyệt
            browser = p.chromium.launch(headless=True) # Sửa thành False nếu muốn xem chạy
            page = browser.new_page()
            
            url = "https://cellphones.com.vn/laptop.html"
            print(f"🕷️ [Crawl] Đang truy cập: {url}")
            page.goto(url, timeout=60000)
            
            # 2. Cuộn trang
            for _ in range(5):
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

            # 4. Duyệt qua từng sản phẩm lấy Link
            links_to_scrape = []
            for i in range(min(count, 5)): 
                try:
                    item = product_items.nth(i)
                    name = item.locator(".product__name h3").inner_text()
                    
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

            # 5. Vào từng trang chi tiết
            for product in links_to_scrape:
                try:
                    print(f"   -> Đang xem: {product['Name']}...")
                    page.goto(product['links_href'], timeout=60000)
                    time.sleep(1)
                    
                    def get_text(selector):
                        if page.locator(selector).count() > 0:
                            return page.locator(selector).first.inner_text().strip()
                        return ""

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
        
        # Sử dụng os.getcwd() để đảm bảo đường dẫn đúng khi chạy từ Scheduler
        # Giả sử file CrawData.py nằm trong D:\LaptopDW\scripts -> Lùi 1 cấp để ra D:\LaptopDW
        # Tuy nhiên, Scheduler đã set thư mục làm việc là D:\LaptopDW rồi, nên ta dùng 'data/raw' trực tiếp
        
        raw_path = os.path.join(os.getcwd(), 'data', 'raw')
        
        if not os.path.exists(raw_path):
            os.makedirs(raw_path)

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