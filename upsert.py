import os
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from supabase import create_client, Client

def format_date_to_iso(date_str):
    """แปลงจาก '17/04/2026' เป็น '2026-04-17'"""
    try:
        # ล้างช่องว่างและตัวอักษรที่ไม่เกี่ยวข้อง
        clean_date = date_str.strip()
        # แปลงจาก วัน/เดือน/ปี(ค.ศ.) เป็น Object และจัด format ใหม่
        return datetime.strptime(clean_date, '%d/%m/%Y').strftime('%Y-%m-%d')
    except Exception as e:
        print(f"⚠️ Error formatting date {date_str}: {e}")
        return None

def get_talaadthai_prices():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(options=chrome_options)
    url = "https://talaadthai.com/products?trending=today"
    
    try:
        driver.get(url)
        time.sleep(10) # ลดลงเหลือ 10-15 วินาทีมักจะเพียงพอสำหรับ headless
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        products = soup.find_all('div', class_='out-div-one')
        
        data_list = []
        for item in products:
            try:
                name = item.find('div', class_='productName').get_text(strip=True)
                location = item.find('div', class_='location').get_text(strip=True)
                min_price = item.find('div', class_='minPrice').get_text(strip=True)
                max_price = item.find('div', class_='maxPrice').get_text(strip=True)
                unit = item.find('div', class_='unit').get_text(strip=True)
                trend = item.find('div', class_='tag-children').get_text(strip=True)
                raw_date = item.find('div', class_='updateDate').get_text(strip=True).replace('(', '').replace(')', '')
                
                # แปลงวันที่ที่นี่
                iso_date = format_date_to_iso(raw_date)
                
                if iso_date:
                    data_list.append({
                        "product_name": name,
                        "location": location,
                        "price_range": f"{min_price}-{max_price}",
                        "unit": unit,
                        "trend": trend,
                        "update_date": iso_date # ส่งวันที่ที่เป็นสากลไป
                    })
            except AttributeError:
                continue # ข้ามรายการที่ดึงข้อมูลไม่ครบ
                
        return data_list
    finally:
        driver.quit()

def upload_to_supabase(data):
    if not data:
        print("No data to upload.")
        return

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    supabase: Client = create_client(url, key)

    try:
        # ใช้ upsert โดยอิงจาก Unique Index (product_name, update_date)
        response = supabase.table("talaadthai_prices").upsert(
            data,
            on_conflict="product_name, update_date"
        ).execute()
        print(f"✅ Successfully uploaded/updated {len(data)} items to Supabase.")
    except Exception as e:
        print(f"❌ Supabase Error: {e}")

if __name__ == "__main__":
    print("🚀 Starting Scraper...")
    scraped_data = get_talaadthai_prices()
    print(f"📦 Scraped {len(scraped_data)} items.")
    upload_to_supabase(scraped_data)
