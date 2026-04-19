import os
import time
from datetime import datetime, timedelta # เพิ่ม timedelta สำหรับคำนวณย้อนหลัง
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from supabase import create_client, Client

def format_date_to_iso(date_str):
    try:
        clean_date = date_str.replace('(', '').replace(')', '').strip()
        return datetime.strptime(clean_date, '%d/%m/%Y') # คืนค่าเป็น object เพื่อนำไปเทียบวันที่ง่ายขึ้น
    except:
        return None

def clean_price(price_str):
    try:
        # 1. ทำความสะอาดและแปลงเป็นตัวเลขก่อน
        price = float(str(price_str).replace(',', '').strip())
        
        # 2. เช็คเงื่อนไขการแสดงผล
        if price % 1 == 0:
            return str(int(price))    # เลขกลม -> "10"
        else:
            return f"{price:.2f}"     # มีเศษ -> "10.50"
            
    except (ValueError, AttributeError):
        return "0"



def scrape_market(driver, market_id):
    url = f"https://talaadthai.com/products?market={market_id}"
    #https://talaadthai.com/products?market=12
    driver.get(url)  
    time.sleep(5) # รอสรุปผลรอบสุดท้าย

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    products = soup.find_all('div', class_='out-div-one')
    
    market_data = []
    three_months_ago = datetime.now() - timedelta(days=90)
    for item in products:
        try:
            # ดึงชื่อสินค้า
            name = item.find('div', class_='productName').get_text(strip=True)
            
            # ดึง Location และจัดการค่าว่าง
            location_raw = item.find('div', class_='location').get_text(strip=True)
            if not location_raw:
                location_raw = "อื่นๆ"

            # แยก Location ด้วยเครื่องหมาย "," (ถ้ามี)
            # เช่น "ข้าวสารและสินค้าอุปโภคบริโภค, ตลาดข้าวสาร" -> ["ข้าวสารและสินค้าอุปโภคบริโภค", "ตลาดข้าวสาร"]
            locations = [loc.strip() for loc in location_raw.split(',')]

            min_p_raw = item.find('div', class_='minPrice').get_text(strip=True)
            max_p_raw = item.find('div', class_='maxPrice').get_text(strip=True)
            unit = item.find('div', class_='unit').get_text(strip=True)
            trend = item.find('div', class_='tag-children').get_text(strip=True)
            raw_date = item.find('div', class_='updateDate').get_text(strip=True)

            dt_object = format_date_to_iso(raw_date)
            min_p = clean_price(min_p_raw)
            max_p = clean_price(max_p_raw)
            
            # กรองวันที่และราคา
            if dt_object and dt_object >= three_months_ago and min_p > 0:
                # วนลูปสร้าง record ตามจำนวน location ที่แยกได้
                for loc in locations:
                    market_data.append({
                        "product_name": name,
                        "location": loc, # ใส่ location ที่แยกแล้ว
                        "price_range": f"{min_p}-{max_p}",
                        "min_price": min_p,
                        "max_price": max_p,
                        "unit": unit,
                        "trend": trend,
                        "update_date": dt_object.strftime('%Y-%m-%d')
                    })
        except Exception:
            continue
    return market_data




def run_all_markets():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080") # กำหนดขนาดหน้าจอให้ใหญ่เพื่อให้เห็นสินค้าครบ
    
    driver = webdriver.Chrome(options=chrome_options)
    all_results = []

    print("🚀 Starting Multi-Market Scraper (1-35)...")
    
    for m_id in range(1, 36):
        print(f"📡 Scraping Market ID: {m_id}...")
        data = scrape_market(driver, m_id)
        if data:
            all_results.extend(data)
            print(f"✅ Found {len(data)} items (filtered by date).")
        else:
            print(f"❌ No recent items in Market {m_id}, skipping.")
            
    driver.quit()
    return all_results

def upload_to_supabase(data):
    if not data: return
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    supabase: Client = create_client(url, key)
    
    # กรองตัวซ้ำโดยใช้ชื่อสินค้า + สถานที่ + หน่วย + วันที่
    unique_items = {}
    for item in data:
        key_id = (item['product_name'], item['location'], item['unit'], item['update_date'])
        unique_items[key_id] = item 
    
    clean_data = list(unique_items.values())
    print(f"🧹 Data Summary: Scraped {len(data)} -> Unique {len(clean_data)} items")

    for i in range(0, len(clean_data), 100):
        batch = clean_data[i:i+100]
        try:
            # ใช้ on_conflict ครอบคลุมฟิลด์ที่ทำให้ข้อมูลแตกต่างกัน
            supabase.table("talaadthai_prices").upsert(
                batch, on_conflict="product_name, location, unit, update_date"
            ).execute()
        except Exception as e:
            print(f"⚠️ Batch Error: {e}")

if __name__ == "__main__":
    final_data = run_all_markets()
    print(f"📊 Total recent items: {len(final_data)}")
    upload_to_supabase(final_data)
    print("🏁 All done!")
