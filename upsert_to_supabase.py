import os
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from supabase import create_client, Client

def format_date_to_iso(date_str):
    try:
        clean_date = date_str.replace('(', '').replace(')', '').strip()
        return datetime.strptime(clean_date, '%d/%m/%Y')
    except:
        return None

def clean_price(price_str):
    try:
        return float(str(price_str).replace(',', '').strip())
    except:
        return 0.0

def scrape_market(driver, market_id):
    url = f"https://talaadthai.com/products?market={market_id}"
    driver.get(url)  
    
    # --- เพิ่มการ Scroll เพื่อดึงข้อมูลให้ครบ ---
    for _ in range(3):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    products = soup.find_all('div', class_='out-div-one')
    
    market_data = []
    three_months_ago = datetime.now() - timedelta(days=90)
    
    for item in products:
        try:
            name = item.find('div', class_='productName').get_text(strip=True)
            location_raw = item.find('div', class_='location').get_text(strip=True) or "อื่นๆ"
            
            # แยก Location ถ้ามีคอมม่า
            locations = [loc.strip() for loc in location_raw.split(',')]

            min_p_raw = item.find('div', class_='minPrice').get_text(strip=True)
            max_p_raw = item.find('div', class_='maxPrice').get_text(strip=True)
            unit = item.find('div', class_='unit').get_text(strip=True)
            trend = item.find('div', class_='tag-children').get_text(strip=True)
            raw_date = item.find('div', class_='updateDate').get_text(strip=True)

            dt_object = format_date_to_iso(raw_date)
            min_p = clean_price(min_p_raw)
            max_p = clean_price(max_p_raw)
            
            # กรองวันที่ใหม่ และ ราคาต้องไม่ใช่ 0
            if dt_object and dt_object >= three_months_ago and min_p > 0:
                for loc in locations:
                    market_data.append({
                        "product_name": name,
                        "location": loc,
                        "price_range": f"{min_p:g}-{max_p:g}", 
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
    chrome_options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=chrome_options)
    all_results = []

    print("🚀 Starting Multi-Market Scraper (1-35)...")
    for m_id in range(1, 36):
        data = scrape_market(driver, m_id)
        if data:
            all_results.extend(data)
            print(f"📡 Market {m_id}: Found {len(data)} items.")
        else:
            print(f"❌ Market {m_id}: No recent/valid data.")
            
    driver.quit()
    return all_results
    
def upload_to_supabase(data):
    if not data: 
        print("💡 No data to upload.")
        return
        
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    supabase: Client = create_client(url, key)
    
    # กรองตัวซ้ำในลิสต์ (ป้องกัน Error 21000)
    unique_items = {}
    for item in data:
        # ต้องใช้ key เดียวกับ Unique Constraint ใน DB
        key_id = (item['product_name'], item['location'], item['unit'], item['update_date'])
        unique_items[key_id] = item 
    
    clean_data = list(unique_items.values())
    print(f"🧹 Summary: Scraped {len(data)} -> Unique {len(clean_data)} items")

    for i in range(0, len(clean_data), 50):
        batch = clean_data[i:i+50]
        try:
            # ใช้ on_conflict ตามที่เราเซ็ตไว้ใน SQL
            supabase.table("talaadthai_prices").upsert(
                batch, on_conflict="product_name, location, unit, update_date"
            ).execute()
            print(f"✅ Batch {i//50 + 1} uploaded successfully.")
        except Exception as e:
            print(f"⚠️ Batch Error: {e}")


if __name__ == "__main__":
    final_data = run_all_markets()
    upload_to_supabase(final_data)
    print("🏁 All done!")
