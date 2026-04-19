import os
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from supabase import create_client, Client

def format_date_to_iso(date_str):
    try:
        clean_date = date_str.replace('(', '').replace(')', '').strip()
        return datetime.strptime(clean_date, '%d/%m/%Y').strftime('%Y-%m-%d')
    except:
        return None

def scrape_market(driver, market_id):
    url = f"https://talaadthai.com/products?market={market_id}"
    driver.get(url)
    time.sleep(15)  # รอโหลดข้อมูลแต่ละตลาด
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    products = soup.find_all('div', class_='out-div-one')
    
    market_data = []
    for item in products:
        try:
            name = item.find('div', class_='productName').get_text(strip=True)
            location = item.find('div', class_='location').get_text(strip=True)
            min_p = item.find('div', class_='minPrice').get_text(strip=True)
            max_p = item.find('div', class_='maxPrice').get_text(strip=True)
            unit = item.find('div', class_='unit').get_text(strip=True)
            trend = item.find('div', class_='tag-children').get_text(strip=True)
            raw_date = item.find('div', class_='updateDate').get_text(strip=True)
            
            iso_date = format_date_to_iso(raw_date)
            if iso_date:
                market_data.append({
                    "product_name": name,
                    "location": location,
                    "price_range": f"{min_p}-{max_p}",
                    "min_price": float(min_p) if min_p else max_p, # เพิ่มฟิลด์แยก
                    "max_price": float(max_p) if max_p else min_p, # เพิ่มฟิลด์แยก
                    "unit": unit,
                    "trend": trend,
                    "update_date": iso_date
                })
        except Exception:
            continue
    return market_data

def run_all_markets():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(options=chrome_options)
    all_results = []

    print("🚀 Starting Multi-Market Scraper (1-35)...")
    
    for m_id in range(1, 36):
        print(f"📡 Scraping Market ID: {m_id}...")
        data = scrape_market(driver, m_id)
        if data:
            all_results.extend(data)
            print(f"✅ Found {len(data)} items.")
        else:
            print(f"❌ No items in Market {m_id}, skipping.")
            
    driver.quit()
    return all_results

def upload_to_supabase(data):
    if not data: return
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    supabase: Client = create_client(url, key)
    
    # แบ่งส่งทีละ 100 รายการเพื่อป้องกัน Request ใหญ่เกินไป
    for i in range(0, len(data), 100):
        batch = data[i:i+100]
        try:
            supabase.table("talaadthai_prices").upsert(
                batch, on_conflict="product_name, update_date"
            ).execute()
        except Exception as e:
            print(f"⚠️ Batch Error: {e}")

if __name__ == "__main__":
    final_data = run_all_markets()
    print(f"📊 Total items scraped: {len(final_data)}")
    upload_to_supabase(final_data)
    print("🏁 All done!")
