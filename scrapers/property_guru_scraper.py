from bs4 import BeautifulSoup
import time
from datetime import date
import cloudscraper
import pandas as pd
import os
import re
import json

# Default Query parameter
STATE = 'D02'

state = {
    "D01": "&districtCode=D01",  # Boat Quay / Raffles Place / Marina
    "D02": "&districtCode=D02",  # Chinatown / Tanjong Pagar
    "D03": "&districtCode=D03",  # Queenstown / Tiong Bahru
    "D04": "&districtCode=D04",  # Telok Blangah /
    "D05": "&districtCode=D05",  # Pasir Panjang / Clementi
    "D06": "&districtCode=D06",  # City Hall / Clarke Quay
    "D07": "&districtCode=D07",  # Bugis / Kampong Glam
    "D08": "&districtCode=D08",  # Little India
    "D09": "&districtCode=D09",  # Orchard / River Valley
    "D10": "&districtCode=D10",  # Ardmore / Bukit Timah
    "D11": "&districtCode=D11",  # Newton / Novena
    "D12": "&districtCode=D12",  # Balestier / Toa Payoh
    "D13": "&districtCode=D13",  # Macpherson / Braddell
    "D14": "&districtCode=D14",  # Eunos / Geylang
    "D15": "&districtCode=D15",  # Katong / Joo Chiat / Amber Road
    "D16": "&districtCode=D16",  # Bedok / Upper East Coast
    "D17": "&districtCode=D17",  # Eastwood / Kew Drive
    "D18": "&districtCode=D18",  # Tampines / Pasir Ris
    "D19": "&districtCode=D19",  # Hougang / Punggol / Sengkang
    "D20": "&districtCode=D20",  # Ang Mo Kio / Bishan / Thomson
    "D21": "&districtCode=D21",  # Serangoon / Upper Paya Lebar
    "D22": "&districtCode=D22",  # Woodlands / Admiralty
    "D23": "&districtCode=D23",  # Yishun / Sembawang
    "D24": "&districtCode=D24",  # Lim Chu Kang / Tengah
    "D25": "&districtCode=D25",  # Choa Chu Kang / Bukit Panjang
    "D26": "&districtCode=D26",  # Hillview / Dairy Farm
    "D27": "&districtCode=D27",  # Bukit Batok / Bukit Gombak
    "D28": "&districtCode=D28",  # Seletar
}

def BSPrep(URL):
    exitcode = 1
    while exitcode == 1:
        try:
            trial = 0
            while trial < 10:
                scraper = cloudscraper.create_scraper(
                    browser={
                        'browser': 'chrome',
                        'platform': 'windows',
                        'mobile': False
                    },
                    delay=5
                )
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Cache-Control': 'max-age=0',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Upgrade-Insecure-Requests': '1'
                }
            
                s = scraper.get(URL, headers=headers)
                time.sleep(10)  # Wait for JavaScript content to load
                
                print(f"Status Code: {s.status_code}")
                
                soup = BeautifulSoup(s.content, 'html.parser')
                
                # Check for various blocking scenarios
                if "captcha" in soup.text.lower():
                    print("Hit CAPTCHA page")
                    trial += 1
                    time.sleep(5)
                    continue
                elif "verify you are a human" in soup.text.lower():
                    print("Hit human verification page")
                    trial += 1
                    time.sleep(5)
                    continue
                elif "access denied" in soup.text.lower():
                    print("Access denied")
                    trial += 1
                    time.sleep(5)
                    continue
                
                # Look for article tags or listing container
                listings = soup.find_all('div', {'data-listing-id': True}) or \
                    soup.find_all('div', class_=lambda x: x and ('listing' in x.lower() or 'property' in x.lower()))
                
                if listings:
                    print(f"Found {len(listings)} listings")
                    return soup
                
                print("No listings found with any selector")
                trial += 1
                time.sleep(3)
                continue
                
            if trial >= 10:
                print('Max retries exceeded')
                return None
                
            exitcode = 0
            
        except Exception as e:
            print(f'Error: {str(e)}')
            print('Connection reset, retrying in 1 minute...', flush=True)
            time.sleep(60)
    
    return None

def extract_from_json(listing_data):
    """Extract property information from JSON data"""
    try:
        mrt_info = listing_data.get('mrt', {}).get('nearbyText', '')
        mrt_station = None
        mrt_distance = None
        
        if mrt_info:
            # Extract MRT station name and distance
            mrt_match = re.match(r'(\d+)\s*min\s*\((\d+)\s*m\)\s*from\s*(.+)', mrt_info)
            if mrt_match:
                mrt_distance = int(mrt_match.group(2))
                mrt_station = mrt_match.group(3).strip()

        url = listing_data['url']

        return {
            'ListID': listing_data['id'],
            'PropertyName': listing_data['localizedTitle'],
            'Type': 'Sale',
            'Price': float(listing_data['price']['value']),
            'Bedrooms': listing_data.get('bedrooms'),
            'Bathrooms': listing_data.get('bathrooms'),
            'Sqft': listing_data.get('floorArea'),
            'Author': listing_data.get('agent', {}).get('name'),
            'ListDate': listing_data.get('postedOn', {}).get('text'),
            'Address': listing_data['fullAddress'],
            'NearestMRT': mrt_station,
            'DistanceToMRT': mrt_distance,
            'PricePerSqft': float(re.sub(r'[^\d.]', '', listing_data.get('pricePerArea', {}).get('localeStringValue', '0'))) if listing_data.get('pricePerArea') else None,
            'URL': url
        }

    except Exception as e:
        print(f"Error extracting listing data: {str(e)}")
        return None

def scrape_district(district_code, max_pages=50):
    """Scrape properties from a district"""
    all_properties = []
    
    for page in range(1, max_pages + 1):
        try:
            url = (f"{HEADER}{KEY}?listingType=sale"
                  f"&page={page}"
                  f"&propertyTypeGroup=N"
                  f"&propertyTypeCode=CONDO"
                  f"&isCommercial=false"
                  f"&districtCode={district_code}")
            
            print(f"\nProcessing district {district_code}, page {page}")
            
            soup = BSPrep(url)
            if not soup:
                print(f"Failed to get page {page}, skipping...")
                continue

            # Find and parse JSON data
            json_script = soup.find('script', id='__NEXT_DATA__')
            if not json_script:
                print("No JSON data found")
                continue

            data = json.loads(json_script.string)
            listings = data['props']['pageProps']['pageData']['data']['listingsData']
            
            if not listings or len(listings) < 20:
                print(f"Found {len(listings)} listings (less than 20). Finishing scraping.")
                
                # Process remaining listings before breaking
                if listings:
                    for listing in listings:
                        listing_data = extract_from_json(listing['listingData'])
                        if listing_data:
                            all_properties.append(listing_data)
                break

            # Process each listing
            for listing in listings:
                listing_data = extract_from_json(listing['listingData'])
                if listing_data:
                    all_properties.append(listing_data)

            time.sleep(5)  # Delay between pages

        except Exception as e:
            print(f"Error processing page {page}: {str(e)}")
            print("Saving progress and retrying in 1 minute...")
            save_progress(district_code, all_properties, page)
            time.sleep(60)
            continue

    return all_properties

def save_progress(district_code, data, page):
    os.makedirs(LIST_DIR, exist_ok=True)
    progress_file = f'{LIST_DIR}/condo-sales-{district_code}-progress-{date.today().strftime("%b%Y")}.csv'
    
    df = pd.DataFrame(data)
    df.to_csv(progress_file, index=False)
    print(f"Progress saved: {len(data)} properties (up to page {page})")


def main():
    print('\n===================================================')
    print('PropertyGuru Property Listing Scraper')
    print('Author: amlukito')
    print('===================================================\n')

    all_data = []
        
    print(f"\nStarting district {district_code}")
    district_data = scrape_district(district_code)
    all_data.extend(district_data)
    print(f"Completed district {district_code}. Found {len(district_data)} properties")
    time.sleep(10)  # Delay between districts

    # Save results
    if all_data:
        df = pd.DataFrame(all_data)
        os.makedirs(LIST_DIR, exist_ok=True)
        df.to_csv(RAW_LISTING, index=False)
        print(f"\nSaved {len(df)} properties to {RAW_LISTING}")
        print("\nSample data:")
        print(df.head())
        print("\nDataset Statistics:")
        print(df.describe())

if __name__ == "__main__":

    for district_num in range(27, 29):
        # Initialize basic configurations
        # district_code = "D08"  
        district_code = f"D{district_num:02d}"
        LIST_DIR = f'./data/{date.today().strftime("%b%Y")}'
        RAW_LISTING = f'{LIST_DIR}/condo-sales-{district_num}-{date.today().strftime("%b%Y")}.csv'
        HEADER = 'https://www.propertyguru.com.sg'
        KEY = '/property-for-sale'
        
        main()