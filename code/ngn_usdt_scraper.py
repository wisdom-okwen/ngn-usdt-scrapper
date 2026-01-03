"""
NGN/USDT Daily Exchange Rate Generator
Generates daily rates from 2000 to present using interpolation
"""

import requests
import json
import csv
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"

HEADERS = {"User-Agent": "Mozilla/5.0"}

# Historical parallel market rates (better reflects P2P than official rates)
PARALLEL_MARKET_RATES = {
    "2020-01-01": 362, "2020-03-01": 380, "2020-06-01": 450, "2020-12-01": 480,
    "2021-01-01": 475, "2021-06-01": 502, "2021-12-01": 565,
    "2022-01-01": 570, "2022-06-01": 615, "2022-12-01": 755,
    "2023-01-01": 755, "2023-06-01": 760, "2023-06-15": 770, "2023-07-01": 820,
    "2023-08-01": 900, "2023-09-01": 940, "2023-10-01": 1050,
    "2023-11-01": 1180, "2023-12-01": 1150,
    "2024-01-01": 1350, "2024-02-01": 1550, "2024-03-01": 1600,
}


def fetch_world_bank_rates():
    """Fetch yearly USD/NGN rates from World Bank API"""
    url = "https://api.worldbank.org/v2/country/NGA/indicator/PA.NUS.FCRF"
    params = {"format": "json", "per_page": 100, "date": "2000:2026"}
    response = requests.get(url, params=params, headers=HEADERS, timeout=30)
    data = response.json()
    return {f"{item['date']}-01-01": item["value"] 
            for item in data[1] if item.get("value")}


def interpolate_daily(known_rates, start_date, end_date):
    """Generate daily rates using linear interpolation"""
    sorted_dates = sorted(known_rates.keys())
    daily = []
    current = start_date
    
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        
        if date_str in known_rates:
            daily.append({"date": date_str, "rate": round(known_rates[date_str], 2)})
        else:
            prev_date = next_date = None
            for d in sorted_dates:
                if d < date_str: prev_date = d
                elif d > date_str: next_date = d; break
            
            if prev_date and next_date:
                prev_dt = datetime.strptime(prev_date, "%Y-%m-%d")
                next_dt = datetime.strptime(next_date, "%Y-%m-%d")
                t = (current - prev_dt).days / (next_dt - prev_dt).days
                rate = known_rates[prev_date] + (known_rates[next_date] - known_rates[prev_date]) * t
                daily.append({"date": date_str, "rate": round(rate, 2)})
            elif prev_date:
                daily.append({"date": date_str, "rate": round(known_rates[prev_date], 2)})
        
        current += timedelta(days=1)
    return daily


def save_data(data, filename):
    """Save data to CSV and JSON"""
    with open(DATA_DIR / f"{filename}.json", 'w') as f:
        json.dump(data, f, indent=2)
    with open(DATA_DIR / f"{filename}.csv", 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["date", "rate"])
        writer.writeheader()
        writer.writerows(data)


def generate_daily_rates():
    """Generate daily NGN/USDT rates from 2000 to now"""
    # Combine all data sources
    known_rates = fetch_world_bank_rates()
    known_rates.update(PARALLEL_MARKET_RATES)
    
    # Generate daily rates
    start = datetime(2000, 1, 1)
    end = datetime.now()
    daily_rates = interpolate_daily(known_rates, start, end)
    
    # Save
    save_data(daily_rates, f"ngn_usdt_daily_{start.year}_{end.year}")
    print(f"✅ Generated {len(daily_rates):,} daily rates ({daily_rates[0]['date']} to {daily_rates[-1]['date']})")
    return daily_rates


if __name__ == "__main__":
    generate_daily_rates()
