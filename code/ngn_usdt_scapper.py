"""
Historical Exchange Rate Scraper for NGN/USDT
Fetches historical data from 2000 to present using multiple sources:
- World Bank API for yearly official rates
- fawazahmed0/currency-api for daily data (2024+)
- Interpolation to generate daily estimates
"""

import requests
import json
import csv
import time
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}


def get_world_bank_historical():
    """
    Fetch historical USD/NGN rates from World Bank API
    Returns yearly official exchange rates from 2000+
    """
    print("📊 Fetching World Bank historical rates...")

    url = "https://api.worldbank.org/v2/country/NGA/indicator/PA.NUS.FCRF"
    params = {
        "format": "json",
        "per_page": 100,
        "date": "2000:2026"
    }

    try:
        response = requests.get(
            url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()

        rates = []
        if len(data) > 1 and data[1]:
            for item in data[1]:
                if item.get("value"):
                    rates.append({
                        "date": f"{item['date']}-01-01",
                        "year": int(item["date"]),
                        "usd_ngn": item["value"],
                        "usdt_ngn_estimate": item["value"],
                        "source": "World Bank",
                        "data_type": "yearly_official"
                    })
            print(f"   ✓ Got {len(rates)} years of data")
        return sorted(rates, key=lambda x: x["date"])
    except Exception as e:
        print(f"   ⚠️ Error: {e}")
        return []


def get_cbn_parallel_rates():
    """
    Add known parallel market rates from historical records
    These are approximate parallel/black market rates that better reflect P2P rates
    Source: Historical news/reports
    """
    # Historical parallel market rates (approximations based on reports)
    # P2P rates typically follow parallel market more than official rates
    parallel_rates = [
        {"date": "2020-01-01", "usd_ngn": 362, "note": "Pre-COVID parallel rate"},
        {"date": "2020-03-01", "usd_ngn": 380, "note": "COVID impact beginning"},
        {"date": "2020-06-01", "usd_ngn": 450,
            "note": "Post-lockdown devaluation"},
        {"date": "2020-12-01", "usd_ngn": 480, "note": "Year-end rate"},
        {"date": "2021-01-01", "usd_ngn": 475, "note": "Start of 2021"},
        {"date": "2021-06-01", "usd_ngn": 502, "note": "Mid-2021"},
        {"date": "2021-12-01", "usd_ngn": 565, "note": "End of 2021"},
        {"date": "2022-01-01", "usd_ngn": 570, "note": "Start of 2022"},
        {"date": "2022-06-01", "usd_ngn": 615, "note": "Mid-2022"},
        {"date": "2022-12-01", "usd_ngn": 755, "note": "End of 2022"},
        {"date": "2023-01-01", "usd_ngn": 755, "note": "Start of 2023"},
        {"date": "2023-06-01", "usd_ngn": 760, "note": "Pre-float June 2023"},
        {"date": "2023-06-15", "usd_ngn": 770, "note": "CBN float announcement"},
        {"date": "2023-07-01", "usd_ngn": 820, "note": "Post-float July"},
        {"date": "2023-08-01", "usd_ngn": 900, "note": "August 2023"},
        {"date": "2023-09-01", "usd_ngn": 940, "note": "September 2023"},
        {"date": "2023-10-01", "usd_ngn": 1050, "note": "October 2023"},
        {"date": "2023-11-01", "usd_ngn": 1180, "note": "November 2023"},
        {"date": "2023-12-01", "usd_ngn": 1150, "note": "December 2023"},
        {"date": "2024-01-01", "usd_ngn": 1350, "note": "January 2024"},
        {"date": "2024-02-01", "usd_ngn": 1550, "note": "February 2024 peak"},
        {"date": "2024-03-01", "usd_ngn": 1600, "note": "March 2024"},
    ]

    for rate in parallel_rates:
        rate["source"] = "Historical parallel market (estimated)"
        rate["data_type"] = "parallel_market"
        rate["usdt_ngn_estimate"] = rate["usd_ngn"]

    return parallel_rates


def get_historical_rates_fawaz(start_year=2020):
    """
    Use fawazahmed0/currency-api (free, reliable, daily data)
    Note: API may not have data for all dates, especially older ones
    """
    print(f"📊 Fetching daily rates from {start_year} (fawazahmed0 API)...")

    all_rates = []
    start_date = datetime(start_year, 1, 1)
    end_date = datetime.now()
    current = start_date

    # Sample every 7 days
    sample_interval = 7

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")

        urls = [
            f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date_str}/v1/currencies/usd.json",
            f"https://raw.githubusercontent.com/fawazahmed0/currency-api/1/{date_str}/currencies/usd.json"
        ]

        for url in urls:
            try:
                response = requests.get(url, headers=HEADERS, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    if "usd" in data and "ngn" in data["usd"]:
                        rate = data["usd"]["ngn"]
                        all_rates.append({
                            "date": date_str,
                            "usd_ngn": rate,
                            "usdt_ngn_estimate": rate,
                            "source": "fawazahmed0/currency-api",
                            "data_type": "daily_market"
                        })
                        break
            except Exception:
                continue

        current = current + timedelta(days=sample_interval)
        time.sleep(0.2)

    print(f"   ✓ Got {len(all_rates)} data points")
    return all_rates


def interpolate_daily_rates(data_points, start_date, end_date):
    """
    Generate daily rates by interpolating between known data points.
    Uses linear interpolation between consecutive known rates.
    """
    print("📊 Interpolating daily rates...")

    # Sort data points by date
    sorted_points = sorted(data_points, key=lambda x: x["date"])

    # Create a dict of date -> rate for quick lookup
    known_rates = {}
    for point in sorted_points:
        date_str = point["date"]
        # Use usdt_ngn_estimate or usd_ngn
        rate = point.get("usdt_ngn_estimate") or point.get("usd_ngn")
        if rate and date_str not in known_rates:
            known_rates[date_str] = {
                "rate": float(rate),
                "source": point.get("source", "unknown"),
                "data_type": point.get("data_type", "unknown")
            }

    # Generate daily dates
    daily_rates = []
    current = start_date
    known_dates = sorted(known_rates.keys())

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")

        if date_str in known_rates:
            # We have actual data for this date
            daily_rates.append({
                "date": date_str,
                "usd_ngn": known_rates[date_str]["rate"],
                "usdt_ngn_estimate": known_rates[date_str]["rate"],
                "source": known_rates[date_str]["source"],
                "data_type": known_rates[date_str]["data_type"],
                "interpolated": False
            })
        else:
            # Find surrounding known dates for interpolation
            prev_date = None
            next_date = None

            for kd in known_dates:
                if kd < date_str:
                    prev_date = kd
                elif kd > date_str and next_date is None:
                    next_date = kd
                    break

            if prev_date and next_date:
                # Linear interpolation
                prev_rate = known_rates[prev_date]["rate"]
                next_rate = known_rates[next_date]["rate"]

                prev_dt = datetime.strptime(prev_date, "%Y-%m-%d")
                next_dt = datetime.strptime(next_date, "%Y-%m-%d")

                total_days = (next_dt - prev_dt).days
                days_from_prev = (current - prev_dt).days

                # Linear interpolation
                interpolated_rate = prev_rate + \
                    (next_rate - prev_rate) * (days_from_prev / total_days)

                daily_rates.append({
                    "date": date_str,
                    "usd_ngn": round(interpolated_rate, 2),
                    "usdt_ngn_estimate": round(interpolated_rate, 2),
                    "source": "interpolated",
                    "data_type": "interpolated",
                    "interpolated": True
                })
            elif prev_date:
                # Use last known rate (for dates after last known)
                daily_rates.append({
                    "date": date_str,
                    "usd_ngn": known_rates[prev_date]["rate"],
                    "usdt_ngn_estimate": known_rates[prev_date]["rate"],
                    "source": "extrapolated",
                    "data_type": "extrapolated",
                    "interpolated": True
                })

        current += timedelta(days=1)

    print(f"   ✓ Generated {len(daily_rates)} daily data points")
    return daily_rates


def save_historical_data(rates, filename):
    """Save historical rates to CSV and JSON"""
    if not rates:
        print("No data to save")
        return

    # Sort by date
    rates = sorted(rates, key=lambda x: x["date"])

    # Save JSON
    json_path = DATA_DIR / f"{filename}.json"
    with open(json_path, 'w') as f:
        json.dump(rates, f, indent=2)
    print(f"📁 Saved JSON: {json_path}")

    # Save CSV
    csv_path = DATA_DIR / f"{filename}.csv"
    # Get all unique keys from all records
    all_keys = set()
    for r in rates:
        all_keys.update(r.keys())
    fieldnames = sorted(all_keys)
    # Ensure date is first
    if "date" in fieldnames:
        fieldnames.remove("date")
        fieldnames = ["date"] + fieldnames

    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(
            f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rates)
    print(f"📁 Saved CSV: {csv_path}")


def scrape_historical():
    """Main function to scrape historical data from multiple sources"""
    print("=" * 60)
    print(f"Historical Exchange Rate Scraper - {datetime.now()}")
    print("=" * 60)
    print("\n⚠️  Note: P2P platforms don't provide historical data.")
    print("   Combining multiple sources and interpolating daily rates.\n")

    all_rates = []

    # 1. Get World Bank official rates (yearly, goes back to 2000)
    wb_rates = get_world_bank_historical()
    all_rates.extend(wb_rates)

    # 2. Get historical parallel market rates (more representative of P2P)
    print("\n📊 Adding historical parallel market rates (2020-2024)...")
    parallel_rates = get_cbn_parallel_rates()
    all_rates.extend(parallel_rates)
    print(f"   ✓ Added {len(parallel_rates)} parallel market data points")

    # 3. Get daily data from fawazahmed0 API (starting 2020)
    print("")
    daily_rates = get_historical_rates_fawaz(start_year=2020)
    all_rates.extend(daily_rates)

    if all_rates:
        # Generate daily interpolated rates from 2000 to now
        print("")
        start_date = datetime(2000, 1, 1)
        end_date = datetime.now()

        daily_interpolated = interpolate_daily_rates(
            all_rates, start_date, end_date)

        # Get date range for filename
        first_year = start_date.year
        last_year = end_date.year

        # Save daily data with descriptive filename
        save_historical_data(
            daily_interpolated, f"ngn_usdt_daily_{first_year}_{last_year}")

        # Print summary
        print("\n" + "=" * 60)
        print("📈 SUMMARY")
        print("=" * 60)
        print(f"   Total daily data points: {len(daily_interpolated)}")
        print(
            f"   Date range: {daily_interpolated[0]['date']} to {daily_interpolated[-1]['date']}")

        # Count by data type
        actual_count = sum(
            1 for r in daily_interpolated if not r.get("interpolated", True))
        interpolated_count = sum(
            1 for r in daily_interpolated if r.get("interpolated", False))
        print(f"\n   Actual data points: {actual_count}")
        print(f"   Interpolated points: {interpolated_count}")

        # Show rate progression
        print(f"\n   Rate progression:")
        for year in [2000, 2005, 2010, 2015, 2020, 2022, 2024, end_date.year]:
            year_data = [
                r for r in daily_interpolated if r["date"].startswith(str(year))]
            if year_data:
                print(f"      {year}: {year_data[0]['usd_ngn']:,.2f} NGN/USD")
    else:
        print("\n❌ Failed to fetch historical data")

    print("\n✅ Historical scraping complete!")
    return daily_interpolated if all_rates else []


if __name__ == "__main__":
    scrape_historical()
