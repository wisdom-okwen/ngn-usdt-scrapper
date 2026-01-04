"""
Unofficial NGN/USDT Exchange Rate Scraper
Fetches live P2P rates from Bybit (works globally, no geo-restriction)
Data source: Bybit P2P API - represents parallel market / "black market" rates
"""

import requests
import json
import csv
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

BYBIT_P2P_URL = "https://api2.bybit.com/fiat/otc/item/online"


def fetch_bybit_p2p(side="buy", currency="NGN", asset="USDT", size=20):
    """
    Fetch P2P offers from Bybit
    side: 'buy' (buy USDT with NGN) or 'sell' (sell USDT for NGN)
    """
    payload = {
        "tokenId": asset,
        "currencyId": currency,
        "side": "1" if side == "buy" else "0",
        "page": "1",
        "size": str(size),
        "payment": []
    }
    
    try:
        response = requests.post(
            BYBIT_P2P_URL, 
            json=payload, 
            headers={"Content-Type": "application/json"}, 
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        if data.get("ret_code") != 0:
            print(f"   ⚠ API error: {data.get('ret_msg')}")
            return []
        
        items = data.get("result", {}).get("items", [])
        rates = []
        
        for item in items:
            payments = item.get("payments", [])
            if isinstance(payments, list):
                payments = ", ".join(str(p) for p in payments)
            rates.append({
                "exchange": "bybit",
                "side": side,
                "price": float(item.get("price", 0)),
                "min_amount": float(item.get("minAmount", 0)),
                "max_amount": float(item.get("maxAmount", 0)),
                "available": float(item.get("lastQuantity", 0)),
                "payment_ids": payments,
                "merchant": item.get("nickName", ""),
                "orders": item.get("recentOrderNum", 0),
                "completion_rate": item.get("recentExecuteRate", 0),
                "currency": currency,
                "asset": asset,
                "timestamp": datetime.utcnow().isoformat()
            })
        
        return rates
        
    except Exception as e:
        print(f"   ❌ Error fetching {side} rates: {e}")
        return []


def calculate_summary(buy_rates, sell_rates):
    """Calculate summary statistics"""
    buy_prices = [r["price"] for r in buy_rates if r.get("price")]
    sell_prices = [r["price"] for r in sell_rates if r.get("price")]
    
    summary = {
        "timestamp": datetime.utcnow().isoformat(),
        "currency_pair": "NGN/USDT",
        "source": "Bybit P2P (unofficial/parallel market rate)",
        "buy_usdt": {
            "count": len(buy_prices),
            "best": min(buy_prices) if buy_prices else None,
            "worst": max(buy_prices) if buy_prices else None,
            "avg": sum(buy_prices) / len(buy_prices) if buy_prices else None,
        },
        "sell_usdt": {
            "count": len(sell_prices),
            "best": max(sell_prices) if sell_prices else None,
            "worst": min(sell_prices) if sell_prices else None,
            "avg": sum(sell_prices) / len(sell_prices) if sell_prices else None,
        },
        "spread_percent": None,
        "mid_rate": None
    }
    
    if summary["buy_usdt"]["avg"] and summary["sell_usdt"]["avg"]:
        buy_avg = summary["buy_usdt"]["avg"]
        sell_avg = summary["sell_usdt"]["avg"]
        spread = buy_avg - sell_avg
        summary["spread_percent"] = (spread / sell_avg) * 100
        summary["mid_rate"] = (buy_avg + sell_avg) / 2
    
    return summary


def save_data(buy_rates, sell_rates, summary, timestamp):
    """Save rates and summary to files"""
    all_rates = buy_rates + sell_rates
    
    if all_rates:
        csv_path = DATA_DIR / f"unofficial_ngn_usdt_rates_{timestamp}.csv"
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=all_rates[0].keys())
            writer.writeheader()
            writer.writerows(all_rates)
        print(f"   📁 Rates: {csv_path.name}")
    
    if summary:
        json_path = DATA_DIR / f"unofficial_ngn_usdt_summary_{timestamp}.json"
        with open(json_path, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"   📁 Summary: {json_path.name}")


def scrape_unofficial_rates():
    """Main function: Fetch unofficial P2P rates from Bybit"""
    print("=" * 60)
    print("  Unofficial NGN/USDT Rate Scraper (Bybit P2P)")
    print("=" * 60)
    print(f"\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("\n📊 Fetching P2P rates from Bybit...")
    
    print("   → BUY USDT offers...")
    buy_rates = fetch_bybit_p2p(side="buy")
    print(f"   ✓ {len(buy_rates)} offers found")
    
    print("   → SELL USDT offers...")
    sell_rates = fetch_bybit_p2p(side="sell")
    print(f"   ✓ {len(sell_rates)} offers found")
    
    if not buy_rates and not sell_rates:
        print("\n❌ No data collected")
        return None
    
    summary = calculate_summary(buy_rates, sell_rates)
    
    print("\n" + "=" * 60)
    print("  UNOFFICIAL NGN/USDT RATES (P2P Market)")
    print("=" * 60)
    
    if summary["buy_usdt"]["avg"]:
        print(f"\n📈 BUY USDT (you pay NGN):")
        print(f"   Best:  ₦{summary['buy_usdt']['best']:,.2f}")
        print(f"   Avg:   ₦{summary['buy_usdt']['avg']:,.2f}")
        print(f"   Worst: ₦{summary['buy_usdt']['worst']:,.2f}")
    
    if summary["sell_usdt"]["avg"]:
        print(f"\n📉 SELL USDT (you receive NGN):")
        print(f"   Best:  ₦{summary['sell_usdt']['best']:,.2f}")
        print(f"   Avg:   ₦{summary['sell_usdt']['avg']:,.2f}")
        print(f"   Worst: ₦{summary['sell_usdt']['worst']:,.2f}")
    
    if summary["mid_rate"]:
        print(f"\n💹 Mid Rate: ₦{summary['mid_rate']:,.2f}")
    if summary["spread_percent"]:
        print(f"   Spread:   {summary['spread_percent']:.2f}%")
    
    print("\n💾 Saving data...")
    save_data(buy_rates, sell_rates, summary, timestamp)
    
    print("\n✅ Done!")
    return summary


if __name__ == "__main__":
    scrape_unofficial_rates()
