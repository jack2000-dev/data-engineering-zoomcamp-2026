"""
Verify Question 3: What is the total amount of money generated in tips?

Usage (requires internet):
    uv run python3 verify_q3.py

Or if taxi_data_all.json exists (from fetch_and_save.py):
    uv run python3 verify_q3.py --offline
"""
import json
import os
import sys
import ssl

# Fix SSL certificate issue on macOS
ssl_ctx = ssl.create_default_context()
try:
    import certifi
    ssl_ctx.load_verify_locations(certifi.where())
except ImportError:
    ssl_ctx = ssl._create_unverified_context()


def from_api():
    """Fetch all pages from API and compute total tips."""
    import urllib.request

    base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
    page = 1
    all_rows = []

    while True:
        url = f"{base_url}?page={page}"
        print(f"Fetching page {page}...")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15, context=ssl_ctx) as response:
                data = json.loads(response.read().decode())
        except Exception as e:
            print(f"Error: {e}")
            print("No internet. Try: uv run python3 verify_q3.py --offline")
            return

        if not data:
            break

        all_rows.extend(data)
        page += 1

    print_results(all_rows)


def from_file():
    """Load from taxi_data_all.json and compute total tips."""
    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "taxi_data_all.json")
    if not os.path.exists(filepath):
        print(f"ERROR: {filepath} not found!")
        print("Run first: uv run python3 fetch_and_save.py")
        return

    with open(filepath) as f:
        data = json.load(f)

    print_results(data)


def print_results(data):
    total_tips = sum(r.get("Tip_Amt", 0) or 0 for r in data)
    total_records = len(data)
    tipped_trips = sum(1 for r in data if (r.get("Tip_Amt") or 0) > 0)

    print("\n" + "=" * 60)
    print("QUESTION 3: Total Tips")
    print("=" * 60)

    print(f"\nTotal records: {total_records}")
    print(f"Total tips: ${total_tips:,.2f}")
    print(f"Trips with tips: {tipped_trips} ({tipped_trips/total_records*100:.1f}%)")
    print(f"Average tip (all trips): ${total_tips/total_records:.2f}")
    if tipped_trips > 0:
        print(f"Average tip (tipped only): ${total_tips/tipped_trips:.2f}")


if __name__ == "__main__":
    if "--offline" in sys.argv:
        from_file()
    else:
        from_api()
