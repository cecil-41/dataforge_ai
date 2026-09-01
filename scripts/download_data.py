"""
Download NYC TLC Yellow Taxi trip data for our PySpark project.

Source: NYC Taxi & Limousine Commission (TLC) Trip Record Data
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

We use a few months of data (each ~40-50MB as Parquet, ~3M rows/month)
plus the taxi zone lookup table used to join pickup/dropoff location IDs
to human-readable borough/zone names.
"""

import sys
from pathlib import Path

import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# A few months across a year boundary -> gives us enough volume (~9M+ rows)
# to make partitioning/shuffling/performance-tuning lessons meaningful.
MONTHS = ["2023-01", "2023-02", "2023-03"]

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def download(url: str, dest: Path) -> None:
    if dest.exists():
        print(f"[skip] {dest.name} already exists ({dest.stat().st_size / 1e6:.1f} MB)")
        return

    print(f"[download] {url} -> {dest}")
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        written = 0
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                written += len(chunk)
                if total:
                    pct = written / total * 100
                    print(f"\r  {pct:5.1f}% ({written / 1e6:.1f}/{total / 1e6:.1f} MB)", end="")
        print()


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    download(LOOKUP_URL, DATA_DIR / "taxi_zone_lookup.csv")

    for month in MONTHS:
        url = f"{BASE_URL}/yellow_tripdata_{month}.parquet"
        dest = DATA_DIR / f"yellow_tripdata_{month}.parquet"
        download(url, dest)

    print("\nDone. Files in data/raw:")
    for p in sorted(DATA_DIR.iterdir()):
        print(f"  {p.name}  ({p.stat().st_size / 1e6:.1f} MB)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
