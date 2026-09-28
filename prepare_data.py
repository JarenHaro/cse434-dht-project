import csv
import gzip
from pathlib import Path

folder = Path(__file__).resolve().parent

source = folder / "StormEvents_details-ftp_v1.0_d1950_c20260323.csv.gz"
destination = folder / "details-1950.csv"

fields = [
    "EVENT_ID",
    "STATE",
    "YEAR",
    "MONTH_NAME",
    "EVENT_TYPE",
    "CZ_TYPE",
    "CZ_NAME",
    "INJURIES_DIRECT",
    "INJURIES_INDIRECT",
    "DEATHS_DIRECT",
    "DEATHS_INDIRECT",
    "DAMAGE_PROPERTY",
    "DAMAGE_CROPS",
    "TOR_F_SCALE"
]

with gzip.open(source, "rt", encoding="utf-8-sig", newline="") as infile:
    reader = csv.DictReader(infile)

    with destination.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerow([field.lower() for field in fields])

        count = 0
        for row in reader:
            writer.writerow([row[field] for field in fields])
            count += 1

print(f"Created {destination.name} with {count} records.")