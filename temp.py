import pandas as pd
import requests
from pathlib import Path
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "saved_data"

DATA_DIR.mkdir(exist_ok=True)

FILE = DATA_DIR / "fuel_receipts.parquet"

URL = (
    "https://s3.us-west-2.amazonaws.com/"
    "pudl.catalyst.coop/eel-hole/"
    "out_eia923__fuel_receipts_costs.parquet"
)

FILE = Path("saved_data/out_eia923__fuel_receipts_costs.parquet")
FILE.parent.mkdir(exist_ok=True)

# download once
if not FILE.exists():
    print("Downloading parquet...")

    with requests.get(URL, stream=True) as r:
        r.raise_for_status()
        with open(FILE, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

# read locally (THIS avoids the error)
df = pd.read_parquet(FILE)

df = df[df["plant_state"] == "IL"]

print(df.head())
