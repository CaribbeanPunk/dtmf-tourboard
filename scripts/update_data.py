from pathlib import Path
import pandas as pd

from tourboard.scraping import scrape_all

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

EVENTS_CSV = DATA_DIR / "events_latest.csv"
SNAPS_CSV = DATA_DIR / "snapshots.csv"

def main():
    snap, events = scrape_all()
    if len(events) == 0:
        raise RuntimeError("Scrape returned 0 events. Aborting update.")

    df_events = pd.DataFrame(events)
    df_events.to_csv(EVENTS_CSV, index=False)

    # append snapshot
    snap_row = pd.DataFrame([snap.__dict__])
    if SNAPS_CSV.exists():
        old = pd.read_csv(SNAPS_CSV)
        out = pd.concat([old, snap_row], ignore_index=True)
    else:
        out = snap_row

    out.to_csv(SNAPS_CSV, index=False)
    print("Updated:", EVENTS_CSV, SNAPS_CSV)

if __name__ == "__main__":
    main()
