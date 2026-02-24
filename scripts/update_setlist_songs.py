import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pathlib import Path

URL = "https://www.setlist.fm/stats/bad-bunny-43cfdb63.html?tour=4bdd83ba"
OUT = Path("data/songs_played.csv")

HEADERS = {
    "User-Agent": "dtmf-tourboard (personal project; contact: your-email@example.com)"
}

def to_int(s: str):
    s = re.sub(r"[^\d]", "", s or "")
    return int(s) if s else None

def main():
    r = requests.get(URL, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")

    # The stats table is typically the first table on this page.
    table = soup.find("table")
    if table is None:
        raise RuntimeError("No table found on setlist.fm stats page. Page structure may have changed.")

    rows = table.find_all("tr")
    data = []
    for tr in rows[1:]:  # skip header
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        # Song name is usually in the first td, within an <a>
        song = tds[0].get_text(" ", strip=True)

        # Performances usually in the last td
        plays = to_int(tds[-1].get_text(" ", strip=True))

        # Clean common extra words like "Play Video" / "stats"
        song = re.sub(r"\bPlay Video\b", "", song).strip()
        song = re.sub(r"\bstats\b", "", song).strip()
        song = re.sub(r"\s{2,}", " ", song).strip()

        if song and plays is not None:
            data.append({"song": song, "plays": plays})

    if not data:
        raise RuntimeError("Parsed 0 songs. Page structure may have changed.")

    df = pd.DataFrame(data).sort_values("plays", ascending=False).reset_index(drop=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False, encoding="utf-8")

    print(f"Wrote {len(df)} rows → {OUT}")

if __name__ == "__main__":
    main()
