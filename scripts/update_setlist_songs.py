import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pathlib import Path

URL = "https://www.setlist.fm/stats/bad-bunny-43cfdb63.html?tour=4bdd83ba"
OUT = Path("data/songs_played.csv")
HEADERS = {"User-Agent": "dtmf-tourboard (personal project)"}

INT_RE = re.compile(r"(\d+)")

def clean_song(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s*\([^)]*song\)\s*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def extract_int(text: str):
    # Take the first integer found (works if cell contains "25 0", "25\n0", etc.)
    m = INT_RE.search(text or "")
    return int(m.group(1)) if m else None

def main():
    r = requests.get(URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    # Find the stats table by headers
    table = None
    for t in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True).lower() for th in t.find_all("th")]
        if any("song" == h for h in headers) and any("perform" in h for h in headers):
            table = t
            break
        if any("song" in h for h in headers) and any("perform" in h for h in headers):
            table = t
            break

    if table is None:
        raise RuntimeError("Could not find Song/Performances table on the page.")

    data = []
    rows = table.find_all("tr")
    for tr in rows[1:]:  # skip header
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue

        # Layout: rank | song | performances
        song_text = tds[1].get_text(" ", strip=True)
        plays_text = tds[-1].get_text(" ", strip=True)

        song = clean_song(song_text)
        plays = extract_int(plays_text)

        if song and plays is not None:
            data.append({"song": song, "plays": plays})

    if not data:
        raise RuntimeError("Parsed 0 songs from table rows. Page structure may have changed.")

    df = (
        pd.DataFrame(data)
        .drop_duplicates(subset=["song"], keep="first")
        .sort_values(["plays", "song"], ascending=[False, True])
        .reset_index(drop=True)
    )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False, encoding="utf-8")
    print(f"Wrote {len(df)} rows → {OUT}")

if __name__ == "__main__":
    main()
