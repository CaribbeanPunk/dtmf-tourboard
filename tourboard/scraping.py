from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Tuple, List, Dict

import requests
from bs4 import BeautifulSoup

SOURCE_URL = "https://touringdata.org/2025/06/19/bad-bunny-debi-tirar-mas-fotos-tour/"


@dataclass
class Snapshot:
    scraped_at: str
    reported_revenue_usd: Optional[float]
    reported_tickets: Optional[int]
    avg_revenue_usd: Optional[float]
    avg_tickets: Optional[int]
    avg_price_usd: Optional[float]
    total_reports_text: Optional[str]
    source_url: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fetch_html(url: str = SOURCE_URL, timeout: int = 25) -> str:
    headers = {
        "User-Agent": "DTMF-Tourboard/1.0 (personal project; contact: you@example.com)"
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text


def _to_float_money(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip()
    if s.upper() == "TBA":
        return None
    m = re.search(r"\$([\d,]+(?:\.\d+)?)", s)
    if not m:
        return None
    return float(m.group(1).replace(",", ""))


def _to_int(s: str) -> Optional[int]:
    if not s:
        return None
    s = s.strip()
    if s.upper() == "TBA":
        return None
    m = re.search(r"([\d,]+)", s)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))


def _parse_capacity_pct(s: str) -> Optional[float]:
    m = re.search(r"\((\d+(?:\.\d+)?)%\)", s)
    if not m:
        return None
    return float(m.group(1))


def _split_location_and_gross(s: str) -> Tuple[str, Optional[float]]:
    s = s.strip()
    if "$" in s:
        parts = s.split("$", 1)
        loc = parts[0].strip()
        gross = _to_float_money("$" + parts[1])
        return loc, gross
    return s.replace(" TBA", "").strip(), None


def parse_snapshot_and_lines(html: str) -> Tuple[Snapshot, List[str]]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]

    def find_value_after(label: str) -> Optional[str]:
        for i, ln in enumerate(lines):
            if ln == label and i + 1 < len(lines):
                return lines[i + 1]
        return None

    reported_revenue = _to_float_money(find_value_after("Reported Revenue") or "")
    reported_tickets = _to_int(find_value_after("Reported Tickets Sold") or "")
    avg_revenue = _to_float_money(find_value_after("Average Revenue") or "")
    avg_tickets = _to_int(find_value_after("Average Tickets Sold") or "")
    avg_price = _to_float_money(find_value_after("Average Price") or "")
    total_reports_text = find_value_after("Total Reports")

    snap = Snapshot(
        scraped_at=_now_iso(),
        reported_revenue_usd=reported_revenue,
        reported_tickets=reported_tickets,
        avg_revenue_usd=avg_revenue,
        avg_tickets=avg_tickets,
        avg_price_usd=avg_price,
        total_reports_text=total_reports_text,
        source_url=SOURCE_URL,
    )
    return snap, lines


def parse_events(lines: List[str], scraped_at: str, source_url: str) -> List[Dict]:
    """
    Robust parser for Touring Data pages.

    Key fix:
    - Handles region headers appearing as TWO LINES:
        "Latin America"
        "Box Office"
      (as seen in your debug output)
    - Also still supports "Latin America Box Office" on one line.
    """

    def norm(s: str) -> str:
        s = s.replace("\u00a0", " ").replace("\u2009", " ").strip()
        s = re.sub(r"\s+", " ", s)
        return s

    L = [norm(x) for x in lines if norm(x)]
    events: List[Dict] = []

    region_names = ["Latin America", "Europe", "Oceania"]
    current_region: Optional[str] = None

    # Date lines like "November 21-22, 2025"
    def is_date_line(s: str) -> bool:
        return bool(re.search(r",\s*\d{4}\s*$", s)) and any(
            s.lower().startswith(m) for m in
            ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
        )

    def looks_like_tickets(s: str) -> bool:
        # IMPORTANT: tickets lines do NOT contain '$' — exclude gross lines

        if "$" in s:
            return False
        if s.strip().upper() == "TBA":
            return True
        if "(" in s and "%" in s:
            return True
        return bool(re.search(r"\b\d{1,3}(?:,\d{3})+\b", s))

    def looks_like_shows(s: str) -> bool:
    # Examples: "2 shows", "8 shows", "1 show"
        return bool(re.search(r"\b\d+\s*show", s, re.IGNORECASE))



    def looks_like_location(s: str) -> bool:
        if "$" in s:
            return False
        if "box office" in s.lower():
            return False
        if "reported" in s.lower():
            return False
        return ("," in s) and (len(s) <= 80)

    def looks_like_tickets(s: str) -> bool:
        if s.strip().upper() == "TBA":
            return True
        if "(" in s and "%" in s:
            return True
        return bool(re.search(r"\b\d{1,3}(?:,\d{3})+\b", s))

    i = 0
    while i < len(L):
        ln = L[i]

        # ✅ Region header can be:
        # 1) "Latin America Box Office" in one line
        # 2) "Latin America" followed by "Box Office" in the next line (your case)
        # Case 1:
        for r in region_names:
            if ln.lower().startswith(r.lower()) and "box office" in ln.lower():
                current_region = r
                break

        # Case 2:
        if ln in region_names and (i + 1) < len(L) and L[i + 1].lower() == "box office":
            current_region = ln
            i += 2
            continue

        # Parse event blocks once we are inside a region
        if current_region and is_date_line(ln):
            date_range = ln
            artist = L[i + 1] if i + 1 < len(L) else ""
            venue = L[i + 2] if i + 2 < len(L) else ""

            # Scan forward to find the "X shows" line (up to 40 lines)
            block = []
            j = i + 3
            while j < len(L) and j < i + 40:
                block.append(L[j])
                if looks_like_shows(L[j]):
                    break
                j += 1

            shows_line = next((x for x in block if looks_like_shows(x)), None)
            if not shows_line:
                i += 1
                continue

            shows = _to_int(shows_line)

            gross_line = next((x for x in block if "$" in x), None)
            gross_usd = _to_float_money(gross_line or "")

            location_line = next((x for x in block if looks_like_location(x)), None)
            location = (location_line or "").replace(" TBA", "").strip()

            city, country = None, None
            if "," in location:
                city, country = [p.strip() for p in location.split(",", 1)]
            elif location:
                city = location

            # --- Tickets line selection (prefer lines with % like "64,175 (100%)") ---
            tickets_line = None
            # 1) Prefer the percentage format (most reliable for tickets)
            tickets_line = next((x for x in block if ("%" in x and "(" in x) and "$" not in x), None)

            # 2) If not found, allow "TBA" (still not gross)
            if tickets_line is None:
                tickets_line = next((x for x in block if x.strip().upper() == "TBA" and "$" not in x), None)
                
            # 3) Fallback: pick a numeric line that is NOT the gross line
            if tickets_line is None:
                gross_line = next((x for x in block if "$" in x), None)
                numeric_candidates = [
                    x for x in block
                    if ("$" not in x)
                    and bool(re.search(r"\b\d{1,3}(?:,\d{3})+\b", x))
                    and (x != gross_line)
                ]
                tickets_line = numeric_candidates[0] if numeric_candidates else None
            
    
            tickets = _to_int(tickets_line or "")
            capacity_pct = _parse_capacity_pct(tickets_line or "")


            events.append(
                {
                    "region": current_region,
                    "date_range": date_range,
                    "start_date": None,
                    "end_date": None,
                    "artist": artist,
                    "venue": venue,
                    "city": city,
                    "country": country,
                    "gross_usd": gross_usd,
                    "tickets": tickets,
                    "capacity_pct": capacity_pct,
                    "shows": shows,
                    "source_url": source_url,
                    "scraped_at": scraped_at,
                }
            )

            i = j + 1
            continue

        i += 1

    return events





def scrape_all(url: str = SOURCE_URL) -> Tuple[Snapshot, List[Dict]]:
    html = fetch_html(url)
    snap, lines = parse_snapshot_and_lines(html)
    events = parse_events(lines, scraped_at=snap.scraped_at, source_url=url)
    print("DEBUG parsed events:", len(events))
    if len(events) == 0:
        print("DEBUG sample lines:")
        for k, ln in enumerate(lines[:120]):
            print(k, repr(ln))

    return snap, events
