from __future__ import annotations

import time
from typing import Optional, Tuple

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

from tourboard.db import geocache_get, geocache_set


# Nominatim requires a real-ish user agent string
_geocoder = Nominatim(user_agent="dtmf-tourboard/1.0 (personal project)")


def geocode_city_country(conn, city: str, country: str, sleep_sec: float = 1.0) -> Optional[Tuple[float, float]]:
    """
    Returns (lat, lon) for a city,country, using SQLite cache first.
    sleep_sec helps respect Nominatim rate limits.
    """
    if not city or not country:
        return None

    key = f"{city.strip().lower()}|{country.strip().lower()}"
    cached = geocache_get(conn, key)
    if cached:
        return float(cached[0]), float(cached[1])

    query = f"{city}, {country}"

    try:
        loc = _geocoder.geocode(query, timeout=10)
        time.sleep(sleep_sec)  # be kind to the free service
        if not loc:
            return None
        lat, lon = float(loc.latitude), float(loc.longitude)
        geocache_set(conn, key, city, country, lat, lon)
        return lat, lon
    except (GeocoderTimedOut, GeocoderServiceError):
        return None
