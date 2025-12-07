import os
import pyodbc
import requests
import time
import logging
from typing import Optional, Dict
try:
    import pycountry
except Exception:
    pycountry = None
from DbConnectionAzure import CONNECTION_STRING
from GoogleMaps_API_Key import GOOGLE_MAPS_API_KEY

FALLBACK_COUNTRY_MAP = {
    "united states": "US",
    "united states of america": "US",
    "usa": "US",
    "germany": "DE",
    "austria": "AT",
    "switzerland": "ch",
    "belgium": "be",
    "netherlands": "nl",
    "united kingdom": "gb",
    "uk": "uk"
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
REQUEST_TIMEOUT = 10 
MAX_RETRIES = 3
INITIAL_BACKOFF = 0.5

def get_db_country_code_map(cursor) -> Dict[str, str]:
    """
    Try to get (CountryName -> Iso2Code) from the Countries table if it has such a column.
    If not available, return an empty dict.
    """
    mapping = {}
    candidates = ["Iso2Code", "IsoCode", "CountryCode", "ISO2", "ISO_CODE"]
    for col in candidates:
        try:
            cursor.execute(f"SELECT TOP 100 CountryName, {col} FROM Countries")
            rows = cursor.fetchall()
            if rows:
                for r in rows:
                    name = (r[0] or "").strip().lower()
                    code = (r[1] or "").strip().upper() if r[1] else None
                    if name and code:
                        mapping[name] = code
                if mapping:
                    logger.info(f"Found country code column in DB: {col}")
                    return mapping
        except Exception:
            continue
    logger.info("No ISO country code column found in Countries table (or no useful values).")
    return mapping

def country_name_to_iso2(name: str, db_map: Dict[str,str]) -> Optional[str]:
    if not name:
        return None
    key = name.strip().lower()
    if key in db_map:
        return db_map[key]
    if pycountry:
        try:
            results = pycountry.countries.search_fuzzy(name)
            if results:
                return results[0].alpha_2
        except Exception:
            pass
    if key in FALLBACK_COUNTRY_MAP:
        return FALLBACK_COUNTRY_MAP[key]
    if "united states" in key or key.endswith("usa"):
        return "US"
    return None

def extract_country_from_result(result) -> Optional[str]:
    """
    Return country ISO2 short_name if found in result['address_components'], else None.
    """
    for comp in result.get("address_components", []):
        if "country" in comp.get("types", []):
            return comp.get("short_name")
    return None

def geocode_address(session: requests.Session, address: str, expected_iso2: Optional[str]) -> Optional[tuple]:
    """
    Try multiple strategies to get the correct geocode:
    1) If expected_iso2 available, try components=country:XX (authoritative).
    2) If (1) fails or not available, do address-only search and scan results for one whose country matches expected_iso2.
    3) If still not found, return first result but log a warning.
    Returns (lat, lng, used_result_country_iso2, chosen_result_index, full_result) or None.
    """
    def request_with_backoff(params):
        backoff = INITIAL_BACKOFF
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = session.get(GEOCODE_URL, params=params, timeout=REQUEST_TIMEOUT)
                data = resp.json()
                status = data.get("status")
                if status in ("OK", "ZERO_RESULTS"):
                    return data
                if status in ("OVER_QUERY_LIMIT", "RESOURCE_EXHAUSTED") or resp.status_code in (429, 503):
                    logger.warning(f"Geocode API returned {status} (HTTP {resp.status_code}). Backing off {backoff}s (attempt {attempt})")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                return data
            except requests.RequestException as e:
                logger.warning(f"Request exception: {e}. Backing off {backoff}s (attempt {attempt})")
                time.sleep(backoff)
                backoff *= 2
        return None

    params_base = {"address": address, "key": GOOGLE_MAPS_API_KEY}

    if expected_iso2:
        params = params_base.copy()
        params["components"] = f"country:{expected_iso2}"
        data = request_with_backoff(params)
        if data:
            status = data.get("status")
            if status == "OK" and data.get("results"):
                for idx, res in enumerate(data["results"]):
                    res_country = extract_country_from_result(res)
                    if res_country and res_country.upper() == expected_iso2.upper():
                        loc = res["geometry"]["location"]
                        return (loc["lat"], loc["lng"], res_country.upper(), idx, res)
                res = data["results"][0]
                loc = res["geometry"]["location"]
                res_country = extract_country_from_result(res) or "?"
                logger.warning(f"components search returned result but country mismatch: expected {expected_iso2}, got {res_country}")
                return (loc["lat"], loc["lng"], res_country, 0, res)
            elif status == "ZERO_RESULTS":
                logger.info(f"components=country:{expected_iso2} returned ZERO_RESULTS for '{address}'")
            else:
                logger.warning(f"components search status={status} for '{address}'")
    params = params_base.copy()
    data = request_with_backoff(params)
    if data and data.get("status") == "OK" and data.get("results"):
        if expected_iso2:
            for idx, res in enumerate(data["results"]):
                res_country = extract_country_from_result(res)
                if res_country and res_country.upper() == expected_iso2.upper():
                    loc = res["geometry"]["location"]
                    logger.info(f"Found matching country in address-only search at result #{idx} for '{address}' -> {res_country}")
                    return (loc["lat"], loc["lng"], res_country.upper(), idx, res)
        res = data["results"][0]
        loc = res["geometry"]["location"]
        res_country = extract_country_from_result(res)
        if expected_iso2 and res_country and res_country.upper() != expected_iso2.upper():
            logger.warning(f"Address-only top result country {res_country} != expected {expected_iso2} for '{address}'")
        return (loc["lat"], loc["lng"], (res_country or "?"), 0, res)
    elif data and data.get("status") == "ZERO_RESULTS":
        logger.info(f"No geocode result for '{address}' (ZERO_RESULTS).")
        return None
    logger.warning(f"Failed to geocode '{address}': {data.get('status') if data else 'no response'}")
    return None


def main():
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()

    db_country_map = get_db_country_code_map(cursor)

    cursor.execute("""
        SELECT l.Id, l.LocationName, l.Area, c.CountryName
        FROM Locations l
        INNER JOIN Countries c ON l.CountryId = c.Id
        WHERE l.Latitude IS NULL OR l.Longitude IS NULL
    """)
    locations = cursor.fetchall()

    session = requests.Session()

    for loc in locations:
        loc_id, loc_name, loc_area, country_name = loc
        if loc_area and str(loc_area).strip():
            address_wo_country = f"{loc_area}, {loc_name}"
            full_location = f"{loc_area}, {loc_name}, {country_name}"
        else:
            address_wo_country = f"{loc_name}"
            full_location = f"{loc_name}, {country_name}"

        logger.info(f"Geocoding: {full_location} (id={loc_id})")

        iso2 = country_name_to_iso2(country_name, db_country_map)
        if not iso2:
            logger.info(f"Could not determine ISO2 for country '{country_name}'. Geocoding without country components may be ambiguous.")

        result = geocode_address(session, address_wo_country, iso2)

        if result:
            lat, lng, result_country_iso, result_index, raw_result = result
            try:
                cursor.execute(
                    "UPDATE Locations SET Latitude = ?, Longitude = ? WHERE Id = ?",
                    (lat, lng, loc_id)
                )
                conn.commit()
                logger.info(f"Updated id={loc_id}: {full_location} -> {lat}, {lng} (result_country={result_country_iso}, result_index={result_index})")
            except Exception as e:
                logger.error(f"DB update failed for id={loc_id}: {e}")
                conn.rollback()
        else:
            logger.warning(f"Failed to geocode id={loc_id}: {full_location}")

        time.sleep(0.2)

    cursor.close()
    conn.close()
    session.close()


if __name__ == "__main__":
    main()