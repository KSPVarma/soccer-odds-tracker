import requests
import json
import gzip
import boto3
import os
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

LEAGUES = {
    "premier_league":   {"id": 1980,  "tier": 1, "country": "England",     "name": "England - Premier League"},
    "champions_league": {"id": 2627,  "tier": 1, "country": "Europe",      "name": "UEFA - Champions League"},
    "la_liga":          {"id": 2196,  "tier": 1, "country": "Spain",       "name": "Spain - La Liga"},
    "bundesliga":       {"id": 1842,  "tier": 1, "country": "Germany",     "name": "Germany - Bundesliga"},
    "serie_a":          {"id": 2436,  "tier": 2, "country": "Italy",       "name": "Italy - Serie A"},
    "ligue_1":          {"id": 2036,  "tier": 2, "country": "France",      "name": "France - Ligue 1"},
    "mls":              {"id": 2663,  "tier": 2, "country": "USA",         "name": "USA - Major League Soccer"},
    "eredivisie":       {"id": 1928,  "tier": 3, "country": "Netherlands", "name": "Netherlands - Eredivisie"},
    "brasileirao":      {"id": 1834,  "tier": 3, "country": "Brazil",      "name": "Brazil - Serie A"},
    "scottish_prem":    {"id": 2421,  "tier": 3, "country": "Scotland",    "name": "Scotland - Premiership"},
    "wc_qualifiers_eu": {"id": 2015,  "tier": 2, "country": "Europe",      "name": "FIFA - World Cup Qualifiers Europe"},
}

BASE_URL = "https://guest.api.arcadia.pinnacle.com/0.1"

HEADERS = {
    "X-Api-Key":       "CmX2KcMrXuFmNg6YFbmTxE0y9CIrOi0R",
    "X-Device-Uuid":   "bdd08300-e287e4d5-9baba0d7-fa7701df",
    "Accept":          "application/json",
    "Accept-Encoding": "identity",
    "Content-Type":    "application/json",
    "Origin":          "https://www.pinnacle.com",
    "Referer":         "https://www.pinnacle.com/",
    "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}


def fetch_json(url: str) -> any:
    """
    Core fetch function used by all endpoints.
    Handles encoding, empty responses, and errors cleanly.
    """
    response = requests.get(url, headers=HEADERS, timeout=15)
    response.raise_for_status()

    if not response.content or response.status_code == 204:
        return []

    content_encoding = response.headers.get("content-encoding", "")
    if content_encoding == "gzip" or response.content[:2] == b"\x1f\x8b":
        raw = gzip.decompress(response.content)
    else:
        raw = response.content

    text = raw.decode("utf-8").strip()
    if not text:
        return []

    return json.loads(text)


def get_matchups(league_id: int) -> list:
    """
    Fetch all upcoming matchups for a league.
    Filters out special props — keeps only standard home/away matches.
    """
    url  = f"{BASE_URL}/leagues/{league_id}/matchups?brandId=0"
    data = fetch_json(url)
    return [m for m in data if m.get("type") != "special"]


def get_markets_for_matchup(matchup_id: int) -> list:
    """
    Fetch straight markets for a single matchup.
    Returns moneyline (1X2), spread, and totals for period 0 (full match).
    """
    url  = f"{BASE_URL}/matchups/{matchup_id}/markets/straight"
    data = fetch_json(url)
    # Keep only full match (period 0) markets
    return [m for m in data if m.get("period") == 0]


def parse_matchup(matchup: dict, markets: list) -> dict:
    """
    Combine a matchup with its markets into one clean object.
    Maps participant IDs to team names for readable odds.
    """
    participants = matchup.get("participants", [])

    # Build id → name lookup
    id_to_name = {p["id"]: p["name"] for p in participants if "id" in p}

    home = next((p["name"] for p in participants if p.get("alignment") == "home"), None)
    away = next((p["name"] for p in participants if p.get("alignment") == "away"), None)

    if not home and len(participants) >= 2:
        home = participants[0]["name"]
        away = participants[1]["name"]

    # Parse each market type
    parsed_markets = []
    for market in markets:
        market_type = market.get("type")
        prices      = market.get("prices", [])

        parsed_prices = []
        for price in prices:
            participant_id   = price.get("participantId")
            participant_name = id_to_name.get(participant_id, f"participant_{participant_id}")
            parsed_prices.append({
                "participant_id":   participant_id,
                "participant_name": participant_name,
                "price":            price.get("price"),  # American odds
            })

        parsed_markets.append({
            "market_type": market_type,  # moneyline, spread, total
            "key":         market.get("key"),
            "cutoff_at":   market.get("cutoffAt"),
            "prices":      parsed_prices,
        })

    return {
        "matchup_id":  matchup["id"],
        "home_team":   home,
        "away_team":   away,
        "start_time":  matchup.get("startTime"),
        "status":      matchup.get("status"),
        "is_live":     matchup.get("isLive", False),
        "league_id":   matchup.get("league", {}).get("id"),
        "league_name": matchup.get("league", {}).get("name"),
        "rotation":    matchup.get("rotation"),
        "markets":     parsed_markets,
    }


def build_snapshot(league_key: str, league_meta: dict) -> dict:
    """
    Full snapshot for one league — matchups + markets combined.
    One markets API call per matchup, with a small delay to be polite.
    """
    league_id  = league_meta["id"]
    scraped_at = datetime.now(timezone.utc).isoformat()

    raw_matchups = get_matchups(league_id)
    print(f"    {len(raw_matchups)} matchups found — fetching markets...")

    parsed_matchups = []
    for matchup in raw_matchups:
        matchup_id = matchup["id"]
        try:
            markets = get_markets_for_matchup(matchup_id)
            parsed  = parse_matchup(matchup, markets)
            parsed_matchups.append(parsed)
            time.sleep(0.3)  # polite delay — avoid hammering the API
        except Exception as e:
            print(f"    Warning: could not fetch markets for matchup {matchup_id}: {e}")
            parsed_matchups.append(parse_matchup(matchup, []))

    return {
        "book":          "pinnacle",
        "league_key":    league_key,
        "league_id":     league_id,
        "tier":          league_meta["tier"],
        "country":       league_meta["country"],
        "scraped_at":    scraped_at,
        "matchup_count": len(parsed_matchups),
        "matchups":      parsed_matchups,
    }


def save_locally(snapshot: dict, league_key: str) -> str:
    now      = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str   = now.strftime("%H%M%S")
    out_dir  = f"data/raw/pinnacle/{league_key}/{date_str}"
    os.makedirs(out_dir, exist_ok=True)

    filepath = f"{out_dir}/{ts_str}.json"
    with open(filepath, "w") as f:
        json.dump(snapshot, f, indent=2)

    print(f"    Saved → {filepath}  ({snapshot['matchup_count']} matches)")
    return filepath


def save_to_s3(snapshot: dict, league_key: str) -> str:
    s3 = boto3.client(
        "s3",
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name           = os.getenv("AWS_REGION", "us-east-1"),
    )

    now      = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str   = now.strftime("%H%M%S")
    bucket   = os.getenv("S3_BUCKET_NAME", "soccer-odds-raw")
    s3_key   = f"raw/pinnacle/{league_key}/{date_str}/{ts_str}.json"

    s3.put_object(
        Bucket      = bucket,
        Key         = s3_key,
        Body        = json.dumps(snapshot, indent=2),
        ContentType = "application/json",
    )

    print(f"    Saved → s3://{bucket}/{s3_key}  ({snapshot['matchup_count']} matches)")
    return s3_key


def run(use_s3: bool = False):
    print(f"\n[Pinnacle] Starting scrape — {datetime.now(timezone.utc).isoformat()}")
    print(f"[Pinnacle] Mode: {'S3' if use_s3 else 'local'} | Leagues: {len(LEAGUES)}\n")

    success = 0
    failed  = 0

    for league_key, league_meta in LEAGUES.items():
        print(f"  [{league_key}]")
        try:
            snapshot = build_snapshot(league_key, league_meta)

            if use_s3:
                save_to_s3(snapshot, league_key)
            else:
                save_locally(snapshot, league_key)

            success += 1

        except requests.HTTPError as e:
            print(f"    HTTP {e.response.status_code} error — skipping")
            failed += 1
        except Exception as e:
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n[Pinnacle] Done — {success} succeeded, {failed} failed\n")


if __name__ == "__main__":
    run(use_s3=False)