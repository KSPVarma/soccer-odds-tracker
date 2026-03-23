import requests
import json
import os
import re
import time
from datetime import datetime, timezone

LEAGUE_PREFIXES = {
    "epl":  {"key": "premier_league",   "tier": 1, "country": "England",  "name": "English Premier League"},
    "ucl":  {"key": "champions_league", "tier": 1, "country": "Europe",   "name": "UEFA Champions League"},
    "lal":  {"key": "la_liga",          "tier": 1, "country": "Spain",    "name": "Spanish La Liga"},
    "bun":  {"key": "bundesliga",       "tier": 1, "country": "Germany",  "name": "German Bundesliga"},
    "sea":  {"key": "serie_a",          "tier": 2, "country": "Italy",    "name": "Italian Serie A"},
    "fl1":  {"key": "ligue_1",          "tier": 2, "country": "France",   "name": "French Ligue 1"},
    "mls":  {"key": "mls",              "tier": 2, "country": "USA",      "name": "MLS"},
}

HEADERS = {
    "Accept":     "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}

GAMMA_API = "https://gamma-api.polymarket.com"


def get_build_id() -> str | None:
    resp  = requests.get("https://polymarket.com", headers=HEADERS, timeout=15)
    match = re.search(r'"buildId":"([^"]+)"', resp.text)
    return match.group(1) if match else None


def get_match_slugs(build_id: str) -> list:
    url  = f"https://polymarket.com/_next/data/{build_id}/en/sports/soccer/games.json?league=soccer"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    data = resp.json()

    parent_map = {}
    for q in data["pageProps"]["dehydratedState"]["queries"]:
        if q.get("queryKey") == ["parentToChildEventIds"]:
            parent_map = q["state"]["data"]
            break

    return [s for s in parent_map.keys() if s.split("-")[0] in LEAGUE_PREFIXES]


def parse_yes_price(market: dict) -> float | None:
    prices = market.get("outcomePrices", [])
    if isinstance(prices, str):
        prices = json.loads(prices)
    try:
        return float(prices[0])
    except Exception:
        return None


def get_event_odds(slug: str) -> dict | None:
    url   = f"{GAMMA_API}/events?slug={slug}"
    resp  = requests.get(url, headers=HEADERS, timeout=15)
    data  = resp.json()

    if not data:
        return None

    event = data[0]

    if event.get("closed") or not event.get("active"):
        return None

    title = event.get("title", "")
    if " vs. " not in title:
        return None

    home_team, away_team = [t.strip() for t in title.split(" vs. ", 1)]

    home_price = draw_price = away_price = None

    for market in event.get("markets", []):
        q         = market.get("question", "").lower()
        yes_price = parse_yes_price(market)

        if yes_price is None:
            continue

        if "draw" in q:
            draw_price = yes_price
        elif home_team.lower() in q:
            home_price = yes_price
        elif away_team.lower() in q:
            away_price = yes_price

    if None in (home_price, draw_price, away_price):
        return None

    return {
        "matchup_id": event.get("id"),
        "slug":       slug,
        "home_team":  home_team,
        "away_team":  away_team,
        "start_time": event.get("endDate"),
        "volume_usd": event.get("volume"),
        "markets": [{
            "market_type": "1x2",
            "prices": [
                {"name": "home", "probability": home_price},
                {"name": "draw", "probability": draw_price},
                {"name": "away", "probability": away_price},
            ]
        }]
    }


def run():
    print(f"\n[Polymarket] {datetime.now(timezone.utc).isoformat()}")

    build_id = get_build_id()
    print(f"  Build ID: {build_id}")

    slugs = get_match_slugs(build_id)
    print(f"  Slugs found: {len(slugs)}")

    league_matches: dict = {}
    for slug in slugs:
        key = LEAGUE_PREFIXES[slug.split("-")[0]]["key"]
        league_matches.setdefault(key, []).append(slug)

    for league_key, league_slugs in league_matches.items():
        print(f"\n  [{league_key}] — {len(league_slugs)} slugs")
        matchups = []

        for slug in league_slugs:
            odds = get_event_odds(slug)
            if odds:
                matchups.append(odds)
                print(f"    OK: {odds['home_team']} vs {odds['away_team']} — {odds['markets'][0]['prices']}")
            else:
                print(f"    SKIP: {slug}")
            time.sleep(0.2)

        out_dir  = f"data/raw/polymarket/{league_key}/{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
        os.makedirs(out_dir, exist_ok=True)
        filepath = f"{out_dir}/{datetime.now(timezone.utc).strftime('%H%M%S')}.json"

        with open(filepath, "w") as f:
            json.dump({
                "book":          "polymarket",
                "league_key":    league_key,
                "scraped_at":    datetime.now(timezone.utc).isoformat(),
                "matchup_count": len(matchups),
                "matchups":      matchups,
            }, f, indent=2)

        print(f"  Saved {len(matchups)} matches → {filepath}")


if __name__ == "__main__":
    run()