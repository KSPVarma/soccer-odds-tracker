import json
import os
import time
import asyncio
from datetime import datetime, timezone
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

LEAGUES = {
    "premier_league":   {"tier": 1, "country": "England",     "name": "England - Premier League",  "url": "https://www.nj.bet365.com/#/AS/B1/C1/D1002/E91422157/G40/"},
    "champions_league": {"tier": 1, "country": "Europe",      "name": "UEFA - Champions League",    "url": "https://www.nj.bet365.com/#/AS/B1/C1/D1002/E91387996/G40/"},
    "la_liga":          {"tier": 1, "country": "Spain",       "name": "Spain - La Liga",            "url": "https://www.nj.bet365.com/#/AS/B1/C1/D1002/E91422169/G40/"},
    "bundesliga":       {"tier": 1, "country": "Germany",     "name": "Germany - Bundesliga",       "url": "https://www.nj.bet365.com/#/AS/B1/C1/D1002/E91422162/G40/"},
    "serie_a":          {"tier": 2, "country": "Italy",       "name": "Italy - Serie A",            "url": "https://www.nj.bet365.com/#/AS/B1/C1/D1002/E91422165/G40/"},
    "ligue_1":          {"tier": 2, "country": "France",      "name": "France - Ligue 1",           "url": "https://www.nj.bet365.com/#/AS/B1/C1/D1002/E91422161/G40/"},
    "mls":              {"tier": 2, "country": "USA",         "name": "USA - MLS",                  "url": "https://www.nj.bet365.com/#/AS/B1/C1/D1002/E91571201/G40/"},
    "eredivisie":       {"tier": 3, "country": "Netherlands", "name": "Netherlands - Eredivisie",   "url": "https://www.nj.bet365.com/#/AS/B1/C1/D1002/E91422168/G40/"},
    "brasileirao":      {"tier": 3, "country": "Brazil",      "name": "Brazil - Serie A",           "url": "https://www.nj.bet365.com/#/AS/B1/C1/D1002/E91422155/G40/"},
    "scottish_prem":    {"tier": 3, "country": "Scotland",    "name": "Scotland - Premiership",     "url": "https://www.nj.bet365.com/#/AS/B1/C1/D1002/E91422172/G40/"},
}


def parse_block(block: str) -> dict:
    """
    Parse a single pipe-block into a key-value dict.
    Each block looks like: 'PA;ID=123;NA=Brentford;OD=4/7;...'
    """
    parts  = block.strip().split(";")
    result = {"_type": parts[0]} if parts else {}
    for part in parts[1:]:
        if "=" in part:
            k, _, v = part.partition("=")
            result[k] = v
    return result


def fraction_to_decimal(fraction: str) -> float | None:
    """
    Convert fractional odds like '4/7' to decimal like 1.571.
    Decimal odds = (numerator / denominator) + 1
    """
    try:
        num, den = fraction.split("/")
        return round(int(num) / int(den) + 1, 4)
    except Exception:
        return None


def parse_bet365_response(raw_text: str, league_key: str, league_meta: dict) -> dict:
    """
    Parse Bet365's pipe-delimited format into our standard snapshot structure.

    Format overview:
      F  = frame separator (major section break)
      CL = container/league block  → HT= league name
      EV = event group
      MG = market group
      MA = market header (Home / Tie / Away labels)
      PA = participant/outcome → NA= team name, OD= fractional odds, BC= kickoff
    """
    scraped_at = datetime.now(timezone.utc).isoformat()
    matchups   = []

    # Split on pipe — each segment is one block
    blocks = [b for b in raw_text.split("|") if b.strip()]

    current_league  = None
    current_match   = None
    current_markets = []
    current_market_name = None
    pending_prices  = []

    for raw_block in blocks:
        block = parse_block(raw_block)
        btype = block.get("_type", "")

        if btype == "CL":
            # New league section
            current_league = block.get("HT") or block.get("NA")

        elif btype == "PA" and block.get("FD"):
            # FD = full description e.g. "Brentford v Wolverhampton"
            # This is a match row — save previous match first
            if current_match and pending_prices:
                _flush_market(current_markets, current_market_name, pending_prices)
                pending_prices = []

            if current_match:
                current_match["markets"] = current_markets
                matchups.append(current_match)

            # Parse kickoff time from BC= e.g. "20260316200000"
            bc       = block.get("BC", "")
            kickoff  = _parse_kickoff(bc)

            home, away = _split_teams(block.get("FD", ""))

            current_match       = {
                "matchup_id":  block.get("OI") or block.get("FI"),
                "home_team":   home,
                "away_team":   away,
                "start_time":  kickoff,
                "league_name": current_league,
                "markets":     [],
            }
            current_markets     = []
            current_market_name = None
            pending_prices      = []

        elif btype == "MA":
            # Market header — flush previous prices, start new market
            if pending_prices and current_market_name:
                _flush_market(current_markets, current_market_name, pending_prices)
                pending_prices = []
            current_market_name = block.get("NA", "").strip() or "1x2"

        elif btype == "PA" and block.get("OD") and not block.get("FD"):
            # Price row — NA= outcome name (Home/Tie/Away), OD= fractional odds
            decimal = fraction_to_decimal(block.get("OD", ""))
            pending_prices.append({
                "name":            block.get("NA"),
                "fractional_odds": block.get("OD"),
                "decimal_odds":    decimal,
            })

    # Flush final match
    if current_match:
        if pending_prices and current_market_name:
            _flush_market(current_markets, current_market_name, pending_prices)
        current_match["markets"] = current_markets
        matchups.append(current_match)

    return {
        "book":          "bet365",
        "league_key":    league_key,
        "tier":          league_meta["tier"],
        "country":       league_meta["country"],
        "scraped_at":    scraped_at,
        "matchup_count": len(matchups),
        "matchups":      matchups,
    }


def _flush_market(markets: list, name: str, prices: list):
    if prices:
        markets.append({"market_type": name, "prices": prices.copy()})


def _split_teams(fd: str):
    """Split 'Brentford v Wolverhampton' into ('Brentford', 'Wolverhampton')."""
    if " v " in fd:
        parts = fd.split(" v ", 1)
        return parts[0].strip(), parts[1].strip()
    return fd, None


def _parse_kickoff(bc: str) -> str | None:
    """Convert '20260316200000' → '2026-03-16T20:00:00Z'."""
    try:
        return f"{bc[:4]}-{bc[4:6]}-{bc[6:8]}T{bc[8:10]}:{bc[10:12]}:{bc[12:14]}Z"
    except Exception:
        return None


def save_locally(snapshot: dict, league_key: str) -> str:
    now      = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str   = now.strftime("%H%M%S")
    out_dir  = f"data/raw/bet365/{league_key}/{date_str}"
    os.makedirs(out_dir, exist_ok=True)

    filepath = f"{out_dir}/{ts_str}.json"
    with open(filepath, "w") as f:
        json.dump(snapshot, f, indent=2)

    print(f"    Saved → {filepath}  ({snapshot['matchup_count']} matches)")
    return filepath


async def scrape_league(page, league_key: str, league_meta: dict) -> dict | None:
    captured = {}

    async def handle_response(response):
        if "splashcontentapi" in response.url or "matchmarketscontentapi" in response.url:
            try:
                body = await response.body()
                text = body.decode("utf-8").strip()
                if text and text.startswith("F|"):
                    captured["text"] = text
                    captured["url"]  = response.url
            except Exception as e:
                print(f"    Capture error: {e}")

    page.on("response", handle_response)

    try:
        await page.goto(league_meta["url"], wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(5000)
    except Exception as e:
        print(f"    Navigation error: {e}")

    page.remove_listener("response", handle_response)

    if "text" not in captured:
        print(f"    No data captured")
        return None

    snapshot = parse_bet365_response(captured["text"], league_key, league_meta)
    return snapshot


async def run_async(use_s3: bool = False):
    print(f"\n[Bet365] Starting scrape — {datetime.now(timezone.utc).isoformat()}")
    print(f"[Bet365] Mode: {'S3' if use_s3 else 'local'} | Leagues: {len(LEAGUES)}\n")

    os.makedirs("data", exist_ok=True)

    success = 0
    failed  = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="en-US",
        )

        page = await context.new_page()

        print("  Initialising browser session...")
        try:
            await page.goto("https://www.nj.bet365.com/", wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(2000)
            print("  Session ready\n")
        except Exception as e:
            print(f"  Warning: homepage load issue — {e}\n")

        for league_key, league_meta in LEAGUES.items():
            print(f"  [{league_key}]")
            try:
                snapshot = await scrape_league(page, league_key, league_meta)

                if snapshot:
                    save_locally(snapshot, league_key)
                    success += 1
                else:
                    failed += 1

            except Exception as e:
                import traceback
                traceback.print_exc()
                failed += 1

            await asyncio.sleep(1)

        await browser.close()

    print(f"\n[Bet365] Done — {success} succeeded, {failed} failed\n")


def run(use_s3: bool = False):
    asyncio.run(run_async(use_s3))


if __name__ == "__main__":
    run(use_s3=False)