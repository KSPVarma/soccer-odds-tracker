import json
import os
import asyncio
from datetime import datetime, timezone
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

LEAGUES = {
    "premier_league":   {"tier": 1, "country": "England",     "competition_id": 10932509, "name": "English Premier League"},
    "champions_league": {"tier": 1, "country": "Europe",      "competition_id": 228,      "name": "UEFA Champions League"},
    "la_liga":          {"tier": 1, "country": "Spain",       "competition_id": 117,      "name": "Spanish La Liga"},
    "bundesliga":       {"tier": 1, "country": "Germany",     "competition_id": 59,       "name": "German Bundesliga"},
    "serie_a":          {"tier": 2, "country": "Italy",       "competition_id": 81,       "name": "Italian Serie A"},
    "ligue_1":          {"tier": 2, "country": "France",      "competition_id": 55,       "name": "French Ligue 1"},
    "mls":              {"tier": 2, "country": "USA",         "competition_id": 141,      "name": "MLS"},
    "eredivisie":       {"tier": 3, "country": "Netherlands", "competition_id": 73,       "name": "Dutch Eredivisie"},
    "brasileirao":      {"tier": 3, "country": "Brazil",      "competition_id": 12199697, "name": "Brazilian Serie A"},
    "scottish_prem":    {"tier": 3, "country": "Scotland",    "competition_id": 45,       "name": "Scottish Premiership"},
}

BASE_URL = "https://sportsbook.fanduel.com"


def parse_fanduel_response(data: dict, league_key: str, league_meta: dict) -> dict:
    """
    Parse FanDuel's content-managed-page response into our standard format.
    FanDuel structure: attachments -> markets -> [events] -> outcomes
    """
    scraped_at = datetime.now(timezone.utc).isoformat()
    matchups   = []

    try:
        attachments = data.get("attachments", {})
        events      = attachments.get("events", {})
        markets     = attachments.get("markets", {})

        for event_id, event in events.items():
            home_team  = None
            away_team  = None

            # Extract team names from runners
            runners = event.get("runners", [])
            for runner in runners:
                if runner.get("handicap") == 0:
                    if not home_team:
                        home_team = runner.get("runnerName")
                    else:
                        away_team = runner.get("runnerName")

            # FanDuel uses home/away in the name field separated by " v "
            name = event.get("name", "")
            if " v " in name and not home_team:
                parts     = name.split(" v ", 1)
                home_team = parts[0].strip()
                away_team = parts[1].strip()

            start_time = event.get("openDate", "")

            # Find markets for this event
            event_markets = []
            for market_id, market in markets.items():
                if str(market.get("eventId")) != str(event_id):
                    continue

                market_name = market.get("marketName", "")
                if market_name not in ["Match Result", "Match Odds", "1X2"]:
                    continue

                runners_odds = market.get("runners", [])
                prices       = []
                for runner in runners_odds:
                    price = runner.get("winRunnerOdds", {})
                    dec   = price.get("trueOdds", {}).get("decimalOdds", {}).get("decimalOdds")
                    prices.append({
                        "name":         runner.get("runnerName"),
                        "decimal_odds": dec,
                        "american_odds": price.get("americanDisplayOdds", {}).get("americanOdds"),
                    })

                event_markets.append({
                    "market_type": market_name,
                    "market_id":   market_id,
                    "prices":      prices,
                })

            if home_team or away_team:
                matchups.append({
                    "matchup_id":  event_id,
                    "home_team":   home_team,
                    "away_team":   away_team,
                    "start_time":  start_time,
                    "markets":     event_markets,
                })

    except Exception as e:
        print(f"    Parse error: {e}")

    return {
        "book":          "fanduel",
        "league_key":    league_key,
        "tier":          league_meta["tier"],
        "country":       league_meta["country"],
        "scraped_at":    scraped_at,
        "matchup_count": len(matchups),
        "matchups":      matchups,
    }


def save_locally(snapshot: dict, league_key: str) -> str:
    now      = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str   = now.strftime("%H%M%S")
    out_dir  = f"data/raw/fanduel/{league_key}/{date_str}"
    os.makedirs(out_dir, exist_ok=True)

    filepath = f"{out_dir}/{ts_str}.json"
    with open(filepath, "w") as f:
        json.dump(snapshot, f, indent=2)

    print(f"    Saved → {filepath}  ({snapshot['matchup_count']} matches)")
    return filepath


async def scrape_league(page, league_key: str, league_meta: dict) -> dict | None:
    captured = {}
    comp_id  = league_meta["competition_id"]

    async def handle_response(response):
        url = response.url
        if "content-managed-page" in url and "eventTypeId=1" in url:
            try:
                body = await response.body()
                text = body.decode("utf-8").strip()
                if text and text.startswith("{"):
                    data = json.loads(text)
                    # Filter to only events for our competition
                    attachments = data.get("attachments", {})
                    events      = attachments.get("events", {})
                    filtered    = {
                        k: v for k, v in events.items()
                        if v.get("competitionId") == comp_id
                    }
                    if filtered:
                        data["attachments"]["events"] = filtered
                        captured["data"] = data
                        print(f"    Captured {len(filtered)} events for competition {comp_id}")
            except Exception as e:
                print(f"    Capture error: {e}")

    page.on("response", handle_response)

    try:
        url = f"{BASE_URL}/soccer"
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(6000)
    except Exception as e:
        print(f"    Navigation error: {e}")

    page.remove_listener("response", handle_response)

    if "data" not in captured:
        print(f"    No data captured for {league_key}")
        return None

    return parse_fanduel_response(captured["data"], league_key, league_meta)


async def run_async(use_s3: bool = False):
    print(f"\n[FanDuel] Starting scrape — {datetime.now(timezone.utc).isoformat()}")
    print(f"[FanDuel] Mode: {'S3' if use_s3 else 'local'} | Leagues: {len(LEAGUES)}\n")

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
            await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(2000)
            print("  Session ready\n")
        except Exception as e:
            print(f"  Warning: {e}\n")

        # FanDuel loads all competitions on one page
        # so we only navigate once and filter by competition ID
        print("  Loading FanDuel soccer page...")
        captured_all = {}

        async def handle_all(response):
            if "content-managed-page" in response.url and "eventTypeId=1" in response.url:
                try:
                    body = await response.body()
                    text = body.decode("utf-8").strip()
                    if text.startswith("{"):
                        captured_all["data"] = json.loads(text)
                        print(f"  Full soccer page captured")
                except Exception as e:
                    print(f"  Capture error: {e}")

        page.on("response", handle_all)
        try:
            await page.goto(f"{BASE_URL}/soccer", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(6000)
        except Exception as e:
            print(f"  Navigation error: {e}")
        page.remove_listener("response", handle_all)

        if "data" not in captured_all:
            print("  Could not capture FanDuel soccer page")
            await browser.close()
            return

        full_data   = captured_all["data"]
        attachments = full_data.get("attachments", {})
        all_events  = attachments.get("events", {})
        all_markets = attachments.get("markets", {})

        print(f"  Total events on page: {len(all_events)}\n")

        for league_key, league_meta in LEAGUES.items():
            print(f"  [{league_key}]")
            comp_id = league_meta["competition_id"]

            try:
                # Filter events and markets for this competition
                filtered_events  = {
                    k: v for k, v in all_events.items()
                    if v.get("competitionId") == comp_id
                }
                filtered_markets = {
                    k: v for k, v in all_markets.items()
                    if str(v.get("eventId")) in filtered_events
                }

                league_data = {
                    "attachments": {
                        "events":  filtered_events,
                        "markets": filtered_markets,
                    }
                }

                snapshot = parse_fanduel_response(league_data, league_key, league_meta)
                print(f"    {len(filtered_events)} events found")

                save_locally(snapshot, league_key)
                success += 1

            except Exception as e:
                import traceback
                traceback.print_exc()
                failed += 1

        await browser.close()

    print(f"\n[FanDuel] Done — {success} succeeded, {failed} failed\n")


def run(use_s3: bool = False):
    asyncio.run(run_async(use_s3))


if __name__ == "__main__":
    run(use_s3=False)