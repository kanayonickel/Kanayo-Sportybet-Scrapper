"""
Sportybet Real-Time Scraper Dashboard
I Call it Kanayo SportyGrab. 
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import asyncio
import json
import re
from playwright.async_api import async_playwright
from datetime import datetime, timedelta, timezone
from typing import List
import requests
from bs4 import BeautifulSoup
import dateutil.parser

app = FastAPI()

active_connections: List[WebSocket] = []

class SportybetScraper:
    def __init__(self):
        self.url_football = 'https://www.sportybet.com/ng/sport/football/upcoming?time=24'
        self.url_basketball = 'https://www.sportybet.com/ng/sport/basketball/upcoming?time=24'
        self.url_code_hub = 'https://www.sportybet.com/ng/m/code-hub/codes'
        self.results = []
        self.booking_codes = []
    
    async def send_update(self, message: dict):
        for connection in active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

    def is_recent(self, date_str):
        """Checks if a post is within the last 45 minutes"""
        try:
            clean_date = date_str.split('·')[0].strip()
            post_time = dateutil.parser.parse(clean_date)
            if post_time.tzinfo is None:
                post_time = post_time.replace(tzinfo=timezone.utc)
            
            now = datetime.now(timezone.utc)
            return (now - post_time) <= timedelta(minutes=45)
        except:
            return True

    def extract_6char_codes(self, text):
        """Strictly 6-character alphanumeric Sportybet codes"""
        found = re.findall(r'\b([A-Z0-9]{6})\b', text.upper())
        valid = []
        for c in found:
            if any(char.isdigit() for char in c) and any(char.isalpha() for char in c):
                if c not in ['SPORTY', 'BETGER', 'UPCOMI', 'VIRTUA', 'FOOTBA', 'BASKET']:
                    valid.append(c)
        return list(set(valid))

    async def scrape_football(self):
        await self.send_update({"type": "status", "message": "⚽ Scraping Football Odds...", "color": "blue"})
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            try:
                await page.goto(self.url_football, timeout=30000)
                await asyncio.sleep(5)
                matches = await page.query_selector_all('.m-table-row')
                for match in matches[:15]:
                    teams = await match.query_selector('.teams')
                    if not teams: continue
                    h = await (await teams.query_selector('.home-team')).inner_text()
                    a = await (await teams.query_selector('.away-team')).inner_text()
                    odds = await match.query_selector_all('.m-outcome-odds')
                    if len(odds) >= 3:
                        res = {
                            "id": len(self.results) + 1,
                            "match": f"{h} vs {a}",
                            "sport": "Football",
                            "market": "1X2",
                            "odds_value": await odds[0].inner_text(),
                            "timestamp": datetime.now().strftime("%H:%M:%S")
                        }
                        self.results.append(res)
                        await self.send_update({"type": "result", "data": res})
            finally:
                await browser.close()

    async def scrape_basketball(self, min_odds=1.0):
        await self.send_update({"type": "status", "message": f"🏀 Scraping Basketball (min {min_odds})...", "color": "blue"})
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            try:
                await page.goto(self.url_basketball, timeout=30000)
                await asyncio.sleep(5)
                matches = await page.query_selector_all('.m-table-row')
                for match in matches[:15]:
                    teams = await match.query_selector('.teams')
                    if not teams: continue
                    h = await (await teams.query_selector('.home-team')).inner_text()
                    a = await (await teams.query_selector('.away-team')).inner_text()
                    odds = await match.query_selector_all('.m-outcome-odds')
                    if odds:
                        val = await odds[0].inner_text()
                        if float(val) >= min_odds:
                            res = {"id": len(self.results) + 1, "match": f"{h} vs {a}", "sport": "Basketball", "market": "1X2", "odds_value": val, "timestamp": datetime.now().strftime("%H:%M:%S")}
                            self.results.append(res)
                            await self.send_update({"type": "result", "data": res})
            finally:
                await browser.close()

    async def scrape_official_hub(self, sport, target_min_odds, processed_codes, codes_found_so_far):
        """Scrape from Sportybet official code hub"""
        await self.send_update({"type": "status", "message": f"📱 Scraping Official Hub...", "color": "blue"})
        
        codes_found = codes_found_so_far
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            context = await browser.new_context(
                viewport={'width': 375, 'height': 812},
                user_agent='Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15'
            )
            page = await context.new_page()
            
            try:
                await page.goto(self.url_code_hub, timeout=30000)
                await asyncio.sleep(5)
                
                # Get all text content
                page_text = await page.evaluate('() => document.body.innerText')
                
                # Find all potential code containers
                code_containers = await page.query_selector_all('div[class*="code"], div[class*="card"], div[class*="item"]')
                
                # Method 1: Structured extraction
                for container in code_containers:
                    if codes_found >= 10:
                        break
                    
                    try:
                        container_text = await container.inner_text()
                        codes = self.extract_6char_codes(container_text)
                        
                        for code in codes:
                            if code in processed_codes or codes_found >= 10:
                                continue
                            
                            odds_match = re.search(r'Odds?:\s*([\d,]+\.?\d*)', container_text, re.IGNORECASE)
                            
                            if odds_match:
                                odds_str = odds_match.group(1).replace(',', '')
                                try:
                                    current_odds = float(odds_str)
                                    
                                    if current_odds >= target_min_odds:
                                        processed_codes.add(code)
                                        codes_found += 1
                                        
                                        res = {
                                            "code": code,
                                            "source": "Official Hub",
                                            "sport": sport.capitalize(),
                                            "odds": current_odds,
                                            "status": "1K+ Official",
                                            "timestamp": datetime.now().strftime("%H:%M:%S")
                                        }
                                        self.booking_codes.append(res)
                                        await self.send_update({"type": "booking_code", "data": res})
                                        await asyncio.sleep(0.2)
                                except ValueError:
                                    continue
                    except Exception as e:
                        continue
                
                # Method 2: Full text parsing if needed
                if codes_found < 10:
                    all_codes = self.extract_6char_codes(page_text)
                    
                    for code in all_codes:
                        if code in processed_codes or codes_found >= 10:
                            break
                        
                        code_pos = page_text.upper().find(code)
                        if code_pos != -1:
                            context_start = max(0, code_pos - 300)
                            context_end = min(len(page_text), code_pos + 300)
                            context = page_text[context_start:context_end]
                            
                            odds_match = re.search(r'Odds?:\s*([\d,]+\.?\d*)', context, re.IGNORECASE)
                            
                            if odds_match:
                                odds_str = odds_match.group(1).replace(',', '')
                                try:
                                    current_odds = float(odds_str)
                                    
                                    if current_odds >= target_min_odds:
                                        processed_codes.add(code)
                                        codes_found += 1
                                        
                                        res = {
                                            "code": code,
                                            "source": "Official Hub",
                                            "sport": sport.capitalize(),
                                            "odds": current_odds,
                                            "status": "1K+ Official",
                                            "timestamp": datetime.now().strftime("%H:%M:%S")
                                        }
                                        self.booking_codes.append(res)
                                        await self.send_update({"type": "booking_code", "data": res})
                                        await asyncio.sleep(0.2)
                                except ValueError:
                                    continue
                
                await self.send_update({
                    "type": "status",
                    "message": f"✓ Official Hub: Found {codes_found - codes_found_so_far} codes",
                    "color": "green"
                })
                
            except Exception as e:
                await self.send_update({
                    "type": "status",
                    "message": f"Official Hub error: {str(e)}",
                    "color": "red"
                })
            finally:
                await browser.close()
        
        return codes_found

    async def scrape_twitter(self, sport, target_min_odds, processed_codes, codes_found_so_far):
        """Scrape from Twitter with 45min filter"""
        await self.send_update({"type": "status", "message": f"🐦 Scraping Twitter (Last 45min)...", "color": "blue"})
        
        codes_found = codes_found_so_far
        
        # Pool of Nitter instances
        nitter_instances = [
            "https://nitter.net",
            "https://nitter.poast.org",
            "https://nitter.privacydev.net",
            "https://nitter.unixfox.eu",
            "https://nitter.1d4.us"
        ]
        
        search_query = f"sportybet booking code {sport}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        
        response = None
        
        # Try each Nitter instance
        for instance in nitter_instances:
            if codes_found >= 10:
                break
            
            try:
                nitter_url = f"{instance}/search?f=tweets&q={search_query.replace(' ', '%20')}"
                response = await asyncio.to_thread(
                    requests.get, nitter_url, headers=headers, timeout=10
                )
                
                if response.status_code == 200:
                    await self.send_update({
                        "type": "status",
                        "message": f"✓ Connected to {instance}",
                        "color": "green"
                    })
                    break
            except:
                continue
        
        if not response or response.status_code != 200:
            await self.send_update({
                "type": "status",
                "message": "Twitter: All Nitter instances failed",
                "color": "red"
            })
            return codes_found
        
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            tweets = soup.find_all('div', class_='timeline-item')
            
            if not tweets:
                tweets = soup.find_all('div', class_='tweet-content')
            
            for tweet in tweets[:50]:
                if codes_found >= 10:
                    break
                
                try:
                    # Check timestamp
                    time_elem = tweet.find('span', class_='tweet-date')
                    if not time_elem:
                        time_elem = tweet.find('a', class_='tweet-link')
                    
                    tweet_time_str = time_elem.get('title', '') if time_elem else ''
                    
                    # Apply 45-minute filter
                    if tweet_time_str and not self.is_recent(tweet_time_str):
                        continue
                    
                    # Get tweet text
                    content_elem = tweet.find('div', class_='tweet-content')
                    if not content_elem:
                        content_elem = tweet
                    
                    text = content_elem.get_text()
                    codes = self.extract_6char_codes(text)
                    
                    for code in codes:
                        if code in processed_codes or codes_found >= 10:
                            continue
                        
                        # Extract odds
                        odds_patterns = [
                            r'odds?:\s*([\d,]+\.?\d*)',
                            r'([\d,]+\.?\d*)\s*odds?',
                            r'@\s*([\d,]+\.?\d*)',
                            r'([\d,]+\.?\d*)[xX]',
                        ]
                        
                        current_odds = None
                        for pattern in odds_patterns:
                            odds_match = re.search(pattern, text, re.IGNORECASE)
                            if odds_match:
                                try:
                                    odds_str = odds_match.group(1).replace(',', '')
                                    current_odds = float(odds_str)
                                    break
                                except:
                                    continue
                        
                        if current_odds and current_odds >= target_min_odds:
                            processed_codes.add(code)
                            codes_found += 1
                            
                            res = {
                                "code": code,
                                "source": "Twitter (45min)",
                                "sport": sport.capitalize(),
                                "odds": current_odds,
                                "status": "1K+ Recent",
                                "timestamp": datetime.now().strftime("%H:%M:%S")
                            }
                            self.booking_codes.append(res)
                            await self.send_update({"type": "booking_code", "data": res})
                            await asyncio.sleep(0.2)
                
                except Exception as e:
                    continue
            
            await self.send_update({
                "type": "status",
                "message": f"✓ Twitter: Found {codes_found - codes_found_so_far} codes",
                "color": "green"
            })
        
        except Exception as e:
            await self.send_update({
                "type": "status",
                "message": f"Twitter error: {str(e)}",
                "color": "red"
            })
        
        return codes_found

    async def scrape_booking_codes(self, sport, target_min_odds=1000):
        """Hybrid scraper - Both Official Hub + Twitter (45min filter)"""
        await self.send_update({
            "type": "status",
            "message": f"🎯 Searching BOTH sources for {sport} codes (Odds >= {target_min_odds})...",
            "color": "blue"
        })
        
        processed_codes = set()
        codes_found = 0
        
        # Run both scrapers in parallel
        official_task = asyncio.create_task(
            self.scrape_official_hub(sport, target_min_odds, processed_codes, codes_found)
        )
        twitter_task = asyncio.create_task(
            self.scrape_twitter(sport, target_min_odds, processed_codes, 0)
        )
        
        # Wait for both to complete
        results = await asyncio.gather(official_task, twitter_task, return_exceptions=True)
        
        # Count total codes from both sources
        total_codes = len(processed_codes)
        
        if total_codes == 0:
            await self.send_update({
                "type": "status",
                "message": "No codes found from either source with odds >= 1000",
                "color": "red"
            })
        else:
            await self.send_update({
                "type": "status",
                "message": f"✅ TOTAL: Found {total_codes} codes from both sources!",
                "color": "green"
            })

scraper = SportybetScraper()

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8"><title>Sportybet 1K Scraper</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-900 text-slate-100 min-h-screen p-8">
    <div class="max-w-6xl mx-auto">
        <h1 class="text-4xl font-bold mb-2">🎯 Kanayo SportyGrab</h1>
        <p class="text-slate-400 mb-8">Filtering codes with total odds of 1,000 and above</p>

        <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
            <button onclick="scrapeFootball()" class="bg-slate-800 border border-slate-700 p-6 rounded-lg hover:bg-slate-700">⚽ Scrape Football</button>
            <button onclick="scrapeBookingCodes('basketball')" class="bg-green-900 border border-green-700 p-6 rounded-lg hover:bg-green-800">🏀 Basketball Codes (1K+)</button>
            <button onclick="scrapeBookingCodes('football')" class="bg-blue-900 border border-blue-700 p-6 rounded-lg hover:bg-blue-800">⚽ Football Codes (1K+)</button>
        </div>

        <div id="statusLog" class="bg-black p-4 rounded mb-8 font-mono text-xs h-32 overflow-y-auto border border-slate-700"></div>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
            <div class="bg-slate-800 p-6 rounded border border-slate-700">
                <h2 class="text-xl font-bold mb-4">Odds Results</h2>
                <table class="w-full text-left text-sm">
                    <thead class="text-slate-500 border-b border-slate-700"><tr><th>Match</th><th>Odds</th><th>Time</th></tr></thead>
                    <tbody id="resultsTable"></tbody>
                </table>
            </div>
            <div class="bg-slate-800 p-6 rounded border border-slate-700">
                <h2 class="text-xl font-bold mb-4 text-green-400">🎫 Booking Codes (Odds >= 1000)</h2>
                <table class="w-full text-left text-sm">
                    <thead class="text-slate-500 border-b border-slate-700"><tr><th>Code</th><th>Odds</th><th>Status</th></tr></thead>
                    <tbody id="codesTable"></tbody>
                </table>
            </div>
        </div>
    </div>
    <script>
        let ws;
        function connect() {
            ws = new WebSocket(`ws://${window.location.host}/ws`);
            ws.onmessage = (e) => {
                const msg = JSON.parse(e.data);
                if (msg.type === 'status') addLog(msg.message, msg.color);
                if (msg.type === 'result') addResult(msg.data);
                if (msg.type === 'booking_code') addBookingCode(msg.data);
            };
        }
        function addLog(m, c) {
            const div = document.getElementById('statusLog');
            div.innerHTML = `<p class="text-${c || 'slate'}-400">[${new Date().toLocaleTimeString()}] ${m}</p>` + div.innerHTML;
        }
        function addResult(d) {
            const tb = document.getElementById('resultsTable');
            tb.innerHTML = `<tr class="border-b border-slate-700"><td class="py-2">${d.match}</td><td class="text-yellow-400">${d.odds_value}</td><td>${d.timestamp}</td></tr>` + tb.innerHTML;
        }
        function addBookingCode(d) {
            const tb = document.getElementById('codesTable');
            tb.innerHTML = `<tr><td class="py-3 font-bold text-2xl text-green-400 font-mono">${d.code}</td><td class="text-yellow-500 font-bold">${d.odds}</td><td><span class="bg-green-900 text-green-200 px-2 py-1 rounded text-xs">${d.status}</span></td></tr>` + tb.innerHTML;
        }
        function scrapeFootball() { fetch('/scrape/football', {method:'POST'}); }
        function scrapeBookingCodes(s) { fetch(`/scrape/booking-codes?sport=${s}`, {method:'POST'}); }
        connect();
    </script>
</body>
</html>
    """

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept(); active_connections.append(websocket)
    try:
        while True: await websocket.receive_text()
    except: active_connections.remove(websocket)

@app.post("/scrape/football")
async def s_f(): asyncio.create_task(scraper.scrape_football()); return {"status": "started"}

@app.post("/scrape/booking-codes")
async def s_bc(sport: str): 
    asyncio.create_task(scraper.scrape_booking_codes(sport, 1000))
    return {"status": "started"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)