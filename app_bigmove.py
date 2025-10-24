"""
Flask Big Move Detection Dashboard
Real-time market analysis with scoring and alerts
"""
import os
import asyncio
import json
import ssl
import threading
import time
import random
from datetime import datetime
from collections import deque
from flask import Flask, Response, render_template, jsonify
from flask_cors import CORS
from dotenv import load_dotenv  

# Import detector
from bigmove_detector import BigMoveDetector
load_dotenv()
# Configuration
USE_MOCK = os.getenv('USE_MOCK', 'False').lower() == 'true'
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN', '')
DEFAULT_INSTRUMENTS = [
    "NSE_FO|59022",  # Options for Greeks
    "NSE_FO|59023",
]

app = Flask(__name__)
CORS(app)

# Import based on mode
if not USE_MOCK:
    import websockets
    import requests
    from google.protobuf.json_format import MessageToDict
    import MarketDataFeedV3_pb2 as pb

# Global state
class GlobalState:
    def __init__(self):
        self.clients = []
        self.upstox_connected = False
        self.market_data_buffer = deque(maxlen=100)
        self.analysis_results = {}  # Store latest analysis per symbol
        self.lock = threading.Lock()
        self.message_count = 0
        self.detector = BigMoveDetector()

state = GlobalState()

# Mock Data Generator with full market data
class MockMarketDataService:
    def __init__(self):
        self.running = False
        self.instruments = {
            "NSE_FO|NIFTY25JAN50CE": {"base": 250, "name": "NIFTY 50 CE"},
            "NSE_FO|BANKNIFTY25JAN50CE": {"base": 180, "name": "BANKNIFTY CE"},
            "NSE_INDEX|Nifty 50": {"base": 19500, "name": "Nifty 50"},
            "NSE_INDEX|Nifty Bank": {"base": 43000, "name": "Nifty Bank"}
        }
        self.prices = {key: data["base"] for key, data in self.instruments.items()}
        self.volumes = {key: random.randint(100000, 500000) for key in self.instruments.keys()}
    
    def generate_full_feed(self, instrument_key: str, config: dict) -> dict:
        """Generate complete market feed with Greeks"""
        # Simulate price movement
        change = random.uniform(-1.0, 1.0)
        self.prices[instrument_key] += (self.prices[instrument_key] * change / 100)
        ltp = round(self.prices[instrument_key], 2)
        cp = config["base"]
        
        # Volume spike simulation (occasionally)
        vol_spike = random.choice([1, 1, 1, 1, 3, 5, 8])  # 25% chance of spike
        self.volumes[instrument_key] = int(self.volumes[instrument_key] * (0.95 + random.random() * 0.1) * vol_spike)
        
        # Generate OHLC
        high = ltp * (1 + random.uniform(0, 0.02))
        low = ltp * (1 - random.uniform(0, 0.02))
        
        feed = {
            "fullFeed": {
                "marketFF": {
                    "ltpc": {
                        "ltp": ltp,
                        "ltt": int(time.time() * 1000),
                        "ltq": random.randint(100, 10000),
                        "cp": cp
                    },
                    "marketLevel": {
                        "bidAskQuote": [
                            {
                                "bidQ": random.randint(1000, 50000),
                                "bidP": ltp - random.uniform(0.1, 5),
                                "askQ": random.randint(1000, 50000),
                                "askP": ltp + random.uniform(0.1, 5)
                            }
                        ]
                    },
                    "optionGreeks": {
                        "delta": random.uniform(0.3, 0.9) * random.choice([1, -1]),
                        "gamma": random.uniform(0.0001, 0.003),
                        "theta": random.uniform(-0.1, -0.01),
                        "vega": random.uniform(0.01, 0.1),
                        "rho": random.uniform(-0.01, 0.01)
                    },
                    "marketOHLC": {
                        "ohlc": [
                            {
                                "interval": "I1",
                                "open": cp,
                                "high": high,
                                "low": low,
                                "close": ltp,
                                "vol": self.volumes[instrument_key],
                                "ts": int(time.time() * 1000)
                            }
                        ]
                    },
                    "atp": ltp * random.uniform(0.98, 1.02),
                    "vtt": self.volumes[instrument_key],
                    "oi": random.randint(100000, 1000000),
                    "iv": random.uniform(0.15, 0.35),
                    "tbq": random.randint(50000, 500000) * random.choice([2, 3, 1]),
                    "tsq": random.randint(50000, 500000)
                }
            },
            "requestMode": "full"
        }
        
        return feed
    
    def generate_feed(self):
        """Generate mock market feed for all instruments"""
        feeds = {}
        for instrument_key, config in self.instruments.items():
            feeds[instrument_key] = self.generate_full_feed(instrument_key, config)
        
        return {
            "type": "live_feed",
            "feeds": feeds,
            "currentTs": str(int(time.time() * 1000)),
            "server_timestamp": datetime.now().isoformat(),
            "message_id": state.message_count
        }
    
    def start(self):
        """Start mock data generation"""
        self.running = True
        state.upstox_connected = True
        print('🎭 Mock Market Data Service Started')
        
        # Generate data every 500ms
        while self.running:
            feed = self.generate_feed()
            self.broadcast_data(feed)
            time.sleep(0.5)
    
    def broadcast_data(self, data):
        """Broadcast data and run analysis"""
        # Run Big Move Detection
        if 'feeds' in data:
            for symbol, feed_value in data['feeds'].items():
                analysis = state.detector.analyze(symbol, feed_value)
                if analysis:
                    with state.lock:
                        state.analysis_results[symbol] = analysis
        
        with state.lock:
            state.message_count += 1
            state.market_data_buffer.append(data)
        
        for client_queue in state.clients:
            try:
                client_queue.append(data)
            except:
                pass
    
    def stop(self):
        self.running = False

# Real Upstox Service (abbreviated - same as before but with analysis)
if not USE_MOCK:
    class MarketDataService:
        def __init__(self):
            self.websocket = None
            self.running = False
            self.loop = None
        
        def get_market_feed_url(self):
            headers = {
                'Accept': 'application/json',
                'Authorization': f'Bearer {ACCESS_TOKEN}'
            }
            url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
            response = requests.get(url=url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()['data']['authorized_redirect_uri']
        
        def decode_protobuf(self, buffer):
            try:
                feed_response = pb.FeedResponse()
                feed_response.ParseFromString(buffer)
                return MessageToDict(feed_response)
            except Exception as e:
                print(f'❌ Protobuf decode error: {e}')
                return None
        
        def broadcast_with_analysis(self, data):
            """Broadcast data with Big Move analysis"""
            if 'feeds' in data:
                for symbol, feed_value in data['feeds'].items():
                    analysis = state.detector.analyze(symbol, feed_value)
                    if analysis:
                        with state.lock:
                            state.analysis_results[symbol] = analysis
            
            with state.lock:
                state.message_count += 1
                state.market_data_buffer.append(data)
            
            for client_queue in state.clients:
                try:
                    client_queue.append(data)
                except:
                    pass
        
        async def connect_and_stream(self):
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            retry_count = 0
            max_retries = 5
            
            while self.running and retry_count < max_retries:
                try:
                    ws_url = self.get_market_feed_url()
                    async with websockets.connect(ws_url, ssl=ssl_context, ping_interval=30) as websocket:
                        self.websocket = websocket
                        state.upstox_connected = True
                        print('✅ Connected to Upstox')
                        
                        await asyncio.sleep(1)
                        
                        subscription = {
                            "guid": "bigmove_detector",
                            "method": "sub",
                            "data": {
                                "mode": "full",
                                "instrumentKeys": DEFAULT_INSTRUMENTS
                            }
                        }
                        
                        await websocket.send(json.dumps(subscription).encode('utf-8'))
                        print(f'📡 Subscribed to: {DEFAULT_INSTRUMENTS}')
                        
                        while self.running:
                            try:
                                message = await asyncio.wait_for(websocket.recv(), timeout=60)
                                decoded_data = self.decode_protobuf(message)
                                
                                if decoded_data:
                                    decoded_data['server_timestamp'] = datetime.now().isoformat()
                                    self.broadcast_with_analysis(decoded_data)
                                    
                            except asyncio.TimeoutError:
                                await websocket.ping()
                                
                except Exception as e:
                    retry_count += 1
                    print(f'❌ Error: {e}')
                    await asyncio.sleep(min(5 * retry_count, 30))
        
        def start(self):
            self.running = True
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self.connect_and_stream())

# Initialize service
if USE_MOCK:
    market_service = MockMarketDataService()
    print('🎭 Using MOCK data mode')
else:
    market_service = MarketDataService()
    print('📡 Using REAL Upstox data')

def start_market_service():
    if not hasattr(start_market_service, 'started'):
        thread = threading.Thread(target=market_service.start, daemon=True)
        thread.start()
        start_market_service.started = True
        print('🚀 Market data service started')

# SSE generator
def generate_sse():
    client_queue = deque(maxlen=100)
    state.clients.append(client_queue)
    
    try:
        yield f"data: {json.dumps({'type': 'connection', 'message': 'connected'})}\n\n"
        
        last_heartbeat = time.time()
        
        while True:
            if time.time() - last_heartbeat > 30:
                yield f": heartbeat\n\n"
                last_heartbeat = time.time()
            
            if client_queue:
                while client_queue:
                    data = client_queue.popleft()
                    yield f"data: {json.dumps(data)}\n\n"
            else:
                time.sleep(0.1)
                
    finally:
        state.clients.remove(client_queue)

# Routes
@app.route('/')
def index():
    return render_template('bigmove_dashboard.html')

@app.route('/stream')
def stream():
    return Response(generate_sse(), mimetype='text/event-stream',
                   headers={'Cache-Control': 'no-cache', 'Connection': 'keep-alive'})

@app.route('/api/analysis')
def get_analysis():
    """Get current analysis results"""
    with state.lock:
        return jsonify({
            'results': list(state.analysis_results.values()),
            'timestamp': datetime.now().isoformat()
        })

@app.route('/health')
def health():
    return jsonify({
        'status': 'ok',
        'mode': 'mock' if USE_MOCK else 'real',
        'upstox': 'connected' if state.upstox_connected else 'disconnected',
        'instruments_analyzed': len(state.analysis_results),
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    print('\n' + '='*60)
    print('🚀 Big Move Detection Dashboard')
    print('='*60)
    print(f'🎭 Mode: {"MOCK" if USE_MOCK else "REAL"}')
    print(f'📊 Dashboard: http://localhost:5000/')
    print('='*60 + '\n')
    
    start_market_service()
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)