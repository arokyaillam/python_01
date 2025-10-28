"""
Complete Market Analysis Dashboard - LIVE DATA ONLY
Big Move Detection + Real-Time Trading Signals
Upstox WebSocket Integration
"""
import os
import asyncio
import json
import ssl
import threading
import time
from datetime import datetime
from collections import deque
from flask import Flask, Response, render_template, jsonify
from flask_cors import CORS
from dotenv import load_dotenv


import websockets
import requests
from google.protobuf.json_format import MessageToDict
import MarketDataFeedV3_pb2 as pb

# Import analyzers
from bigmove_detector import BigMoveDetector
from signal_analyzer_strong import StrongSignalAnalyzer  # Strong signals only
# Load environment variables
load_dotenv()

# Configuration
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN', 'YOUR_ACCESS_TOKEN')
DEFAULT_INSTRUMENTS = [
    "NSE_FO|58998",
    "NSE_FO|59003",
    
]

app = Flask(__name__)
CORS(app)

# Global state
class GlobalState:
    def __init__(self):
        self.clients = []
        self.signal_clients = []
        self.upstox_connected = False
        self.market_data_buffer = deque(maxlen=100)
        self.analysis_results = {}
        self.signal_results = {}
        self.lock = threading.Lock()
        self.message_count = 0
        self.bigmove_detector = BigMoveDetector()
        self.signal_analyzer = StrongSignalAnalyzer(lookback_ticks=5)

state = GlobalState()

# Live Upstox Service
class UpstoxMarketDataService:
    def __init__(self):
        self.websocket = None
        self.running = False
        self.loop = None
        
    def get_market_feed_url(self):
        """Get WebSocket URL from Upstox API"""
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {ACCESS_TOKEN}'
        }
        url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
        
        try:
            response = requests.get(url=url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()['data']['authorized_redirect_uri']
        except Exception as e:
            print(f'❌ Failed to get market feed URL: {e}')
            raise
    
    def decode_protobuf(self, buffer):
        """Decode protobuf message"""
        try:
            feed_response = pb.FeedResponse()
            feed_response.ParseFromString(buffer)
            return MessageToDict(feed_response)
        except Exception as e:
            print(f'❌ Protobuf decode error: {e}')
            return None
    
    def broadcast_data(self, data):
        """Broadcast data with analysis"""
        if 'feeds' in data:
            for symbol, feed_value in data['feeds'].items():
                # Run Big Move Detection
                try:
                    bigmove_analysis = state.bigmove_detector.analyze(symbol, feed_value)
                    if bigmove_analysis:
                        with state.lock:
                            state.analysis_results[symbol] = bigmove_analysis
                except Exception as e:
                    print(f"❌ Big Move analysis error for {symbol}: {e}")
                
                # Run Signal Analysis
                try:
                    mff = None
                    if 'fullFeed' in feed_value and 'marketFF' in feed_value['fullFeed']:
                        mff = feed_value['fullFeed']['marketFF']
                    elif 'fullFeed' in feed_value and 'indexFF' in feed_value['fullFeed']:
                        mff = feed_value['fullFeed']['indexFF']
                    
                    if mff:
                        signal_analysis = state.signal_analyzer.analyze_tick(symbol, mff)
                        if signal_analysis:
                            with state.lock:
                                state.signal_results[symbol] = signal_analysis
                except Exception as e:
                    print(f"❌ Signal analysis error for {symbol}: {e}")
        
        with state.lock:
            state.message_count += 1
            state.market_data_buffer.append(data)
        
        # Broadcast to main clients
        for client_queue in state.clients:
            try:
                client_queue.append(data)
            except Exception as e:
                print(f"❌ Broadcast to client error: {e}")
        
        # Broadcast signals with error handling
        try:
            signal_data = {
                'type': 'signal_update',
                'analyses': dict(state.signal_results),
                'timestamp': datetime.now().isoformat()
            }
            
            for client_queue in state.signal_clients:
                try:
                    client_queue.append(signal_data)
                except Exception as e:
                    print(f"❌ Signal broadcast error: {e}")
        except Exception as e:
            print(f"❌ Signal data creation error: {e}")
    
    async def connect_and_stream(self):
        """Connect to Upstox WebSocket and stream data"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        retry_count = 0
        max_retries = 5
        
        while self.running and retry_count < max_retries:
            try:
                ws_url = self.get_market_feed_url()
                print(f'🔗 Connecting to Upstox... (Attempt {retry_count + 1})')
                
                async with websockets.connect(ws_url, ssl=ssl_context, ping_interval=30) as websocket:
                    self.websocket = websocket
                    state.upstox_connected = True
                    retry_count = 0
                    print('✅ Connected to Upstox')
                    
                    await asyncio.sleep(1)
                    
                    # Subscribe to instruments
                    subscription = {
                        "guid": "live_market_feed",
                        "method": "sub",
                        "data": {
                            "mode": "full",
                            "instrumentKeys": DEFAULT_INSTRUMENTS
                        }
                    }
                    
                    await websocket.send(json.dumps(subscription).encode('utf-8'))
                    print(f'📡 Subscribed to: {DEFAULT_INSTRUMENTS}')
                    
                    # Receive and process data
                    while self.running:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=60)
                            decoded_data = self.decode_protobuf(message)
                            
                            if decoded_data:
                                decoded_data['server_timestamp'] = datetime.now().isoformat()
                                decoded_data['message_id'] = state.message_count
                                self.broadcast_data(decoded_data)
                                
                        except asyncio.TimeoutError:
                            print('⚠️ WebSocket timeout, sending ping...')
                            await websocket.ping()
                            
                        except websockets.exceptions.ConnectionClosed:
                            print('❌ WebSocket connection closed')
                    state.upstox_connected = False
                    break
                    
            except Exception as e:
                retry_count += 1
                print(f'❌ WebSocket error: {e}')
                state.upstox_connected = False
                
                if retry_count < max_retries:
                    wait_time = min(5 * retry_count, 30)
                    print(f'🔄 Retrying in {wait_time}s... ({retry_count}/{max_retries})')
                    await asyncio.sleep(wait_time)
                else:
                    print('❌ Max retries reached. Stopping service.')
                    self.running = False
    
    def start(self):
        """Start the market data service"""
        self.running = True
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.connect_and_stream())
    
    def stop(self):
        """Stop the market data service"""
        self.running = False
        if self.loop:
            self.loop.stop()

# Initialize service
market_service = UpstoxMarketDataService()

def start_market_service():
    """Start market data service in background thread"""
    if not hasattr(start_market_service, 'started'):
        thread = threading.Thread(target=market_service.start, daemon=True)
        thread.start()
        start_market_service.started = True
        print('🚀 Live market data service started')

# SSE generators
def generate_sse():
    """Generate SSE stream for main dashboard"""
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

def generate_signal_sse():
    """Generate SSE stream for signals dashboard"""
    client_queue = deque(maxlen=100)
    state.signal_clients.append(client_queue)
    
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
        state.signal_clients.remove(client_queue)

# Routes
@app.route('/')
def index():
    """Big Move Detection Dashboard"""
    try:
        return render_template('bigmove_dashboard.html')
    except Exception as e:
        return f"""
        <html>
        <body style="background: #111; color: white; padding: 40px; font-family: Arial;">
            <h1>⚠️ Template Not Found</h1>
            <p>bigmove_dashboard.html is missing from templates/ directory.</p>
            <p>Error: {str(e)}</p>
        </body>
        </html>
        """

@app.route('/signals')
def signals_page():
    """Real-Time Signals Dashboard"""
    try:
        return render_template('signals.html')
    except Exception as e:
        return f"""
        <html>
        <body style="background: #111; color: white; padding: 40px; font-family: Arial;">
            <h1>⚠️ Template Not Found</h1>
            <p>signals.html is missing from templates/ directory.</p>
            <p><a href="/" style="color: #4ade80;">← Back to Big Move Dashboard</a></p>
            <p>Error: {str(e)}</p>
        </body>
        </html>
        """

@app.route('/stream')
def stream():
    """SSE stream for main dashboard"""
    return Response(generate_sse(), mimetype='text/event-stream',
                   headers={'Cache-Control': 'no-cache', 'Connection': 'keep-alive'})

@app.route('/stream/signals')
def stream_signals():
    """SSE stream for signals dashboard"""
    return Response(generate_signal_sse(), mimetype='text/event-stream',
                   headers={'Cache-Control': 'no-cache', 'Connection': 'keep-alive'})

@app.route('/api/analysis')
def get_analysis():
    """Get Big Move analysis results"""
    with state.lock:
        return jsonify({
            'results': list(state.analysis_results.values()),
            'timestamp': datetime.now().isoformat()
        })

@app.route('/api/signals')
def get_signals():
    """Get trading signals analysis"""
    with state.lock:
        return jsonify({
            'analyses': dict(state.signal_results),
            'timestamp': datetime.now().isoformat()
        })

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'mode': 'live',
        'upstox': 'connected' if state.upstox_connected else 'disconnected',
        'instruments_analyzed': len(state.analysis_results),
        'active_signals': len([s for s in state.signal_results.values() 
                              if s.get('signals', {}).get('buy') or s.get('signals', {}).get('sell')]),
        'messages_processed': state.message_count,
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    print('\n' + '='*70)
    print('🚀 Live Market Analysis Dashboard')
    print('='*70)
    print(f'📡 Mode:              LIVE DATA (Upstox)')
    print(f'📊 Big Move Dashboard: http://localhost:5000/')
    print(f'⚡ Signals Dashboard:  http://localhost:5000/signals')
    print(f'💓 Health:            http://localhost:5000/health')
    print(f'📡 Analysis API:      http://localhost:5000/api/analysis')
    print(f'🎯 Signals API:       http://localhost:5000/api/signals')
    print('='*70)
    
    if ACCESS_TOKEN == 'YOUR_ACCESS_TOKEN':
        print('\n❌ ERROR: ACCESS_TOKEN not configured!')
        print('Set your Upstox token:')
        print('  export ACCESS_TOKEN="your_token_here"')
        print('  OR')
        print('  Create .env file with: ACCESS_TOKEN=your_token')
        print('='*70 + '\n')
        exit(1)
    
    print('\n✅ ACCESS_TOKEN configured')
    print('🔗 Connecting to Upstox...')
    print('='*70 + '\n')
    
    start_market_service()
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)