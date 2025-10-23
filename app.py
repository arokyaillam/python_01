"""
Flask Market Data Streaming Server with SSE
Run: uv run app.py
"""
import asyncio
import json
import ssl
import threading
import time
from datetime import datetime
from collections import deque
from flask import Flask, Response, render_template, jsonify, request
from flask_cors import CORS
import websockets
import requests
from google.protobuf.json_format import MessageToDict
import MarketDataFeedV3_pb2 as pb

app = Flask(__name__)
CORS(app)

# Configuration
import os
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN', 'eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI3QkJSUkUiLCJqdGkiOiI2OGY5OTNiMDhiMTc1OTQ0MjIwYzU2N2IiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzYxMTg2NzM2LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NjEyNTY4MDB9.aMzR2EIN8MzCxeYAAjdsQ4PgmatqgmOqQ7vkPA4K2_A')  # Load from environment or use fallback
DEFAULT_INSTRUMENTS = ["NSE_INDEX|Nifty Bank", "NSE_INDEX|Nifty 50"]

# Global state
class GlobalState:
    def __init__(self):
        self.clients = []
        self.upstox_connected = False
        self.market_data_buffer = deque(maxlen=100)
        self.lock = threading.Lock()
        self.message_count = 0

state = GlobalState()

class MarketDataService:
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
    
    async def connect_and_stream(self):
        """Connect to Upstox WebSocket and stream data"""
        try:
            ws_url = self.get_market_feed_url()
            print('🔗 Connecting to Upstox...')

            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            async with websockets.connect(ws_url, ssl=ssl_context, ping_interval=30) as websocket:
                self.websocket = websocket
                state.upstox_connected = True
                print('✅ Connected to Upstox')

                await asyncio.sleep(1)

                # Subscribe to instruments
                subscription = {
                    "guid": "flask_server_1",
                    "method": "sub",
                    "data": {
                        "mode": "full",
                        "instrumentKeys": DEFAULT_INSTRUMENTS
                    }
                }

                await websocket.send(json.dumps(subscription).encode('utf-8'))
                print(f'📡 Subscribed to: {DEFAULT_INSTRUMENTS}')

                # Receive and broadcast data
                while self.running:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=60)
                        decoded_data = self.decode_protobuf(message)

                        if decoded_data:
                            # Add timestamp and metadata
                            decoded_data['server_timestamp'] = datetime.now().isoformat()
                            decoded_data['message_id'] = state.message_count

                            with state.lock:
                                state.message_count += 1
                                state.market_data_buffer.append(decoded_data)

                            # Broadcast to all SSE clients
                            self.broadcast_to_clients(decoded_data)

                    except asyncio.TimeoutError:
                        print('⚠️ WebSocket timeout, sending ping...')
                        await websocket.ping()

                    except websockets.exceptions.ConnectionClosed:
                        print('❌ WebSocket connection closed')
                        break

        except Exception as e:
            print(f'❌ WebSocket error: {e}')
            state.upstox_connected = False
            self.running = False
    
    def broadcast_to_clients(self, data):
        """Broadcast data to all SSE clients"""
        dead_clients = []
        
        for client_queue in state.clients:
            try:
                client_queue.append(data)
            except Exception as e:
                print(f'❌ Failed to queue data for client: {e}')
                dead_clients.append(client_queue)
        
        # Clean up dead clients
        for client in dead_clients:
            try:
                state.clients.remove(client)
            except ValueError:
                pass
    
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
market_service = MarketDataService()

def start_market_service():
    """Start market data service in background thread"""
    if not hasattr(start_market_service, 'started'):
        thread = threading.Thread(target=market_service.start, daemon=True)
        thread.start()
        start_market_service.started = True
        print('🚀 Market data service started in background')
    else:
        print('⚠️  Market service already running')

# SSE event generator
def generate_sse():
    """Generate SSE events for clients"""
    client_queue = deque(maxlen=100)
    state.clients.append(client_queue)
    client_id = len(state.clients)
    
    print(f'✅ SSE client #{client_id} connected (total: {len(state.clients)})')
    
    try:
        # Send initial connection message
        connection_msg = {
            'type': 'connection',
            'message': 'connected',
            'client_id': client_id,
            'timestamp': datetime.now().isoformat(),
            'upstox_status': 'connected' if state.upstox_connected else 'disconnected'
        }
        yield f"data: {json.dumps(connection_msg)}\n\n"
        
        # Send recent market data from buffer
        with state.lock:
            for data in list(state.market_data_buffer):
                yield f"data: {json.dumps(data)}\n\n"
        
        last_heartbeat = time.time()
        
        while True:
            # Send heartbeat every 30 seconds
            current_time = time.time()
            if current_time - last_heartbeat > 30:
                yield f": heartbeat {int(current_time)}\n\n"
                last_heartbeat = current_time
            
            # Send queued market data
            if client_queue:
                while client_queue:
                    market_data = client_queue.popleft()
                    yield f"data: {json.dumps(market_data)}\n\n"
            else:
                time.sleep(0.1)  # Small delay to prevent busy waiting
                
    except GeneratorExit:
        print(f'❌ SSE client #{client_id} disconnected')
    finally:
        try:
            state.clients.remove(client_queue)
        except ValueError:
            pass
        print(f'🧹 Cleaned up client #{client_id} ({len(state.clients)} remaining)')

@app.route('/')
def index():
    """Serve the main HTML page"""
    return render_template('index.html')

@app.route('/stream')
def stream():
    """SSE endpoint for market data"""
    return Response(
        generate_sse(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',
            'Access-Control-Allow-Origin': '*'
        }
    )

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'upstox': 'connected' if state.upstox_connected else 'disconnected',
        'clients': len(state.clients),
        'messages_processed': state.message_count,
        'buffer_size': len(state.market_data_buffer),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/stats')
def stats():
    """Get server statistics"""
    return jsonify({
        'connected_clients': len(state.clients),
        'total_messages': state.message_count,
        'buffer_size': len(state.market_data_buffer),
        'upstox_connected': state.upstox_connected,
        'instruments': DEFAULT_INSTRUMENTS,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/subscribe', methods=['POST'])
def subscribe():
    """Subscribe to additional instruments"""
    try:
        data = request.get_json()
        instruments = data.get('instruments', [])
        mode = data.get('mode', 'full')
        
        if not instruments:
            return jsonify({'error': 'No instruments provided'}), 400
        
        # Send subscription to Upstox WebSocket
        if market_service.websocket and state.upstox_connected:
            subscription = {
                "guid": f"custom_{int(time.time())}",
                "method": "sub",
                "data": {
                    "mode": mode,
                    "instrumentKeys": instruments
                }
            }
            
            async def send_subscription():
                await market_service.websocket.send(json.dumps(subscription).encode('utf-8'))
            
            asyncio.run_coroutine_threadsafe(send_subscription(), market_service.loop)
            
            return jsonify({
                'status': 'success',
                'message': f'Subscribed to {len(instruments)} instruments',
                'instruments': instruments
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Not connected to Upstox'
            }), 503
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/recent')
def recent_data():
    """Get recent market data from buffer"""
    with state.lock:
        data = list(state.market_data_buffer)
    
    return jsonify({
        'count': len(data),
        'data': data[-10:],  # Last 10 messages
        'timestamp': datetime.now().isoformat()
    })

@app.errorhandler(404)
def not_found(e):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def internal_error(e):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    print('\n' + '='*60)
    print('🚀 Flask Market Data Streaming Server')
    print('='*60)
    print(f'📊 Dashboard:    http://localhost:5000/')
    print(f'📡 SSE Stream:   http://localhost:5000/stream')
    print(f'💓 Health:       http://localhost:5000/health')
    print(f'📈 Stats:        http://localhost:5000/api/stats')
    print(f'📚 Recent Data:  http://localhost:5000/api/recent')
    print('='*60 + '\n')
    
    # Start market data service
    start_market_service()
    
    # Run Flask app
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=False,
        threaded=True
    )