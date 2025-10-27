import asyncio
import json
import ssl
import websockets
import requests
from google.protobuf.json_format import MessageToDict
import MarketDataFeedV3_pb2 as pb
from datetime import datetime
import sqlite3
from pathlib import Path
import logging
from typing import Dict, Any, List
import pandas as pd
import os
from configparser import ConfigParser

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('market_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MarketDataCollector:
    """Collect and store Upstox market data for ML datasets."""
    
    def __init__(self, db_path: str = "market_data.db", access_token: str = None, config_file: str = "config.ini"):
        self.db_path = db_path
        self.config_file = config_file
        self.access_token = access_token or self.load_token_from_config()
        self.setup_database()
    
    def load_token_from_config(self):
        """Load access token from config file."""
        config = ConfigParser()
        
        if os.path.exists(self.config_file):
            config.read(self.config_file)
            if 'upstox' in config and 'access_token' in config['upstox']:
                token = config['upstox']['access_token']
                logger.info("Access token loaded from config file")
                return token
        
        logger.warning("No access token found in config file")
        return None
    
    def save_token_to_config(self, access_token: str):
        """Save access token to config file."""
        config = ConfigParser()
        
        if os.path.exists(self.config_file):
            config.read(self.config_file)
        
        if 'upstox' not in config:
            config['upstox'] = {}
        
        config['upstox']['access_token'] = access_token
        config['upstox']['updated_at'] = datetime.now().isoformat()
        
        with open(self.config_file, 'w') as f:
            config.write(f)
        
        self.access_token = access_token
        logger.info(f"Access token saved to {self.config_file}")
    
    def update_token(self, new_token: str):
        """Update the access token."""
        self.save_token_to_config(new_token)
        logger.info("Access token updated successfully")
        
    def setup_database(self):
        """Initialize SQLite database with optimized schema."""
        # Register datetime adapter for SQLite (Python 3.12+ compatibility)
        import sqlite3
        sqlite3.register_adapter(datetime, lambda dt: dt.isoformat())
        sqlite3.register_converter("DATETIME", lambda s: datetime.fromisoformat(s.decode()))
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Main ticks table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS market_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                feed_timestamp TEXT,
                instrument_key TEXT NOT NULL,
                ltp REAL,
                ltt TEXT,
                ltq INTEGER,
                cp REAL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                oi INTEGER,
                iv REAL,
                atp REAL,
                vtt INTEGER,
                tbq INTEGER,
                tsq INTEGER,
                request_mode TEXT
            )
        ''')
        
        # Market depth table (bid-ask quotes)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS market_depth (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tick_id INTEGER,
                instrument_key TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                depth_level INTEGER,
                bid_price REAL,
                bid_qty INTEGER,
                ask_price REAL,
                ask_qty INTEGER,
                FOREIGN KEY (tick_id) REFERENCES market_ticks(id)
            )
        ''')
        
        # Option Greeks table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS option_greeks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tick_id INTEGER,
                instrument_key TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                delta REAL,
                theta REAL,
                gamma REAL,
                vega REAL,
                rho REAL,
                FOREIGN KEY (tick_id) REFERENCES market_ticks(id)
            )
        ''')
        
        # OHLC intervals table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ohlc_intervals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tick_id INTEGER,
                instrument_key TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                interval TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                interval_ts TEXT,
                FOREIGN KEY (tick_id) REFERENCES market_ticks(id)
            )
        ''')
        
        # Raw data table for complete feed storage
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS raw_feeds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tick_id INTEGER,
                instrument_key TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                feed_type TEXT,
                raw_json TEXT,
                FOREIGN KEY (tick_id) REFERENCES market_ticks(id)
            )
        ''')
        
        # Create indexes for faster queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_instrument_timestamp ON market_ticks(instrument_key, received_timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_depth_instrument ON market_depth(instrument_key, timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_greeks_instrument ON option_greeks(instrument_key, timestamp)')
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_path}")
    
    def get_market_data_feed_authorize(self):
        """Get authorization for market data feed."""
        if not self.access_token:
            raise ValueError("Access token not set. Use update_token() or provide token in config.ini")
        
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
        
        try:
            api_response = requests.get(url=url, headers=headers, timeout=10)
            api_response.raise_for_status()
            return api_response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.error("Invalid or expired access token. Please update token.")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Authorization failed: {e}")
            raise
    
    def decode_protobuf(self, buffer):
        """Decode protobuf message."""
        feed_response = pb.FeedResponse()
        feed_response.ParseFromString(buffer)
        return feed_response
    
    def parse_and_store_tick(self, data_dict: Dict[str, Any]):
        """Parse tick data matching the exact Upstox structure and store in database."""
        if 'feeds' not in data_dict:
            return
        
        feeds = data_dict['feeds']
        current_ts = feeds.get('currentTs', '')
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            for instrument_key, feed_data in feeds.items():
                if instrument_key == 'currentTs':
                    continue
                
                # Check if it's a full feed
                if 'fullFeed' not in feed_data:
                    continue
                
                full_feed = feed_data['fullFeed']
                market_ff = full_feed.get('marketFF', {})
                
                # Extract LTPC (Last Traded Price & related)
                ltpc = market_ff.get('ltpc', {})
                
                # Extract market OHLC
                market_ohlc = market_ff.get('marketOHLC', {}).get('ohlc', [])
                daily_ohlc = next((x for x in market_ohlc if x.get('interval') == '1d'), {})
                
                # Insert main tick data
                cursor.execute('''
                    INSERT INTO market_ticks (
                        received_timestamp, feed_timestamp, instrument_key,
                        ltp, ltt, ltq, cp, open, high, low, close, volume,
                        oi, iv, atp, vtt, tbq, tsq, request_mode
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    datetime.now(),
                    current_ts,
                    instrument_key,
                    ltpc.get('ltp'),
                    ltpc.get('ltt'),
                    ltpc.get('ltq'),
                    ltpc.get('cp'),
                    daily_ohlc.get('open'),
                    daily_ohlc.get('high'),
                    daily_ohlc.get('low'),
                    daily_ohlc.get('close'),
                    daily_ohlc.get('vol'),
                    market_ff.get('oi'),
                    market_ff.get('iv'),
                    market_ff.get('atp'),
                    market_ff.get('vtt'),
                    market_ff.get('tbq'),
                    market_ff.get('tsq'),
                    full_feed.get('requestMode')
                ))
                
                tick_id = cursor.lastrowid
                
                # Store market depth (bid-ask quotes)
                market_level = market_ff.get('marketLevel', {})
                bid_ask_quotes = market_level.get('bidAskQuote', [])
                
                for level, quote in enumerate(bid_ask_quotes):
                    cursor.execute('''
                        INSERT INTO market_depth (
                            tick_id, instrument_key, depth_level,
                            bid_price, bid_qty, ask_price, ask_qty
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        tick_id,
                        instrument_key,
                        level,
                        quote.get('bidP'),
                        quote.get('bidQ'),
                        quote.get('askP'),
                        quote.get('askQ')
                    ))
                
                # Store option Greeks if available
                option_greeks = market_ff.get('optionGreeks', {})
                if option_greeks:
                    cursor.execute('''
                        INSERT INTO option_greeks (
                            tick_id, instrument_key, delta, theta, gamma, vega, rho
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        tick_id,
                        instrument_key,
                        option_greeks.get('delta'),
                        option_greeks.get('theta'),
                        option_greeks.get('gamma'),
                        option_greeks.get('vega'),
                        option_greeks.get('rho')
                    ))
                
                # Store all OHLC intervals
                for ohlc in market_ohlc:
                    cursor.execute('''
                        INSERT INTO ohlc_intervals (
                            tick_id, instrument_key, interval,
                            open, high, low, close, volume, interval_ts
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        tick_id,
                        instrument_key,
                        ohlc.get('interval'),
                        ohlc.get('open'),
                        ohlc.get('high'),
                        ohlc.get('low'),
                        ohlc.get('close'),
                        ohlc.get('vol'),
                        ohlc.get('ts')
                    ))
                
                # Store raw JSON for complete data preservation
                cursor.execute('''
                    INSERT INTO raw_feeds (
                        tick_id, instrument_key, feed_type, raw_json
                    ) VALUES (?, ?, ?, ?)
                ''', (
                    tick_id,
                    instrument_key,
                    'live_feed',
                    json.dumps(feed_data)
                ))
            
            conn.commit()
            
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    async def collect_market_data(self, instrument_keys: List[str], mode: str = "full"):
        """Main method to collect market data via WebSocket."""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        response = self.get_market_data_feed_authorize()
        ws_url = response["data"]["authorized_redirect_uri"]
        
        logger.info(f"Connecting to WebSocket for {len(instrument_keys)} instruments")
        
        async with websockets.connect(ws_url, ssl=ssl_context) as websocket:
            logger.info('WebSocket connection established')
            await asyncio.sleep(1)
            
            # Subscribe to instruments
            subscription_data = {
                "guid": "market_data_collector",
                "method": "sub",
                "data": {
                    "mode": mode,
                    "instrumentKeys": instrument_keys
                }
            }
            
            binary_data = json.dumps(subscription_data).encode('utf-8')
            await websocket.send(binary_data)
            logger.info(f"Subscribed successfully in '{mode}' mode")
            
            tick_count = 0
            
            try:
                while True:
                    message = await websocket.recv()
                    decoded_data = self.decode_protobuf(message)
                    data_dict = MessageToDict(decoded_data)
                    
                    # Parse and store the tick
                    self.parse_and_store_tick(data_dict)
                    tick_count += 1
                    
                    if tick_count % 50 == 0:
                        logger.info(f"Processed {tick_count} ticks")
                    
            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket connection closed")
            except KeyboardInterrupt:
                logger.info(f"Collection stopped. Total ticks: {tick_count}")
            except Exception as e:
                logger.error(f"Error during data collection: {e}", exc_info=True)
    
    def export_to_csv(self, output_dir: str = "exported_data", 
                      instrument_key: str = None,
                      start_date: str = None, 
                      end_date: str = None):
        """Export collected data to CSV files for ML processing."""
        Path(output_dir).mkdir(exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        
        # Build query conditions
        where_conditions = []
        params = []
        
        if instrument_key:
            where_conditions.append("instrument_key = ?")
            params.append(instrument_key)
        if start_date:
            where_conditions.append("received_timestamp >= ?")
            params.append(start_date)
        if end_date:
            where_conditions.append("received_timestamp <= ?")
            params.append(end_date)
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        # Export main ticks
        query = f"SELECT * FROM market_ticks WHERE {where_clause} ORDER BY received_timestamp"
        df_ticks = pd.read_sql_query(query, conn, params=params)
        ticks_path = f"{output_dir}/market_ticks.csv"
        df_ticks.to_csv(ticks_path, index=False)
        logger.info(f"Exported {len(df_ticks)} ticks to {ticks_path}")
        
        # Export market depth
        if not df_ticks.empty:
            tick_ids = df_ticks['id'].tolist()
            placeholders = ','.join('?' * len(tick_ids))
            depth_query = f"SELECT * FROM market_depth WHERE tick_id IN ({placeholders})"
            df_depth = pd.read_sql_query(depth_query, conn, params=tick_ids)
            depth_path = f"{output_dir}/market_depth.csv"
            df_depth.to_csv(depth_path, index=False)
            logger.info(f"Exported {len(df_depth)} depth records to {depth_path}")
            
            # Export option Greeks
            greeks_query = f"SELECT * FROM option_greeks WHERE tick_id IN ({placeholders})"
            df_greeks = pd.read_sql_query(greeks_query, conn, params=tick_ids)
            if not df_greeks.empty:
                greeks_path = f"{output_dir}/option_greeks.csv"
                df_greeks.to_csv(greeks_path, index=False)
                logger.info(f"Exported {len(df_greeks)} Greeks records to {greeks_path}")
        
        conn.close()
        return df_ticks
    
    def get_statistics(self):
        """Get collection statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                instrument_key,
                COUNT(*) as tick_count,
                MIN(received_timestamp) as first_tick,
                MAX(received_timestamp) as last_tick,
                AVG(ltp) as avg_ltp,
                MAX(high) as max_high,
                MIN(low) as min_low
            FROM market_ticks
            GROUP BY instrument_key
        ''')
        
        stats = cursor.fetchall()
        conn.close()
        
        print("\n=== Collection Statistics ===")
        for stat in stats:
            print(f"\nInstrument: {stat[0]}")
            print(f"  Ticks: {stat[1]}")
            print(f"  First: {stat[2]}")
            print(f"  Last: {stat[3]}")
            print(f"  Avg LTP: {stat[4]:.2f}" if stat[4] else "  Avg LTP: N/A")
            print(f"  High: {stat[5]}" if stat[5] else "  High: N/A")
            print(f"  Low: {stat[6]}" if stat[6] else "  Low: N/A")
        
        return stats
    
    def create_ml_dataset(self, instrument_key: str, output_path: str = "ml_dataset.csv"):
        """Create a flattened ML-ready dataset with features."""
        conn = sqlite3.connect(self.db_path)
        
        query = '''
        SELECT 
            mt.received_timestamp,
            mt.instrument_key,
            mt.ltp,
            mt.open,
            mt.high,
            mt.low,
            mt.close,
            mt.volume,
            mt.oi,
            mt.iv,
            mt.atp,
            mt.tbq,
            mt.tsq,
            og.delta,
            og.theta,
            og.gamma,
            og.vega,
            og.rho,
            md.bid_price as best_bid,
            md.bid_qty as best_bid_qty,
            md.ask_price as best_ask,
            md.ask_qty as best_ask_qty
        FROM market_ticks mt
        LEFT JOIN option_greeks og ON mt.id = og.tick_id
        LEFT JOIN market_depth md ON mt.id = md.tick_id AND md.depth_level = 0
        WHERE mt.instrument_key = ?
        ORDER BY mt.received_timestamp
        '''
        
        df = pd.read_sql_query(query, conn, params=[instrument_key])
        conn.close()
        
        # Add derived features
        if not df.empty:
            df['spread'] = df['best_ask'] - df['best_bid']
            df['mid_price'] = (df['best_ask'] + df['best_bid']) / 2
            df['imbalance'] = (df['best_bid_qty'] - df['best_ask_qty']) / (df['best_bid_qty'] + df['best_ask_qty'])
            
        df.to_csv(output_path, index=False)
        logger.info(f"Created ML dataset with {len(df)} records: {output_path}")
        return df


# Example usage
async def main():
    # Method 1: Initialize with token directly
    # collector = MarketDataCollector(access_token="YOUR_ACCESS_TOKEN_HERE")
    
    # Method 2: Load from config.ini file (recommended)
    collector = MarketDataCollector()
    
    # Method 3: Update token programmatically
    # collector.update_token("YOUR_NEW_ACCESS_TOKEN")
    
    # Define instruments to track
    instruments = [
        "NSE_FO|61755",  # Example from your data
        "NSE_INDEX|Nifty Bank",
        "NSE_INDEX|Nifty 50"
    ]
    
    # Start collecting data
    await collector.collect_market_data(instruments, mode="full")


def setup_config():
    """Helper function to create config file with token."""
    token = input("Enter your Upstox access token: ")
    
    collector = MarketDataCollector()
    collector.update_token(token)
    
    print(f"\n✓ Token saved to config.ini")
    print("You can now run the collector without specifying the token.")


if __name__ == "__main__":
    import sys
    
    # Check if user wants to setup config
    if len(sys.argv) > 1 and sys.argv[1] == "setup":
        setup_config()
        sys.exit(0)
    
    # Check if token exists before running
    collector = MarketDataCollector()
    if not collector.access_token:
        print("\n❌ No access token found!")
        print("Please run: python3 upstox_collector.py setup")
        sys.exit(1)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nData collection stopped by user")
        
        # Show statistics and export data
        collector.get_statistics()
        
        # Export for ML
        print("\nExporting data for ML...")
        collector.export_to_csv()
        print("Export complete!")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        if "401" in str(e) or "Unauthorized" in str(e):
            print("\nYour access token is invalid or expired.")
            print("Please run: python3 upstox_collector.py setup")
        sys.exit(1)