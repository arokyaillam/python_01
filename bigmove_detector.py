"""
Big Move Detection Algorithm (v2)
Analyzes Upstox Market Feed data with Greeks, OB, Volume metrics
"""
from typing import Dict, List, Any, Optional
from collections import deque

class BigMoveDetector:
    def __init__(self):
        self.history: Dict[str, deque] = {}
        self.avg_volumes: Dict[str, float] = {}
        
    def to_num(self, value: Any) -> float:
        """Convert safely to number"""
        if value is None or value == '':
            return 0.0
        try:
            n = float(value)
            return n if not (n != n) else 0.0  # Check for NaN
        except (ValueError, TypeError):
            return 0.0
    
    def extract_market_data(self, feed_value: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract relevant feed section"""
        if 'fullFeed' in feed_value and 'marketFF' in feed_value['fullFeed']:
            return feed_value['fullFeed']['marketFF']
        elif 'ltpc' in feed_value:
            return {'ltpc': feed_value['ltpc']}
        elif 'firstLevelWithGreeks' in feed_value:
            return feed_value['firstLevelWithGreeks']
        return None
    
    def calculate_greeks_score(self, gamma: float, delta: float, iv: float) -> float:
        """Calculate Greeks score for options analysis"""
        score = 0.0
        
        # Gamma analysis (0-5 points)
        if gamma > 0.001:
            score += 5
        elif gamma > 0.0005:
            score += 3
        elif gamma > 0.0001:
            score += 1
        
        # Delta analysis (0-5 points)
        abs_delta = abs(delta)
        if abs_delta > 0.7:
            score += 5
        elif abs_delta > 0.5:
            score += 3
        elif abs_delta > 0.3:
            score += 1
        
        # IV analysis (0-5 points)
        if iv > 0.3:
            score += 5
        elif iv > 0.2:
            score += 3
        elif iv > 0.1:
            score += 1
        
        return min(score, 15)
    
    def generate_signals(
        self,
        vol_ratio: float,
        price_range: float,
        ob_ratio: float,
        gamma: float,
        delta: Optional[float] = None,
        iv: Optional[float] = None
    ) -> List[Dict[str, str]]:
        """Generate alert signals"""
        signals = []
        
        # Volume spike
        if vol_ratio > 3:
            signals.append({
                'type': 'CRITICAL',
                'title': 'Volume Spike',
                'message': f'{vol_ratio:.2f}× avg volume'
            })
        
        # Price movement
        if price_range > 2:
            signals.append({
                'type': 'CRITICAL',
                'title': 'Explosive Candle',
                'message': f'{price_range:.2f}% move'
            })
        
        # Order book pressure
        if ob_ratio > 3:
            signals.append({
                'type': 'WARNING',
                'title': 'Buy Pressure',
                'message': f'{ob_ratio:.2f}:1 bid/ask'
            })
        elif ob_ratio < 0.33 and ob_ratio > 0:
            signals.append({
                'type': 'WARNING',
                'title': 'Sell Pressure',
                'message': f'{(1/ob_ratio):.1f}:1 ask/bid'
            })
        
        # Gamma alert
        if gamma > 0.0005:
            signals.append({
                'type': 'INFO',
                'title': 'High Gamma Detected',
                'message': f'Gamma = {gamma:.4f}'
            })
        
        # Delta exposure
        if delta and abs(delta) > 0.7:
            signals.append({
                'type': 'WARNING',
                'title': 'High Delta Exposure',
                'message': f'Delta = {delta:.3f} (High directional risk)'
            })
        
        # IV alert
        if iv and iv > 0.25:
            signals.append({
                'type': 'INFO',
                'title': 'High Implied Volatility',
                'message': f'IV = {(iv * 100):.1f}%'
            })
        
        # Combined analysis
        if gamma > 0.001 and delta and abs(delta) > 0.6:
            signals.append({
                'type': 'CRITICAL',
                'title': 'Gamma-Delta Squeeze',
                'message': f'High Gamma ({gamma:.4f}) + High Delta ({delta:.3f})'
            })
        
        return signals
    
    def analyze(self, symbol: str, feed_value: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Core analysis - comprehensive market data evaluation"""
        # Extract market full feed
        mff = None
        if 'fullFeed' in feed_value and 'marketFF' in feed_value['fullFeed']:
            mff = feed_value['fullFeed']['marketFF']
        
        if not mff:
            return None
        
        # Extract values
        ltpc = mff.get('ltpc', {})
        ltp = self.to_num(ltpc.get('ltp', 0))
        volume = self.to_num(mff.get('vtt', 0))
        tbq = self.to_num(mff.get('tbq', 0))
        tsq = self.to_num(mff.get('tsq', 0))
        
        greeks = mff.get('optionGreeks', {})
        gamma = self.to_num(greeks.get('gamma', 0))
        delta = self.to_num(greeks.get('delta', 0))
        iv = self.to_num(mff.get('iv', 0))
        
        # Volume ratio
        avg_vol = self.avg_volumes.get(symbol, volume / 10 if volume > 0 else 1)
        volume_ratio = volume / avg_vol if avg_vol > 0 else 0
        
        # Update running average
        self.avg_volumes[symbol] = avg_vol * 0.9 + volume * 0.1
        
        # Order book ratio
        ob_ratio = tbq / tsq if tsq > 0 else 0
        
        # Price range from OHLC
        price_range = 0.0
        market_ohlc = mff.get('marketOHLC', {})
        candles = market_ohlc.get('ohlc', []) if isinstance(market_ohlc, dict) else []
        
        if candles:
            # Find 1-minute candle or use first available
            candle = None
            for c in candles:
                if c.get('interval') == 'I1':
                    candle = c
                    break
            if not candle:
                candle = candles[0]
            
            if candle:
                high = self.to_num(candle.get('high', 0))
                low = self.to_num(candle.get('low', 0))
                if low > 0:
                    price_range = ((high - low) / low) * 100
        
        # Greeks score
        greeks_score = self.calculate_greeks_score(gamma, delta, iv)
        
        # Calculate total score
        score = 0.0
        
        # Volume factor (0-35)
        if volume_ratio > 5:
            score += 35
        elif volume_ratio > 3:
            score += 25
        elif volume_ratio > 2:
            score += 15
        elif volume_ratio > 1.5:
            score += 8
        
        # Price range factor (0-30)
        if price_range > 3:
            score += 30
        elif price_range > 2:
            score += 20
        elif price_range > 1:
            score += 12
        elif price_range > 0.5:
            score += 5
        
        # Order book factor (0-20)
        if ob_ratio > 5:
            score += 20
        elif ob_ratio > 3:
            score += 15
        elif ob_ratio > 2:
            score += 10
        elif ob_ratio > 1.5:
            score += 5
        
        # Greeks factor (0-15)
        score += greeks_score
        
        score = min(score, 100)
        
        # Alert classification
        alert_level = "NORMAL"
        if score >= 75:
            alert_level = "CRITICAL"
        elif score >= 55:
            alert_level = "WARNING"
        elif score >= 35:
            alert_level = "WATCH"
        
        # Generate signals
        signals = self.generate_signals(volume_ratio, price_range, ob_ratio, gamma, delta, iv)
        
        return {
            'symbol': symbol,
            'score': round(score, 2),
            'alertLevel': alert_level,
            'metrics': {
                'ltp': round(ltp, 2),
                'volume': int(volume),
                'volumeRatio': round(volume_ratio, 2),
                'tbq': int(tbq),
                'tsq': int(tsq),
                'obRatio': round(ob_ratio, 2),
                'priceRange': round(price_range, 2),
                'gamma': round(gamma, 4),
                'delta': round(delta, 3),
                'iv': round(iv, 3)
            },
            'signals': signals,
            'greeksScore': round(greeks_score, 1)
        }