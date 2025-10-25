"""
Real-Time Signal Analyzer
Calculates Order Flow, Momentum, Volume Spikes, and Entry/Exit Signals
"""
from typing import Dict, List, Optional, Any
from collections import deque
import time

class SignalAnalyzer:
    def __init__(self, lookback_ticks=5):
        self.lookback_ticks = lookback_ticks
        self.price_history: Dict[str, deque] = {}
        self.volume_history: Dict[str, deque] = {}
        self.delta_history: Dict[str, deque] = {}
        self.oi_history: Dict[str, deque] = {}
        self.tick_count: Dict[str, int] = {}
        self.start_time: Dict[str, float] = {}
        self.positions: Dict[str, Dict] = {}  # Track open positions
        
    def to_num(self, value: Any) -> float:
        """Convert safely to number"""
        if value is None or value == '':
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def calculate_ofi(self, tbq: float, tsq: float) -> Dict[str, Any]:
        """
        Calculate Order Flow Imbalance
        OFI = (Total_Bid_Qty - Total_Ask_Qty) / (Total_Bid_Qty + Total_Ask_Qty)
        """
        if tbq + tsq == 0:
            return {'value': 0, 'signal': 'NEUTRAL', 'strength': 0}
        
        ofi = (tbq - tsq) / (tbq + tsq)
        
        # Determine signal
        if ofi > 0.5:
            signal = 'STRONG_BUY'
            strength = min((ofi - 0.5) * 2, 1.0)  # 0.5-1.0 → 0-1
        elif ofi > 0.3:
            signal = 'MODERATE_BUY'
            strength = (ofi - 0.3) / 0.2  # 0.3-0.5 → 0-1
        elif ofi < 0:
            signal = 'SELL_PRESSURE'
            strength = min(abs(ofi), 1.0)
        elif ofi < 0.3:
            signal = 'NEUTRAL'
            strength = 0.3 - ofi  # Lower = more neutral
        else:
            signal = 'NEUTRAL'
            strength = 0
        
        return {
            'value': round(ofi, 4),
            'signal': signal,
            'strength': round(strength, 2),
            'tbq': int(tbq),
            'tsq': int(tsq)
        }
    
    def calculate_spread(self, best_bid: float, best_ask: float, ltp: float) -> Dict[str, Any]:
        """
        Calculate Spread Quality
        Spread % = ((Best_Ask - Best_Bid) / LTP) * 100
        """
        if ltp == 0:
            return {'value': 0, 'percentage': 0, 'signal': 'UNKNOWN', 'quality': 0}
        
        spread = best_ask - best_bid
        spread_pct = (spread / ltp) * 100
        
        # Determine quality
        if spread_pct < 0.2:
            signal = 'EXCELLENT'
            quality = 100
        elif spread_pct < 0.5:
            signal = 'GOOD'
            quality = 70
        else:
            signal = 'CAUTION'
            quality = 30
        
        return {
            'value': round(spread, 2),
            'percentage': round(spread_pct, 3),
            'signal': signal,
            'quality': quality,
            'best_bid': round(best_bid, 2),
            'best_ask': round(best_ask, 2)
        }
    
    def calculate_momentum(self, symbol: str, current_ltp: float) -> Dict[str, Any]:
        """
        Calculate Price Momentum based on last N ticks
        momentum = (current_ltp - avg_of_last_N) / avg_of_last_N
        """
        if symbol not in self.price_history:
            self.price_history[symbol] = deque(maxlen=self.lookback_ticks)
        
        self.price_history[symbol].append(current_ltp)
        
        if len(self.price_history[symbol]) < 2:
            return {'value': 0, 'signal': 'NEUTRAL', 'avg': current_ltp}
        
        avg_price = sum(self.price_history[symbol]) / len(self.price_history[symbol])
        momentum = (current_ltp - avg_price) / avg_price if avg_price > 0 else 0
        
        # Determine signal
        if momentum > 0.001:
            signal = 'BULLISH'
        elif momentum < -0.001:
            signal = 'BEARISH'
        else:
            signal = 'NEUTRAL'
        
        return {
            'value': round(momentum, 5),
            'signal': signal,
            'current': round(current_ltp, 2),
            'avg': round(avg_price, 2),
            'ticks': len(self.price_history[symbol])
        }
    
    def calculate_volume_spike(self, symbol: str, current_volume: int) -> Dict[str, Any]:
        """
        Detect Volume Spike
        spike_ratio = current_volume / avg_volume_per_tick
        """
        if symbol not in self.volume_history:
            self.volume_history[symbol] = deque(maxlen=60)  # Last 60 ticks (~1 min)
            self.start_time[symbol] = time.time()
            self.tick_count[symbol] = 0
        
        self.volume_history[symbol].append(current_volume)
        self.tick_count[symbol] += 1
        
        if len(self.volume_history[symbol]) < 5:
            return {'ratio': 1.0, 'signal': 'NEUTRAL', 'strength': 0}
        
        avg_volume = sum(self.volume_history[symbol]) / len(self.volume_history[symbol])
        spike_ratio = current_volume / avg_volume if avg_volume > 0 else 1.0
        
        # Determine signal
        if spike_ratio >= 3:
            signal = 'VERY_STRONG'
            strength = min((spike_ratio - 3) / 2, 1.0)
        elif spike_ratio >= 2:
            signal = 'STRONG_MOVE'
            strength = (spike_ratio - 2)
        else:
            signal = 'NORMAL'
            strength = 0
        
        return {
            'ratio': round(spike_ratio, 2),
            'signal': signal,
            'strength': round(strength, 2),
            'current': current_volume,
            'avg': int(avg_volume)
        }
    
    def check_buy_signal(self, symbol: str, tick_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Generate BUY signal based on multiple factors
        Requires 3+ confirming signals
        """
        signals = []
        signal_count = 0
        
        # Extract data
        tbq = self.to_num(tick_data.get('tbq', 0))
        tsq = self.to_num(tick_data.get('tsq', 0))
        ltp = self.to_num(tick_data.get('ltpc', {}).get('ltp', 0))
        ltq = self.to_num(tick_data.get('ltpc', {}).get('ltq', 0))
        
        # Signal 1: Order Flow Imbalance > 0.5
        ofi_result = self.calculate_ofi(tbq, tsq)
        if ofi_result['value'] > 0.5:
            signal_count += 1
            signals.append(f"Strong Buy Flow (OFI: {ofi_result['value']:.2f})")
        
        # Signal 2: Price breaking resistance (bid/ask analysis)
        bid_ask_quotes = tick_data.get('marketLevel', {}).get('bidAskQuote', [])
        if bid_ask_quotes:
            top_ask = self.to_num(bid_ask_quotes[0].get('askP', 0))
            if ltp >= top_ask and top_ask > 0:
                signal_count += 1
                signals.append(f"Breaking Ask ({ltp:.2f} >= {top_ask:.2f})")
        
        # Signal 3: Volume surge
        volume_spike = self.calculate_volume_spike(symbol, int(ltq))
        if volume_spike['ratio'] > 2:
            signal_count += 1
            signals.append(f"Volume Spike ({volume_spike['ratio']:.1f}x)")
        
        # Signal 4: Delta increasing
        if symbol not in self.delta_history:
            self.delta_history[symbol] = deque(maxlen=5)
        
        greeks = tick_data.get('optionGreeks', {})
        current_delta = self.to_num(greeks.get('delta', 0))
        
        if len(self.delta_history[symbol]) > 0:
            prev_delta = self.delta_history[symbol][-1]
            if current_delta > prev_delta:
                signal_count += 1
                signals.append(f"Delta Rising ({prev_delta:.3f} → {current_delta:.3f})")
        
        self.delta_history[symbol].append(current_delta)
        
        # Signal 5: OI increasing
        if symbol not in self.oi_history:
            self.oi_history[symbol] = deque(maxlen=5)
        
        current_oi = self.to_num(tick_data.get('oi', 0))
        
        if len(self.oi_history[symbol]) > 0:
            prev_oi = self.oi_history[symbol][-1]
            if current_oi > prev_oi:
                signal_count += 1
                signals.append(f"OI Increasing ({int(prev_oi):,} → {int(current_oi):,})")
        
        self.oi_history[symbol].append(current_oi)
        
        # Generate BUY signal if 3+ conditions met
        if signal_count >= 3:
            confidence = signal_count / 5.0
            return {
                'action': 'BUY',
                'symbol': symbol,
                'entry': round(ltp, 2),
                'confidence': round(confidence, 2),
                'signal_count': signal_count,
                'signals': signals,
                'timestamp': time.time()
            }
        
        return None
    
    def check_sell_signal(
        self,
        symbol: str,
        tick_data: Dict[str, Any],
        entry_price: Optional[float] = None,
        target_points: float = 5.0
    ) -> Optional[Dict[str, Any]]:
        """
        Generate SELL signal based on multiple factors
        Requires 3+ confirming signals
        """
        signals = []
        signal_count = 0
        
        # Extract data
        tbq = self.to_num(tick_data.get('tbq', 0))
        tsq = self.to_num(tick_data.get('tsq', 0))
        ltp = self.to_num(tick_data.get('ltpc', {}).get('ltp', 0))
        
        # Signal 1: Order Flow Negative (< 0.2)
        ofi_result = self.calculate_ofi(tbq, tsq)
        if ofi_result['value'] < 0.2:
            signal_count += 1
            signals.append(f"Weak Flow (OFI: {ofi_result['value']:.2f})")
        
        # Signal 2: Price rejecting resistance
        if symbol in self.price_history and len(self.price_history[symbol]) > 1:
            prev_ltp = list(self.price_history[symbol])[-2]
            if ltp < prev_ltp:
                signal_count += 1
                signals.append(f"Price Rejecting ({prev_ltp:.2f} → {ltp:.2f})")
        
        # Signal 3: Bid quantity dropping
        if symbol in self.tick_count and self.tick_count[symbol] > 2:
            # Compare with previous tick (simplified - would need to store previous tbq)
            if tbq < tsq:
                signal_count += 1
                signals.append(f"Bid Weakness (Bid < Ask)")
        
        # Signal 4: Delta decreasing
        if symbol in self.delta_history and len(self.delta_history[symbol]) > 1:
            current_delta = self.delta_history[symbol][-1]
            prev_delta = self.delta_history[symbol][-2]
            if current_delta < prev_delta - 0.02:
                signal_count += 1
                signals.append(f"Delta Falling ({prev_delta:.3f} → {current_delta:.3f})")
        
        # Signal 5: Profit target hit
        if entry_price and ltp >= entry_price + target_points:
            signal_count += 1
            profit = ltp - entry_price
            signals.append(f"Target Hit (+{profit:.2f} points)")
        
        # Generate SELL signal if 3+ conditions met
        if signal_count >= 3:
            reason = "Target/Weakness" if entry_price and ltp >= entry_price + target_points else "Weakness Detected"
            return {
                'action': 'SELL',
                'symbol': symbol,
                'exit': round(ltp, 2),
                'signal_count': signal_count,
                'signals': signals,
                'reason': reason,
                'timestamp': time.time()
            }
        
        return None
    
    def calculate_stop_loss(
        self,
        entry_price: float,
        tick_data: Dict[str, Any],
        method: str = 'fixed'
    ) -> Dict[str, Any]:
        """
        Calculate dynamic stop loss
        Methods: fixed, atr, support
        """
        ltp = self.to_num(tick_data.get('ltpc', {}).get('ltp', 0))
        
        # Method 1: Fixed percentage (1.5%)
        sl_fixed = entry_price * 0.985
        
        # Method 2: ATR-based (simplified - using price range)
        ohlc_data = tick_data.get('marketOHLC', {}).get('ohlc', [])
        atr = 0
        if ohlc_data:
            candle = ohlc_data[0]
            high = self.to_num(candle.get('high', 0))
            low = self.to_num(candle.get('low', 0))
            atr = (high - low) * 2 if high > 0 else 0
        sl_atr = entry_price - atr if atr > 0 else sl_fixed
        
        # Method 3: Support-based (using bid level)
        bid_ask = tick_data.get('marketLevel', {}).get('bidAskQuote', [])
        sl_support = sl_fixed
        if bid_ask:
            best_bid = self.to_num(bid_ask[0].get('bidP', 0))
            sl_support = best_bid - 0.5 if best_bid > 0 else sl_fixed
        
        # Use tightest SL
        stop_loss = max(sl_fixed, sl_atr, sl_support)
        
        return {
            'stop_loss': round(stop_loss, 2),
            'sl_fixed': round(sl_fixed, 2),
            'sl_atr': round(sl_atr, 2),
            'sl_support': round(sl_support, 2),
            'risk': round(entry_price - stop_loss, 2),
            'risk_pct': round(((entry_price - stop_loss) / entry_price) * 100, 2)
        }
    
    def trail_stop_loss(
        self,
        current_price: float,
        entry_price: float,
        current_sl: float
    ) -> Dict[str, Any]:
        """
        Trailing stop loss based on profit levels
        """
        profit = current_price - entry_price
        new_sl = current_sl
        action = 'HOLD'
        
        if profit >= 6:
            # Trail: SL = Current - 2
            new_sl = current_price - 2
            action = 'TRAIL_TIGHT'
        elif profit >= 4:
            # Trail: SL = Entry + 2
            new_sl = entry_price + 2
            action = 'TRAIL_MODERATE'
        elif profit >= 2:
            # Lock profit: SL = Entry (breakeven)
            new_sl = entry_price
            action = 'BREAKEVEN'
        
        return {
            'new_sl': round(new_sl, 2),
            'current_sl': round(current_sl, 2),
            'profit': round(profit, 2),
            'action': action,
            'locked_profit': round(max(0, new_sl - entry_price), 2)
        }
    
    def analyze_tick(self, symbol: str, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Complete tick analysis with all indicators
        """
        # Extract core data
        tbq = self.to_num(tick_data.get('tbq', 0))
        tsq = self.to_num(tick_data.get('tsq', 0))
        ltpc = tick_data.get('ltpc', {})
        ltp = self.to_num(ltpc.get('ltp', 0))
        ltq = self.to_num(ltpc.get('ltq', 0))
        
        # Get bid/ask for spread
        bid_ask = tick_data.get('marketLevel', {}).get('bidAskQuote', [])
        best_bid = self.to_num(bid_ask[0].get('bidP', ltp)) if bid_ask else ltp
        best_ask = self.to_num(bid_ask[0].get('askP', ltp)) if bid_ask else ltp
        
        # Calculate all indicators
        ofi = self.calculate_ofi(tbq, tsq)
        spread = self.calculate_spread(best_bid, best_ask, ltp)
        momentum = self.calculate_momentum(symbol, ltp)
        volume_spike = self.calculate_volume_spike(symbol, int(ltq))
        
        # Check for entry signals
        buy_signal = self.check_buy_signal(symbol, tick_data)
        sell_signal = self.check_sell_signal(symbol, tick_data)
        
        return {
            'symbol': symbol,
            'ltp': round(ltp, 2),
            'indicators': {
                'ofi': ofi,
                'spread': spread,
                'momentum': momentum,
                'volume_spike': volume_spike
            },
            'signals': {
                'buy': buy_signal,
                'sell': sell_signal
            },
            'timestamp': time.time()
        }