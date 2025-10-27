"""
Strong Signal Analyzer - Option Buying Only
Tamil: வலுவான signals மட்டும் - Noise இல்லை
Low noise, high confidence signals for option buying
"""
from typing import Dict, List, Optional, Any
from collections import deque
import time

class StrongSignalAnalyzer:
    def __init__(self, lookback_ticks=10):
        self.lookback_ticks = lookback_ticks
        self.price_history: Dict[str, deque] = {}
        self.volume_history: Dict[str, deque] = {}
        self.delta_history: Dict[str, deque] = {}
        self.gamma_history: Dict[str, deque] = {}
        self.oi_history: Dict[str, deque] = {}
        self.ofi_history: Dict[str, deque] = {}
        self.tick_count: Dict[str, int] = {}
        self.start_time: Dict[str, float] = {}
        
    def to_num(self, value: Any) -> float:
        """Convert safely to number"""
        if value is None or value == '':
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def calculate_ofi(self, tbq: float, tsq: float) -> Dict[str, Any]:
        """Calculate Order Flow Imbalance - வாங்கும் விற்கும் pressure"""
        if tbq + tsq == 0:
            return {'value': 0, 'signal': 'NEUTRAL', 'strength': 0}
        
        ofi = (tbq - tsq) / (tbq + tsq)
        
        # STRONG thresholds - noise குறைக்க
        if ofi > 0.7:  # Very strong buy pressure
            signal = 'VERY_STRONG_BUY'
            strength = 1.0
        elif ofi > 0.6:  # Strong buy
            signal = 'STRONG_BUY'
            strength = 0.8
        else:
            signal = 'WEAK'  # Ignore weak signals
            strength = 0
        
        return {
            'value': round(ofi, 4),
            'signal': signal,
            'strength': round(strength, 2),
            'tbq': int(tbq),
            'tsq': int(tsq)
        }
    
    def calculate_volume_spike(self, symbol: str, current_volume: int) -> Dict[str, Any]:
        """Volume Spike - மிக அதிக volume மட்டும்"""
        if symbol not in self.volume_history:
            self.volume_history[symbol] = deque(maxlen=60)
            self.tick_count[symbol] = 0
        
        self.volume_history[symbol].append(current_volume)
        self.tick_count[symbol] += 1
        
        if len(self.volume_history[symbol]) < 10:
            return {'ratio': 1.0, 'signal': 'WEAK', 'strength': 0}
        
        avg_volume = sum(self.volume_history[symbol]) / len(self.volume_history[symbol])
        spike_ratio = current_volume / avg_volume if avg_volume > 0 else 1.0
        
        # VERY HIGH thresholds - noise இல்லாமல்
        if spike_ratio >= 5:  # 5x volume spike!
            signal = 'EXTREME_SPIKE'
            strength = 1.0
        elif spike_ratio >= 4:  # 4x spike
            signal = 'VERY_STRONG_SPIKE'
            strength = 0.9
        elif spike_ratio >= 3:  # 3x spike
            signal = 'STRONG_SPIKE'
            strength = 0.7
        else:
            signal = 'WEAK'
            strength = 0
        
        return {
            'ratio': round(spike_ratio, 2),
            'signal': signal,
            'strength': round(strength, 2),
            'current': current_volume,
            'avg': int(avg_volume)
        }
    
    def calculate_momentum(self, symbol: str, current_ltp: float) -> Dict[str, Any]:
        """Strong momentum calculation - வலுவான momentum மட்டும்"""
        if symbol not in self.price_history:
            self.price_history[symbol] = deque(maxlen=self.lookback_ticks)
        
        self.price_history[symbol].append(current_ltp)
        
        if len(self.price_history[symbol]) < 5:
            return {'value': 0, 'signal': 'WEAK', 'trend': 'NONE'}
        
        prices = list(self.price_history[symbol])
        
        # Check consistent uptrend - தொடர்ச்சியான uptrend
        uptrend_count = 0
        for i in range(1, len(prices)):
            if prices[i] > prices[i-1]:
                uptrend_count += 1
        
        uptrend_ratio = uptrend_count / (len(prices) - 1)
        
        avg_price = sum(prices) / len(prices)
        momentum = (current_ltp - avg_price) / avg_price if avg_price > 0 else 0
        
        # Strong momentum thresholds
        if uptrend_ratio >= 0.8 and momentum > 0.003:  # 80% uptrend + 0.3% momentum
            signal = 'VERY_STRONG'
            trend = 'CONSISTENT_UP'
        elif uptrend_ratio >= 0.7 and momentum > 0.002:
            signal = 'STRONG'
            trend = 'MOSTLY_UP'
        else:
            signal = 'WEAK'
            trend = 'CHOPPY'
        
        return {
            'value': round(momentum, 5),
            'signal': signal,
            'trend': trend,
            'uptrend_ratio': round(uptrend_ratio, 2),
            'current': round(current_ltp, 2),
            'avg': round(avg_price, 2)
        }
    
    def analyze_greeks_strength(self, symbol: str, greeks: Dict) -> Dict[str, Any]:
        """Greeks strength - Option வாங்குவதற்கு சரியா check செய்யும்"""
        if symbol not in self.gamma_history:
            self.gamma_history[symbol] = deque(maxlen=5)
            self.delta_history[symbol] = deque(maxlen=5)
        
        gamma = self.to_num(greeks.get('gamma', 0))
        delta = self.to_num(greeks.get('delta', 0))
        iv = self.to_num(greeks.get('iv', 0)) if 'iv' in greeks else 0
        
        # Track history
        self.gamma_history[symbol].append(gamma)
        self.delta_history[symbol].append(delta)
        
        # Check gamma increase - Gamma அதிகரிக்குதா
        gamma_increasing = False
        if len(self.gamma_history[symbol]) >= 3:
            recent_gamma = list(self.gamma_history[symbol])[-3:]
            gamma_increasing = recent_gamma[-1] > recent_gamma[0]
        
        # Check delta increase - Delta அதிகரிக்குதா
        delta_increasing = False
        if len(self.delta_history[symbol]) >= 3:
            recent_delta = list(self.delta_history[symbol])[-3:]
            delta_increasing = recent_delta[-1] > recent_delta[0]
        
        score = 0
        signals = []
        
        # High gamma = explosive move possible
        if gamma > 0.002:
            score += 3
            signals.append(f"மிக உயர் Gamma: {gamma:.4f} (பெரிய move வரும்)")
        elif gamma > 0.001:
            score += 2
            signals.append(f"உயர் Gamma: {gamma:.4f}")
        
        # Increasing gamma = building pressure
        if gamma_increasing and gamma > 0.0005:
            score += 2
            signals.append("Gamma அதிகரிக்கிறது (pressure building)")
        
        # High positive delta for calls
        if delta > 0.7:
            score += 3
            signals.append(f"மிக உயர் Delta: {delta:.3f} (Call வாங்க நல்லது)")
        elif delta > 0.6:
            score += 2
            signals.append(f"நல்ல Delta: {delta:.3f}")
        
        # Delta increasing = momentum
        if delta_increasing:
            score += 2
            signals.append("Delta அதிகரிக்கிறது (momentum வருது)")
        
        # IV not too high (avoid overpaying)
        if 0.15 < iv < 0.30:
            score += 1
            signals.append(f"சரியான IV: {iv:.2f} (விலை நல்லா இருக்கு)")
        elif iv > 0.35:
            score -= 2
            signals.append(f"மிக உயர் IV: {iv:.2f} (premium அதிகம்)")
        
        # Final strength
        if score >= 8:
            strength = 'VERY_STRONG'
        elif score >= 5:
            strength = 'STRONG'
        else:
            strength = 'WEAK'
        
        return {
            'score': score,
            'strength': strength,
            'gamma': round(gamma, 4),
            'delta': round(delta, 3),
            'iv': round(iv, 3),
            'gamma_increasing': gamma_increasing,
            'delta_increasing': delta_increasing,
            'signals': signals
        }
    
    def check_strong_buy_signal(
        self,
        symbol: str,
        tick_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Strong BUY signal - 5/7 conditions வேண்டும்
        மிக வலுவான option buying signal மட்டும்
        """
        signals = []
        signal_count = 0
        confidence_score = 0
        
        # Extract data
        tbq = self.to_num(tick_data.get('tbq', 0))
        tsq = self.to_num(tick_data.get('tsq', 0))
        ltp = self.to_num(tick_data.get('ltpc', {}).get('ltp', 0))
        ltq = self.to_num(tick_data.get('ltpc', {}).get('ltq', 0))
        
        # 1. VERY STRONG Order Flow (OFI > 0.6)
        ofi_result = self.calculate_ofi(tbq, tsq)
        if ofi_result['signal'] in ['VERY_STRONG_BUY', 'STRONG_BUY']:
            signal_count += 1
            confidence_score += ofi_result['strength'] * 20
            signals.append(f"✓ மிக வலுவான வாங்கும் pressure (OFI: {ofi_result['value']:.2f})")
        
        # 2. EXTREME Volume Spike (>= 3x)
        volume_spike = self.calculate_volume_spike(symbol, int(ltq))
        if volume_spike['signal'] in ['EXTREME_SPIKE', 'VERY_STRONG_SPIKE', 'STRONG_SPIKE']:
            signal_count += 1
            confidence_score += volume_spike['strength'] * 15
            signals.append(f"✓ Volume வெடிப்பு: {volume_spike['ratio']:.1f}x")
        
        # 3. Strong Momentum (consistent uptrend)
        momentum = self.calculate_momentum(symbol, ltp)
        if momentum['signal'] in ['VERY_STRONG', 'STRONG']:
            signal_count += 1
            confidence_score += 15
            signals.append(f"✓ வலுவான momentum: {momentum['trend']}")
        
        # 4. Greeks Analysis - Option buying க்கு சரியா
        greeks = tick_data.get('optionGreeks', {})
        greeks_analysis = self.analyze_greeks_strength(symbol, greeks)
        if greeks_analysis['strength'] in ['VERY_STRONG', 'STRONG']:
            signal_count += 1
            confidence_score += greeks_analysis['score'] * 2
            for sig in greeks_analysis['signals'][:2]:  # Top 2 signals
                signals.append(f"✓ {sig}")
        
        # 5. OI Increasing - புதிய positions வருது
        if symbol not in self.oi_history:
            self.oi_history[symbol] = deque(maxlen=5)
        
        current_oi = self.to_num(tick_data.get('oi', 0))
        
        if len(self.oi_history[symbol]) > 0:
            avg_oi = sum(self.oi_history[symbol]) / len(self.oi_history[symbol])
            if current_oi > avg_oi * 1.1:  # 10% OI increase
                signal_count += 1
                confidence_score += 10
                signals.append(f"✓ OI அதிகரிப்பு: {int(current_oi):,}")
        
        self.oi_history[symbol].append(current_oi)
        
        # 6. Price Breaking Resistance
        bid_ask_quotes = tick_data.get('marketLevel', {}).get('bidAskQuote', [])
        if bid_ask_quotes:
            top_ask = self.to_num(bid_ask_quotes[0].get('askP', 0))
            if ltp >= top_ask and top_ask > 0:
                signal_count += 1
                confidence_score += 10
                signals.append(f"✓ Resistance breakout: ₹{ltp:.2f}")
        
        # 7. OFI History - தொடர்ச்சியான buy pressure
        if symbol not in self.ofi_history:
            self.ofi_history[symbol] = deque(maxlen=5)
        
        self.ofi_history[symbol].append(ofi_result['value'])
        
        if len(self.ofi_history[symbol]) >= 3:
            recent_ofi = list(self.ofi_history[symbol])[-3:]
            if all(ofi > 0.5 for ofi in recent_ofi):
                signal_count += 1
                confidence_score += 10
                signals.append("✓ தொடர்ச்சியான buy pressure (3+ ticks)")
        
        # STRICT REQUIREMENT: Need 5 out of 7 conditions
        # Noise இல்லாமல் வலுவான signal மட்டும்
        if signal_count >= 5:
            confidence = min(confidence_score / 100, 1.0)
            
            return {
                'action': 'STRONG_BUY',
                'symbol': symbol,
                'entry': round(ltp, 2),
                'confidence': round(confidence, 2),
                'signal_count': signal_count,
                'total_conditions': 7,
                'signals': signals,
                'timestamp': time.time(),
                'greeks': {
                    'gamma': greeks_analysis['gamma'],
                    'delta': greeks_analysis['delta'],
                    'iv': greeks_analysis['iv']
                },
                'ofi': ofi_result['value'],
                'volume_ratio': volume_spike['ratio'],
                'alert': '🔥 STRONG OPTION BUYING SIGNAL 🔥'
            }
        
        return None
    
    def analyze_tick(self, symbol: str, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """Complete analysis with strong signals only"""
        # Extract core data
        tbq = self.to_num(tick_data.get('tbq', 0))
        tsq = self.to_num(tick_data.get('tsq', 0))
        ltpc = tick_data.get('ltpc', {})
        ltp = self.to_num(ltpc.get('ltp', 0))
        ltq = self.to_num(ltpc.get('ltq', 0))
        
        # Calculate indicators
        ofi = self.calculate_ofi(tbq, tsq)
        volume_spike = self.calculate_volume_spike(symbol, int(ltq))
        momentum = self.calculate_momentum(symbol, ltp)
        greeks = tick_data.get('optionGreeks', {})
        greeks_analysis = self.analyze_greeks_strength(symbol, greeks)
        
        # Check for strong buy signal
        buy_signal = self.check_strong_buy_signal(symbol, tick_data)
        
        return {
            'symbol': symbol,
            'ltp': round(ltp, 2),
            'indicators': {
                'ofi': ofi,
                'volume_spike': volume_spike,
                'momentum': momentum,
                'greeks': greeks_analysis
            },
            'signals': {
                'buy': buy_signal,
                'sell': None  # No sell signals - option buying மட்டும்
            },
            'timestamp': time.time()
        }