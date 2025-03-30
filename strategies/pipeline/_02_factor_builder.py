import pandas as pd
import numpy as np
from typing import Dict, Any, List
import logging
import talib

class FactorBuilder:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.factors: Dict[str, callable] = {}
        
    def register_factor(self, name: str, factor_func: callable):
        """注册因子计算函数
        
        Args:
            name: 因子名称
            factor_func: 因子计算函数
        """
        self.factors[name] = factor_func
        
    def build_factors(self, market_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """构建所有因子
        
        Args:
            market_data: 处理后的市场数据字典
            
        Returns:
            因子数据DataFrame
        """
        factor_data = pd.DataFrame(index=market_data['tick'].index)
        
        for name, factor_func in self.factors.items():
            try:
                factor_data[name] = factor_func(market_data)
            except Exception as e:
                self.logger.error(f"计算因子 {name} 时出错: {str(e)}")
                
        return factor_data
    
    @staticmethod
    def create_technical_factors(market_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """创建技术指标因子
        
        Args:
            market_data: 市场数据字典
            
        Returns:
            技术指标因子DataFrame
        """
        tick_data = market_data['tick']
        daily_data = market_data['daily']
        
        factors = pd.DataFrame(index=tick_data.index)
        
        # RSI
        factors['rsi'] = talib.RSI(tick_data['close'].values)
        
        # MACD
        macd, macd_signal, macd_hist = talib.MACD(tick_data['close'].values)
        factors['macd'] = macd
        factors['macd_signal'] = macd_signal
        factors['macd_hist'] = macd_hist
        
        # KDJ
        slowk, slowd = talib.STOCH(tick_data['high'].values, 
                                  tick_data['low'].values, 
                                  tick_data['close'].values)
        factors['kdj_k'] = slowk
        factors['kdj_d'] = slowd
        
        # 布林带
        upper, middle, lower = talib.BBANDS(tick_data['close'].values)
        factors['bb_upper'] = upper
        factors['bb_middle'] = middle
        factors['bb_lower'] = lower
        
        return factors
    
    @staticmethod
    def create_price_factors(market_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """创建价格特征因子
        
        Args:
            market_data: 市场数据字典
            
        Returns:
            价格特征因子DataFrame
        """
        tick_data = market_data['tick']
        daily_data = market_data['daily']
        
        factors = pd.DataFrame(index=tick_data.index)
        
        # 收益率
        factors['returns'] = tick_data['close'].pct_change()
        
        # 波动率
        factors['volatility'] = tick_data['close'].rolling(20).std()
        
        # 动量
        factors['momentum'] = tick_data['close'].pct_change(10)
        
        # 趋势强度
        factors['trend_strength'] = (tick_data['close'] - tick_data['close'].rolling(20).mean()) / tick_data['close'].rolling(20).std()
        
        return factors
    
    @staticmethod
    def create_volume_factors(market_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """创建成交量特征因子
        
        Args:
            market_data: 市场数据字典
            
        Returns:
            成交量特征因子DataFrame
        """
        tick_data = market_data['tick']
        daily_data = market_data['daily']
        
        factors = pd.DataFrame(index=tick_data.index)
        
        # 成交量变化
        factors['volume_change'] = tick_data['volume'].pct_change()
        
        # 量价背离
        factors['volume_price_divergence'] = tick_data['volume'].pct_change() - tick_data['close'].pct_change()
        
        # 资金流向
        factors['money_flow'] = (tick_data['close'] - tick_data['open']) * tick_data['volume']
        
        # 成交量趋势
        factors['volume_trend'] = tick_data['volume'].rolling(20).mean().pct_change()
        
        return factors
    
    @staticmethod
    def create_market_microstructure_factors(market_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """创建市场微观结构因子
        
        Args:
            market_data: 市场数据字典
            
        Returns:
            市场微观结构因子DataFrame
        """
        tick_data = market_data['tick']
        
        factors = pd.DataFrame(index=tick_data.index)
        
        # 买卖盘口压力
        if 'bid_volume' in tick_data.columns and 'ask_volume' in tick_data.columns:
            factors['order_book_pressure'] = (tick_data['bid_volume'] - tick_data['ask_volume']) / (tick_data['bid_volume'] + tick_data['ask_volume'])
        
        # 价格冲击
        factors['price_impact'] = tick_data['close'].pct_change() / tick_data['volume']
        
        # 交易活跃度
        factors['trading_activity'] = tick_data['volume'] / tick_data['volume'].rolling(20).mean()
        
        return factors
    
    @staticmethod
    def create_combined_factors(market_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """创建组合因子
        
        Args:
            market_data: 市场数据字典
            
        Returns:
            组合因子DataFrame
        """
        tick_data = market_data['tick']
        daily_data = market_data['daily']
        
        factors = pd.DataFrame(index=tick_data.index)
        
        # 量价趋势一致性
        price_trend = tick_data['close'].rolling(20).mean().pct_change()
        volume_trend = tick_data['volume'].rolling(20).mean().pct_change()
        factors['price_volume_trend_alignment'] = price_trend * volume_trend
        
        # 多周期动量
        short_momentum = tick_data['close'].pct_change(5)
        long_momentum = tick_data['close'].pct_change(20)
        factors['momentum_contrast'] = short_momentum - long_momentum
        
        # 波动率调整后的收益率
        returns = tick_data['close'].pct_change()
        volatility = tick_data['close'].rolling(20).std()
        factors['volatility_adjusted_returns'] = returns / volatility
        
        return factors 