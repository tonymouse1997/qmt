import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Callable

class FactorBuilder:
    """因子构建器类"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.factor_registry = {}
        
    def register_factor(self, factor_name: str, factor_func: Callable):
        """注册因子计算函数
        
        Args:
            factor_name: 因子名称
            factor_func: 因子计算函数
        """
        self.factor_registry[factor_name] = factor_func
        
    def build_factors(self, market_data):
        """
        构建因子
        
        Args:
            market_data: 市场数据，字典格式，key为股票代码，value为DataFrame
            
        Returns:
            因子数据，字典格式，key为股票代码，value为DataFrame
            
        Raises:
            ValueError: 当市场数据为空时抛出
        """
        self.logger.info("开始构建因子")
        
        if not market_data:
            raise ValueError("市场数据为空")
            
        self.logger.info(f"市场数据包含 {len(market_data)} 只股票")
        
        factor_data = {}
        for stock_code, df in market_data.items():
            try:
                if df is None or df.empty:
                    self.logger.warning(f"股票 {stock_code} 的数据为空，跳过因子构建")
                    continue
                    
                self.logger.info(f"构建股票 {stock_code} 的因子，数据形状: {df.shape}")
                
                # 创建技术因子
                technical_factors = self.create_technical_factors(df)
                
                # 创建价格因子
                price_factors = self.create_price_factors(df)
                
                # 创建成交量因子
                volume_factors = self.create_volume_factors(df)
                
                # 创建市场微观结构因子
                microstructure_factors = self.create_market_microstructure_factors(df)
                
                # 创建组合因子
                combined_factors = self.create_combined_factors(df)
                
                # 合并所有因子
                all_factors = pd.concat([
                    technical_factors,
                    price_factors,
                    volume_factors,
                    microstructure_factors,
                    combined_factors
                ], axis=1)
                
                factor_data[stock_code] = all_factors
                self.logger.info(f"股票 {stock_code} 的因子构建完成，因子数量: {all_factors.shape[1]}")
                
            except Exception as e:
                self.logger.error(f"构建股票 {stock_code} 的因子时出错: {str(e)}")
                raise
                
        if not factor_data:
            raise ValueError("因子数据为空")
            
        self.logger.info(f"因子构建完成，共 {len(factor_data)} 只股票")
        return factor_data
        
    def create_technical_factors(self, data: pd.DataFrame) -> pd.DataFrame:
        """创建技术因子
        
        Args:
            data: 市场数据
            
        Returns:
            技术因子DataFrame
        """
        try:
            self.logger.info("开始创建技术因子")
            
            # 检查数据列
            required_columns = ['lastprice']
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                self.logger.error(f"缺少必要的列: {missing_columns}")
                self.logger.info(f"可用的列: {data.columns.tolist()}")
                # 尝试使用替代列
                if 'lastprice' not in data.columns and 'close' in data.columns:
                    self.logger.info("使用'close'列替代'lastprice'列")
                    data = data.copy()
                    data['lastprice'] = data['close']
                else:
                    raise ValueError(f"缺少必要的列: {missing_columns}")
            
            factors = pd.DataFrame(index=data.index)
            
            # 计算移动平均
            factors['ma5'] = data['lastprice'].rolling(5).mean()
            factors['ma10'] = data['lastprice'].rolling(10).mean()
            factors['ma20'] = data['lastprice'].rolling(20).mean()
            
            # 计算RSI
            delta = data['lastprice'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            factors['rsi'] = 100 - (100 / (1 + rs))
            
            self.logger.info(f"技术因子创建成功，形状: {factors.shape}")
            return factors
            
        except Exception as e:
            self.logger.error(f"创建技术因子时出错: {str(e)}")
            raise
        
    def create_price_factors(self, data: pd.DataFrame) -> pd.DataFrame:
        """创建价格因子
        
        Args:
            data: 市场数据
            
        Returns:
            价格因子DataFrame
        """
        try:
            self.logger.info("开始创建价格因子")
            
            # 检查数据列
            required_columns = ['lastprice', 'openprice', 'highprice', 'lowprice']
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                self.logger.error(f"缺少必要的列: {missing_columns}")
                self.logger.info(f"可用的列: {data.columns.tolist()}")
                # 尝试使用替代列
                data = data.copy()
                if 'lastprice' not in data.columns and 'close' in data.columns:
                    self.logger.info("使用'close'列替代'lastprice'列")
                    data['lastprice'] = data['close']
                if 'openprice' not in data.columns and 'open' in data.columns:
                    self.logger.info("使用'open'列替代'openprice'列")
                    data['openprice'] = data['open']
                if 'highprice' not in data.columns and 'high' in data.columns:
                    self.logger.info("使用'high'列替代'highprice'列")
                    data['highprice'] = data['high']
                if 'lowprice' not in data.columns and 'low' in data.columns:
                    self.logger.info("使用'low'列替代'lowprice'列")
                    data['lowprice'] = data['low']
                
                # 再次检查是否所有必要的列都存在
                missing_columns = [col for col in required_columns if col not in data.columns]
                if missing_columns:
                    raise ValueError(f"缺少必要的列: {missing_columns}")
            
            factors = pd.DataFrame(index=data.index)
            
            # 计算价格动量
            factors['price_momentum_1d'] = data['lastprice'].pct_change(1)
            factors['price_momentum_5d'] = data['lastprice'].pct_change(5)
            factors['price_momentum_10d'] = data['lastprice'].pct_change(10)
            
            # 计算价格波动率
            factors['price_volatility_5d'] = data['lastprice'].rolling(5).std()
            factors['price_volatility_10d'] = data['lastprice'].rolling(10).std()
            
            # 计算价格区间
            factors['price_range'] = (data['highprice'] - data['lowprice']) / data['openprice']
            
            self.logger.info(f"价格因子创建成功，形状: {factors.shape}")
            return factors
            
        except Exception as e:
            self.logger.error(f"创建价格因子时出错: {str(e)}")
            raise
        
    def create_volume_factors(self, data: pd.DataFrame) -> pd.DataFrame:
        """创建成交量因子
        
        Args:
            data: 市场数据
            
        Returns:
            成交量因子DataFrame
        """
        try:
            self.logger.info("开始创建成交量因子")
            
            # 检查数据列
            required_columns = ['volume', 'lastprice']
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                self.logger.error(f"缺少必要的列: {missing_columns}")
                self.logger.info(f"可用的列: {data.columns.tolist()}")
                # 尝试使用替代列
                data = data.copy()
                if 'lastprice' not in data.columns and 'close' in data.columns:
                    self.logger.info("使用'close'列替代'lastprice'列")
                    data['lastprice'] = data['close']
                
                # 再次检查是否所有必要的列都存在
                missing_columns = [col for col in required_columns if col not in data.columns]
                if missing_columns:
                    raise ValueError(f"缺少必要的列: {missing_columns}")
            
            factors = pd.DataFrame(index=data.index)
            
            # 计算成交量变化
            factors['volume_change'] = data['volume'].pct_change()
            
            # 计算成交量移动平均
            factors['volume_ma5'] = data['volume'].rolling(5).mean()
            factors['volume_ma10'] = data['volume'].rolling(10).mean()
            
            # 计算成交量相对强度
            factors['volume_relative_strength'] = data['volume'] / data['volume'].rolling(5).mean()
            
            # 计算成交额
            factors['amount'] = data['volume'] * data['lastprice']
            factors['amount_ma5'] = factors['amount'].rolling(5).mean()
            
            self.logger.info(f"成交量因子创建成功，形状: {factors.shape}")
            return factors
            
        except Exception as e:
            self.logger.error(f"创建成交量因子时出错: {str(e)}")
            raise
        
    def create_market_microstructure_factors(self, data: pd.DataFrame) -> pd.DataFrame:
        """创建市场微观结构因子
        
        Args:
            data: 市场数据
            
        Returns:
            市场微观结构因子DataFrame
        """
        try:
            self.logger.info("开始创建市场微观结构因子")
            
            # 检查数据列
            required_columns = ['lastprice', 'volume']
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                self.logger.error(f"缺少必要的列: {missing_columns}")
                self.logger.info(f"可用的列: {data.columns.tolist()}")
                # 尝试使用替代列
                data = data.copy()
                if 'lastprice' not in data.columns and 'close' in data.columns:
                    self.logger.info("使用'close'列替代'lastprice'列")
                    data['lastprice'] = data['close']
                
                # 再次检查是否所有必要的列都存在
                missing_columns = [col for col in required_columns if col not in data.columns]
                if missing_columns:
                    raise ValueError(f"缺少必要的列: {missing_columns}")
            
            factors = pd.DataFrame(index=data.index)
            
            # 计算价格波动率
            factors['price_volatility'] = data['lastprice'].rolling(5).std()
            
            # 计算成交量波动率
            factors['volume_volatility'] = data['volume'].rolling(5).std()
            
            # 计算价格-成交量相关性
            price_change = data['lastprice'].pct_change()
            volume_change = data['volume'].pct_change()
            factors['price_volume_correlation'] = price_change.rolling(5).corr(volume_change)
            
            # 计算价格自相关性
            factors['price_autocorrelation'] = data['lastprice'].rolling(5).apply(
                lambda x: x.autocorr() if len(x) > 1 else 0
            )
            
            self.logger.info(f"市场微观结构因子创建成功，形状: {factors.shape}")
            return factors
            
        except Exception as e:
            self.logger.error(f"创建市场微观结构因子时出错: {str(e)}")
            raise
        
    def create_combined_factors(self, data: pd.DataFrame) -> pd.DataFrame:
        """创建组合因子
        
        Args:
            data: 市场数据
            
        Returns:
            组合因子DataFrame
        """
        try:
            self.logger.info("开始创建组合因子")
            
            # 检查数据列
            required_columns = ['lastprice', 'volume']
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                self.logger.error(f"缺少必要的列: {missing_columns}")
                self.logger.info(f"可用的列: {data.columns.tolist()}")
                # 尝试使用替代列
                data = data.copy()
                if 'lastprice' not in data.columns and 'close' in data.columns:
                    self.logger.info("使用'close'列替代'lastprice'列")
                    data['lastprice'] = data['close']
                
                # 再次检查是否所有必要的列都存在
                missing_columns = [col for col in required_columns if col not in data.columns]
                if missing_columns:
                    raise ValueError(f"缺少必要的列: {missing_columns}")
            
            factors = pd.DataFrame(index=data.index)
            
            # 计算价格动量与成交量的组合因子
            price_momentum = data['lastprice'].pct_change(5)
            volume_momentum = data['volume'].pct_change(5)
            factors['price_volume_momentum'] = price_momentum * volume_momentum
            
            # 计算价格波动率与成交量的组合因子
            price_volatility = data['lastprice'].rolling(5).std()
            volume_volatility = data['volume'].rolling(5).std()
            factors['price_volume_volatility'] = price_volatility * volume_volatility
            
            # 计算价格趋势与成交量的组合因子
            price_trend = data['lastprice'].rolling(5).mean().pct_change()
            volume_trend = data['volume'].rolling(5).mean().pct_change()
            factors['price_volume_trend'] = price_trend * volume_trend
            
            self.logger.info(f"组合因子创建成功，形状: {factors.shape}")
            return factors
            
        except Exception as e:
            self.logger.error(f"创建组合因子时出错: {str(e)}")
            raise 