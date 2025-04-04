import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List
from core.data_feed import DataFeed

class DataProcessor:
    """数据处理器类"""
    
    def __init__(self, data_feed: DataFeed):
        self.logger = logging.getLogger(__name__)
        self.data_feed = data_feed
                
    def prepare_data(self, stock_list: List[str], start_date: str, end_date: str, period: str = 'tick', field: List[str]=[]):
        """
        准备数据
        
        Args:
            stock_list: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            period: 数据周期，默认为'tick'
            fields: 字段列表，默认为None
            
        Returns:
            dict: 股票代码为key，DataFrame为value的字典
        """
        self.logger.info(f"准备数据: 股票列表={stock_list}, 开始日期={start_date}, 结束日期={end_date}, 周期={period}, 字段={field}")
                
        self.market_data = self.data_feed.get_market_data(stock_list, start_date, end_date, period, field)

    

    def process_data(self):
        """
        处理市场数据
        
        Args:
            market_data: 市场数据，字典格式，key为股票代码，value为DataFrame
            
        Returns:
            处理后的市场数据
            
        Raises:
            ValueError: 当市场数据为空时抛出
        """
        self.logger.info("开始处理市场数据")
        
        if not self.market_data:
            raise ValueError("原始市场数据为空")
            
        self.logger.info(f"原始市场数据包含 {len(self.market_data)} 只股票")
        
        processed_data = {}
        for stock_code, df in self.market_data.items():
            try:
                if df is None or df.empty:
                    self.logger.warning(f"股票 {stock_code} 的数据为空，跳过处理")
                    continue
                    
                self.logger.info(f"处理股票 {stock_code} 的数据，形状: {df.shape}")
                
                # 确保索引是datetime类型
                if not isinstance(df.index, pd.DatetimeIndex):
                    df.index = pd.to_datetime(df.index)
                    
                # 确保列名是小写
                df.columns = df.columns.str.lower()
                
                # 添加股票代码列
                df['stock_code'] = stock_code
                
                processed_data[stock_code] = df
                
            except Exception as e:
                self.logger.error(f"处理股票 {stock_code} 的数据时出错: {str(e)}")
                
        if not processed_data:
            raise ValueError("处理后的市场数据为空")
            
        self.logger.info(f"处理后的市场数据包含 {len(processed_data)} 只股票")
        return processed_data