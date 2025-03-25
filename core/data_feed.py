from abc import ABC, abstractmethod
import pandas as pd

class DataFeed(ABC):
    """数据源接口"""
    
    @abstractmethod
    def get_tick_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取tick数据"""
        pass
    
    @abstractmethod
    def get_market_data(self, stock_list: list, start_date: str, end_date: str, 
                       period: str = '1d', fields: list = None) -> dict:
        """获取市场数据"""
        pass
    
    @abstractmethod
    def get_sector_stocks(self, sector: str) -> list:
        """获取板块内的股票列表"""
        pass
    
    @abstractmethod
    def get_basic_info_df(self) -> pd.DataFrame:
        """获取基础信息DataFrame"""
        pass
    
    @abstractmethod
    def get_single_stock_turnover(self, stock_code: str) -> float:
        """获取单只股票的平均成交额"""
        pass 