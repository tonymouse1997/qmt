from abc import ABC, abstractmethod
import pandas as pd
from functools import wraps
from typing import Dict, List, Any

def validate_market_data(func):
    """验证市场数据返回格式的装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        
        if not isinstance(result, dict):
            raise TypeError(f"返回值必须是字典类型，但得到 {type(result)}")
        
        for key, value in result.items():
            if not isinstance(key, str):
                raise TypeError(f"字典的键必须是字符串类型，但得到 {type(key)}")
            if not isinstance(value, pd.DataFrame):
                raise TypeError(f"字典的值必须是DataFrame类型，但得到 {type(value)}")
            if not isinstance(value.index, pd.DatetimeIndex):
                raise TypeError(f"DataFrame的索引必须是datetime类型，但得到 {type(value.index)}")
        
        return result
    return wrapper

class DataFeed(ABC):
    """数据源抽象基类"""
    @abstractmethod
    @validate_market_data
    def get_market_data(self, stock_list: List[str], start_date: str, end_date: str, 
                       period: str = 'tick', field: List[str] = None) -> Dict[str, Any]:
        """获取市场数据
        
        Args:
            stock_list: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            period: 数据周期
            fields: 字段列表
            
        Returns:
            市场数据字典
        """
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
    
    @abstractmethod
    def download_data(self, stock_list: List[str], period: str = '1d',
                     start_time: str = None, end_time: str = None):
        """下载历史数据
        
        Args:
            stock_list: 股票代码列表
            period: 数据周期
            start_time: 开始时间
            end_time: 结束时间
        """
        pass 