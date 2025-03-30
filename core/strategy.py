from abc import ABC, abstractmethod
from core.data_feed import DataFeed
from core.trade_interface import TradeInterface

class Strategy(ABC):
    """策略基类"""
    
    def __init__(self, data_feed: DataFeed, trade_interface: TradeInterface):
        self.data_feed = data_feed
        self.trade_interface = trade_interface
    
    @abstractmethod
    def on_data(self):
        """处理每个数据点的逻辑"""
        pass
        
    def buy(self, stock_code: str, price: float, volume: int) -> bool:
        """买入股票"""
        return self.trade_interface.buy(stock_code, price, volume)
        
    def sell(self, stock_code: str, price: float, volume: int) -> bool:
        """卖出股票"""
        return self.trade_interface.sell(stock_code, price, volume)