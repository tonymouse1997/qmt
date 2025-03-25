from abc import ABC, abstractmethod
from core.data_feed import DataFeed
from core.trade_interface import TradeInterface

class Strategy(ABC):
    """策略基类"""
    
    def __init__(self, data_feed: DataFeed, trade_interface: TradeInterface):
        self.data_feed = data_feed
        self.trade_interface = trade_interface
    
    @abstractmethod
    def on_bar(self):
        """每个bar的处理逻辑"""
        pass
    
    @abstractmethod
    def on_tick(self):
        """每个tick的处理逻辑"""
        pass 