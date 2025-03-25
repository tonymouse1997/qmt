from abc import ABC, abstractmethod

class TradeInterface(ABC):
    """交易接口"""
    
    @abstractmethod
    def buy(self, stock_code: str, price: float, volume: int) -> bool:
        """买入股票"""
        pass
    
    @abstractmethod
    def sell(self, stock_code: str, price: float, volume: int) -> bool:
        """卖出股票"""
        pass
    
    @abstractmethod
    def get_position(self, stock_code: str) -> dict:
        """获取持仓信息"""
        pass
    
    @abstractmethod
    def get_cash(self) -> float:
        """获取可用资金"""
        pass
    
    @abstractmethod
    def get_total_asset(self) -> float:
        """获取总资产"""
        pass 