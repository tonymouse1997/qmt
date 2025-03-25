from abc import ABC, abstractmethod
import logging

class TradeInterface(ABC):
    """交易接口抽象基类"""
    
    @abstractmethod
    def buy(self, stock_code: str, price: float, volume: int):
        """
        买入股票
        :param stock_code: 股票代码
        :param price: 买入价格
        :param volume: 买入数量
        :return: bool 是否成功
        """
        pass
    
    @abstractmethod
    def sell(self, stock_code: str, price: float, volume: int):
        """
        卖出股票
        :param stock_code: 股票代码
        :param price: 卖出价格
        :param volume: 卖出数量
        :return: bool 是否成功
        """
        pass
    
    @abstractmethod
    def get_position(self, stock_code: str):
        """
        获取持仓信息
        :param stock_code: 股票代码
        :return: dict 持仓信息
        """
        pass
    
    @abstractmethod
    def get_cash(self):
        """
        获取可用资金
        :return: float 可用资金
        """
        pass
    
    @abstractmethod
    def get_total_asset(self):
        """
        获取总资产
        :return: float 总资产
        """
        pass 