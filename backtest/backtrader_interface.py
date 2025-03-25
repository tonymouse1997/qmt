import backtrader as bt
import logging
from core.trade_interface import TradeInterface

class BacktraderTradeInterface(TradeInterface):
    """Backtrader交易接口实现"""
    
    def __init__(self, broker):
        self.broker = broker
        
    def buy(self, stock_code: str, price: float, volume: int):
        """买入股票"""
        try:
            order = self.broker.buy(data=stock_code, size=volume, price=price)
            return order.status == bt.Order.Completed
        except Exception as e:
            logging.error(f"买入股票 {stock_code} 失败: {str(e)}")
            return False
            
    def sell(self, stock_code: str, price: float, volume: int):
        """卖出股票"""
        try:
            order = self.broker.sell(data=stock_code, size=volume, price=price)
            return order.status == bt.Order.Completed
        except Exception as e:
            logging.error(f"卖出股票 {stock_code} 失败: {str(e)}")
            return False
            
    def get_position(self, stock_code: str):
        """获取持仓信息"""
        try:
            position = self.broker.getposition(data=stock_code)
            if position:
                return {
                    'volume': position.size,
                    'price': position.price
                }
            return None
        except Exception as e:
            logging.error(f"获取持仓信息失败: {str(e)}")
            return None
            
    def get_cash(self):
        """获取可用资金"""
        return self.broker.getcash()
        
    def get_total_asset(self):
        """获取总资产"""
        return self.broker.getvalue() 