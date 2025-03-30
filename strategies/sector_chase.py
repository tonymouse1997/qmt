import logging
from datetime import datetime
from core.strategy import Strategy
from core.data_feed import DataFeed
from core.trade_interface import TradeInterface

class SectorChaseStrategy(Strategy):
    """板块轮动策略"""
    
    def __init__(self, data_feed: DataFeed, trade_interface: TradeInterface):
        super().__init__(data_feed, trade_interface)
        self.current_sector = None
        self.current_stock = None
        self.last_sector_change = None
        self.sector_change_interval = 5  # 板块切换间隔（天）
        
    def on_data(self):
        """处理每个数据点的逻辑"""
        try:
            # 获取当前日期
            current_date = datetime.now().date()
            
            # 检查是否需要切换板块
            if (self.last_sector_change is None or 
                (current_date - self.last_sector_change).days >= self.sector_change_interval):
                self._change_sector()
            
            # 如果当前没有持仓，选择板块内最强的股票
            if not self.current_stock:
                self._select_best_stock()
                
        except Exception as e:
            logging.error(f"策略执行失败: {str(e)}")
    
    def _change_sector(self):
        """切换板块"""
        try:
            # 获取所有板块
            sectors = self.data_feed._get_sector_info().keys()
            if not sectors:
                logging.error("获取板块列表失败")
                return
                
            # 选择成交额最大的板块
            max_turnover = 0
            best_sector = None
            
            for sector in sectors:
                stocks = self.data_feed.get_sector_stocks(sector)
                if not stocks:
                    continue
                    
                # 计算板块平均成交额
                turnovers = self.data_feed.get_batch_turnover(stocks)
                avg_turnover = sum(turnovers.values()) / len(turnovers)
                
                if avg_turnover > max_turnover:
                    max_turnover = avg_turnover
                    best_sector = sector
            
            if best_sector:
                self.current_sector = best_sector
                self.last_sector_change = datetime.now().date()
                logging.info(f"切换到板块: {best_sector}")
                
                # 清空当前持仓
                if self.current_stock:
                    self._sell_current_stock()
                    
        except Exception as e:
            logging.error(f"切换板块失败: {str(e)}")
    
    def _select_best_stock(self):
        """选择板块内最强的股票"""
        try:
            if not self.current_sector:
                return
                
            stocks = self.data_feed.get_sector_stocks(self.current_sector)
            if not stocks:
                return
                
            # 获取所有股票的成交额
            turnovers = self.data_feed.get_batch_turnover(stocks)
            
            # 选择成交额最大的股票
            max_turnover = 0
            best_stock = None
            
            for stock, turnover in turnovers.items():
                if turnover > max_turnover:
                    max_turnover = turnover
                    best_stock = stock
            
            if best_stock:
                self.current_stock = best_stock
                self._buy_stock(best_stock)
                
        except Exception as e:
            logging.error(f"选择股票失败: {str(e)}")
    
    def _buy_stock(self, stock_code: str):
        """买入股票"""
        try:
            # 获取当前价格
            data = self.data_feed.get_market_data(
                stock_list=[stock_code],
                start_date=datetime.now().strftime('%Y%m%d'),
                end_date=datetime.now().strftime('%Y%m%d'),
                period='tick'
            )
            
            if stock_code not in data or data[stock_code].empty:
                logging.error(f"获取股票 {stock_code} 价格失败")
                return
                
            current_price = data[stock_code]['lastPrice'].iloc[-1]
            
            # 计算买入数量
            cash = self.trade_interface.get_cash()
            volume = int(cash / current_price / 100) * 100  # 确保是100的整数倍
            
            if volume > 0:
                if self.trade_interface.buy(stock_code, current_price, volume):
                    logging.info(f"买入股票 {stock_code}: {volume}股")
                else:
                    logging.error(f"买入股票 {stock_code} 失败")
                    
        except Exception as e:
            logging.error(f"买入股票 {stock_code} 失败: {str(e)}")
    
    def _sell_current_stock(self):
        """卖出当前持仓"""
        try:
            if not self.current_stock:
                return
                
            position = self.trade_interface.get_position(self.current_stock)
            if not position:
                return
                
            # 获取当前价格
            data = self.data_feed.get_market_data(
                stock_list=[self.current_stock],
                start_date=datetime.now().strftime('%Y%m%d'),
                end_date=datetime.now().strftime('%Y%m%d'),
                period='tick'
            )
            
            if self.current_stock not in data or data[self.current_stock].empty:
                logging.error(f"获取股票 {self.current_stock} 价格失败")
                return
                
            current_price = data[self.current_stock]['lastPrice'].iloc[-1]
            
            # 卖出持仓
            if self.trade_interface.sell(self.current_stock, current_price, position['volume']):
                logging.info(f"卖出股票 {self.current_stock}: {position['volume']}股")
            else:
                logging.error(f"卖出股票 {self.current_stock} 失败")
                
            self.current_stock = None
            
        except Exception as e:
            logging.error(f"卖出股票 {self.current_stock} 失败: {str(e)}") 