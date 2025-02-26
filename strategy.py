import backtrader as bt
from datetime import datetime, time
import pandas as pd
from utils import setup_logger, parse_time, is_trading_time, calculate_percentage_change
from config import Config

class SectorChaseStrategy(bt.Strategy):
    """涨停板板块联动策略
    策略逻辑实现：
    1. 盘前：
       - 筛选流通市值≥300亿的股票构成权重池（支持自定义添加）
       - 筛选流通市值＜300亿且平均成交额≥3亿的股票构成小票池
    2. 盘中：
       - 监控权重池个股涨幅，触发板块池建立
       - 追踪板块池个股异动，筛选符合条件的小票
       - 执行涨停板买入和次日定时卖出
    3. 风控：
       - 控制最大持仓数量
       - 限制当日涨停板块数量
    """
    params = (
        ('big_market_cap', Config.BIG_MARKET_CAP),  # 权重池市值阈值
        ('weight_gain', Config.WEIGHT_GAIN_THRESHOLD),  # 权重股涨幅阈值
        ('sector_gain', Config.SECTOR_GAIN_THRESHOLD),  # 板块个股涨幅阈值
        ('tick_amount', Config.TICK_AMOUNT_THRESHOLD),  # 单tick成交额阈值
        ('daily_amount', Config.DAILY_AMOUNT_THRESHOLD),  # 当日成交额阈值
        ('order_amount', Config.ORDER_AMOUNT),  # 单笔委托金额
        ('max_sectors', Config.MAX_SECTORS),  # 最大涨停板块数
        ('sell_time', Config.SELL_TIME),  # 卖出时间
        ('max_positions', Config.MAX_POSITIONS)  # 最大持仓数
    )

    def __init__(self):
        self.logger = setup_logger('SectorChaseStrategy')
        self.weights_pool = set()  # 权重池
        self.small_cap_pool = set()  # 小票池
        self.section_pools = {}  # 板块池
        self.prepare_pool = set()  # 准备下单池
        self.limit_up_sections = set()  # 已涨停板块
        self.current_date = None
        self.positions = {}  # 持仓记录

    def _init_daily_data(self):
        """每日开盘前初始化数据"""
        self.logger.info(f"Initializing data for {self.current_date}")
        
        # 获取流通市值数据
        market_cap_data = get_market_cap_data()
        
        # 构建权重池和小票池
        self.weights_pool = set(
            stock for stock, cap in market_cap_data.items()
            if cap >= self.p.big_market_cap
        ).union(set(Config.ADDITIONAL_STOCKS))
        
        self.small_cap_pool = set(
            stock for stock, cap in market_cap_data.items()
            if cap < self.p.big_market_cap and 
            self._get_average_turnover(stock) >= Config.SMALL_AMOUNT_THRESHOLD
        )
        
        # 清空其他池
        self.section_pools.clear()
        self.prepare_pool.clear()
        self.limit_up_sections.clear()

    def _update_prepare_pool(self, tick_data):
        """更新待打池（策略步骤3+4实现）"""
        # 步骤3：筛选符合条件的小票
        for sector, pool in self.section_pools.items():
            for stock in pool['stocks']:
                # 仅处理小票池中的股票
                if stock in self.small_cap_pool:
                    tick_info = tick_data.get(stock, {})
                    price_change = tick_info.get('price_change_pct', 0)
                    
                    # 涨幅超过8%且属于小票池
                    if price_change >= self.p.sector_gain:
                        self.prepare_pool.add(stock)
        
        # 步骤4：成交额筛选
        final_pool = set()
        for stock in self.prepare_pool:
            tick_info = tick_data.get(stock, {})
            # 单tick成交额超过阈值 或 当日累计成交额超过阈值
            if (tick_info.get('amount', 0) >= self.p.tick_amount or
                tick_info.get('daily_amount', 0) >= self.p.daily_amount):
                final_pool.add(stock)
        self.prepare_pool = final_pool

    def _execute_orders(self):
        """执行交易（策略步骤5实现）"""
        if len(self.limit_up_sections) >= self.p.max_sectors:
            return
            
        for stock in self.prepare_pool:
            # 获取涨停价
            limit_up_price = tick_data[stock]['limit_up']
            # 当前价达到涨停价且还有仓位空间
            if (tick_data[stock]['last'] >= limit_up_price and 
                len(self.positions) < self.p.max_positions):
                
                # 计算可买数量
                size = self.p.order_amount // limit_up_price
                self.buy(data=stock, price=limit_up_price, size=size)
                
                # 记录板块信息
                sector = get_stock_sector(stock)
                self.limit_up_sections.add(sector)

    def _sell_positions(self):
        """次日卖出（策略步骤7实现）"""
        for stock in list(self.positions.keys()):
            if self.getposition(stock).size > 0:
                self.close(data=stock)

    def next(self):
        current_time = self.data.datetime.time()
        if self.current_date != self.data.datetime.date():
            self._init_daily_data()
            self.current_date = self.data.datetime.date()
            
        if is_trading_time(current_time):
            tick_data = self._get_current_tick_data()
            self._process_tick_data(tick_data)
            
        if current_time >= parse_time(self.p.sell_time):
            self._sell_positions()

# 需要实现的辅助函数（需对接QMT接口）
def get_market_cap_data():
    """获取全市场流通市值数据"""
    pass

def get_stock_sector(stock):
    """通过QMT获取股票所属板块"""
    pass

def get_sector_stocks(sector):
    """通过QMT获取板块成分股"""
    pass
