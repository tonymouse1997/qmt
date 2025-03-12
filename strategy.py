import backtrader as bt
from datetime import datetime, time
import pandas as pd
from utils import setup_logger, parse_time, is_trading_time, calculate_percentage_change
from qmt_data_fetcher import *
from data_preprocessor import TickDataPreprocessor

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
        ('big_market_cap', 300e8),  # 权重池市值阈值
        ('weight_gain', 0.03),  # 权重股涨幅阈值
        ('sector_gain', 0.08),  # 板块个股涨幅阈值
        ('avg_amount', 3e8),  # 平均成交额阈值
        ('daily_amount', 1e8),  # 当日成交额阈值
        ('order_amount', 1e8),  # 单笔委托金额
        ('max_sectors', 1000),  # 最大涨停板块数
        ('sell_time', '9:40'),  # 卖出时间
        ('max_positions', 10),  # 最大持仓数
        ('additional_stock_codes', []),  # 自定义添加的股票代码列表
        ('data_dir', 'data/tick'),  # tick数据目录
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
        
        # 初始化数据预处理器
        self.data_preprocessor = TickDataPreprocessor(self.p.data_dir)
        
    def _init_daily_data(self):
        """每日开盘前初始化数据"""
        self.logger.info(f"Initializing data for {self.current_date}")
        
        # 获取基础数据
        self.basic_info_df = get_basic_info_df()
        
        # 构建权重池和小票池
        self.weights_pool = set(
            stock_code for index, row[stock_code] in basic_info_df.iterrows()
            if row[float_amount] >= self.p.big_market_cap
        ).union(set(self.p.additional_stock_codes))
        
        # 构建小票池
        self.small_cap_pool = set(
            stock_code for index, row[stock_code] in basic_info_df.iterrows()
            if (row['float_amount'] < self.p.big_market_cap and  # 流通市值小于300亿
                self._get_average_turnover(stock_code) >= self.p.avg_amount   # 平均成交额大于3亿
                and not stock_code.startswith('68')  # 排除科创板
                and not stock_code.startswith('43')  # 排除北交所
                and not stock_code.startswith('*ST')  # 排除ST股票
                and not stock_code.startswith('ST')
                # and (datetime.now() - row['list_date']).days > 365  # 排除次新股（上市不满1年）
            )        
        )
        
        # 清空其他池
        self.section_pools.clear()
        self.prepare_pool.clear()
        self.limit_up_sections.clear()
        
        # 预加载所有池中股票的tick数据
        all_stocks = self.weights_pool.union(self.small_cap_pool)
        self.data_preprocessor.load_stock_data(all_stocks)
        self.data_preprocessor.update_pools(self.weights_pool, self.small_cap_pool)

    def _update_sector_pools(self, tick_data):
        """更新板块池（策略步骤2实现）
        根据权重池股票涨幅超过3%的情况，建立或更新板块池
        """
        # 记录涨幅超过3%的权重股票及其板块
        triggered_sectors = set()
        
        for stock in self.weights_pool:
            tick_info = tick_data.get(stock, {})
            price_change = tick_info.get('price_change_pct', 0)
            
            # 涨幅超过3%的权重股触发板块池建立
            if price_change >= self.p.weight_gain:
                sector = get_stock_sector(stock)
                triggered_sectors.add(sector)
                if sector not in self.section_pools:
                    self.section_pools[sector] = {
                        'trigger_stocks': set(),  # 触发股票
                        'stocks': set()  # 板块内所有股票
                    }
                self.section_pools[sector]['trigger_stocks'].add(stock)
        
        # 获取触发板块中的所有股票
        for sector in triggered_sectors:
            sector_stocks = get_stock_list_in_sector(sector)
            self.section_pools[sector]['stocks'].update(sector_stocks)
            
            # 预加载新增股票的tick数据
            new_stocks = sector_stocks - set(self.data_preprocessor.stock_data_cache.keys())
            if new_stocks:
                self.data_preprocessor.load_stock_data(new_stocks)

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
            if (tick_info.get('amount', 0) >= self.p.daily_amount or
                tick_info.get('daily_amount', 0) >= self.p.daily_amount):
                final_pool.add(stock)
        self.prepare_pool = final_pool

    def _execute_orders(self, tick_data):
        """执行交易（策略步骤5实现）"""
        if len(self.limit_up_sections) >= self.p.max_sectors:
            return
            
        for stock in self.prepare_pool:
            # 获取涨停价
            tick_info = tick_data.get(stock, {})
            if not tick_info:
                continue
                
            limit_up_price = tick_info.get('limit_up', 0)
            # 当前价达到涨停价且还有仓位空间
            if (tick_info.get('last', 0) >= limit_up_price and 
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

    def _process_tick_data(self, tick_data):
        """处理tick数据的主函数"""
        # 更新板块池
        self._update_sector_pools(tick_data)
        
        # 更新待打池
        self._update_prepare_pool(tick_data)
        
        # 执行交易
        self._execute_orders(tick_data)

    def next(self):
        current_time = self.data.datetime.time()
        current_timestamp = self.data.datetime.datetime()
        
        if self.current_date != self.data.datetime.date():
            self._init_daily_data()
            self.current_date = self.data.datetime.date()
            
        if is_trading_time(current_time):
            # 使用预处理器获取当前时间点的所有股票tick数据
            tick_data = self.data_preprocessor.get_current_tick_data(current_timestamp)
            self._process_tick_data(tick_data)
            
        if current_time >= parse_time(self.p.sell_time):
            self._sell_positions()


