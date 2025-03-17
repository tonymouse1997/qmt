from datetime import datetime, time
import pandas as pd
from utils import setup_logger, parse_time, is_trading_time, calculate_percentage_change
from qmt_data_fetcher import *
import backtrader as bt
import datetime

def timestamp_to_datetime(timestamp):
    """将毫秒时间戳转换为 datetime 对象"""
    timestamp = int(timestamp)
    return datetime.datetime.fromtimestamp(timestamp / 1000)

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

    def __init__(self, big_market_cap=300e8, weight_gain=0.03, sector_gain=0.08, avg_amount=3e8, tick_amount=2000e4, daily_amount=1e8, order_amount=100000, max_sectors=3, sell_time='09:40:00', max_positions=10, additional_stock_codes=[]):
        self.logger = setup_logger('SectorChaseStrategy')
        self.big_market_cap = big_market_cap
        self.weight_gain = weight_gain
        self.sector_gain = sector_gain
        self.avg_amount = avg_amount
        self.tick_amount = tick_amount
        self.daily_amount = daily_amount
        self.order_amount = order_amount
        self.max_sectors = max_sectors
        self.sell_time = sell_time
        self.max_positions = max_positions
        self.additional_stock_codes = additional_stock_codes
        self.weights_pool = set()  # 权重池
        self.small_cap_pool = set()  # 小票池
        self.section_pools = {}  # 板块池
        self.prepare_pool = set()  # 准备下单池
        self.limit_up_sections = set()  # 已涨停板块
        self.current_date = None

        self._init_daily_data()
        self.time = self.datas[0].datetime
        
    def _init_daily_data(self):
        """每日开盘前初始化数据"""
        self.logger.info(f"Initializing data for {self.current_date}")
        
        # 获取基础数据
        basic_info_df = get_basic_info_df()
        
        # 构建权重池和小票池
        self.weights_pool = set(
            stock_code for index, row in basic_info_df.iterrows()
            if row['float_amount'] >= self.big_market_cap
        ).union(set(self.additional_stock_codes))
        
        # 构建小票池
        self.small_cap_pool = set(
            stock_code for index, row in basic_info_df.iterrows()
            if (row['float_amount'] < self.big_market_cap and  # 流通市值小于300亿
                self._get_average_turnover(stock_code) >= self.avg_amount   # 平均成交额大于3亿
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

    def _get_average_turnover(self, stock_code):
        """获取股票的平均成交额"""
        # 这里需要根据实际情况实现获取历史数据并计算平均成交额的逻辑
        # 例如，可以从CSV文件或数据库中读取历史成交额数据
        # 这里为了简化示例，暂时返回一个固定的值
        return 4e8  # 假设平均成交额为4亿

    def _update_sector_pools(self):
        """更新板块池（策略步骤2实现）
        根据权重池股票涨幅超过3%的情况，建立或更新板块池
        """
        # 记录涨幅超过3%的权重股票及其板块
        for stock in self.datas:
            tick_info = get_tick(stock.p.dataname)
            if tick_info is None:
                continue
            price_change = (tick_info['last'] - tick_info['preclose']) / tick_info['preclose']

            # 检查是否为权重股
            if stock.p.dataname in self.weights_pool:
                # 涨幅超过3%的权重股触发板块池建立
                if price_change >= self.weight_gain:
                    sector = get_stock_sector(stock.p.dataname)  # 使用股票代码
                    if sector:  # 确保获取到板块信息
                        if sector not in self.section_pools:
                            self.section_pools[sector] = {
                                'trigger_stocks': set(),  # 触发股票
                                'stocks': set()  # 板块内所有股票
                            }
                        self.section_pools[sector]['trigger_stocks'].add(stock.p.dataname)
                        # 获取并更新板块内所有股票
                        sector_stocks = get_stock_list_in_sector(sector)
                        if sector_stocks:
                            self.section_pools[sector]['stocks'].update(sector_stocks)


    def _update_prepare_pool(self, tick_data):
        """更新待打池（策略步骤3+4实现）"""
        # 步骤3：筛选符合条件的小票
        for sector, pool in self.section_pools.items():
            for stock in pool['stocks']:
                # 仅处理小票池中的股票
                if stock in self.small_cap_pool:
                    tick_info = get_tick(stock)
                    if tick_info is None:
                        continue
                    price_change = (tick_info['last'] - tick_info['preclose']) / tick_info['preclose']

                    # 涨幅超过8%且属于小票池
                    if price_change >= self.sector_gain:
                        self.prepare_pool.add(stock)

        # 步骤4：成交额筛选
        final_pool = set()
        for stock in self.prepare_pool:
            tick_info = get_tick(stock)
            if tick_info is None:
                continue
            # 单tick成交额超过阈值 或 当日累计成交额超过阈值
            if (tick_info['money'] >= self.tick_amount or
                tick_info['amount'] >= self.daily_amount):
                final_pool.add(stock)
        self.prepare_pool = final_pool

    def _execute_orders(self):
        """执行交易（策略步骤5实现）"""
        if len(self.limit_up_sections) >= self.max_sectors:
            return

        for stock in self.prepare_pool:
            tick_info = get_tick(stock)
            if tick_info is None:
                continue
            # 获取涨停价
            limit_up_price = tick_info['high_limit']
            # 当前价达到涨停价且还有仓位空间
            if (tick_info['last'] >= limit_up_price and
                len(self.broker.get_positions()) < self.max_positions):

                # 计算可买数量,涨停价向下取整到分
                buy_price = int(limit_up_price*100)/100
                size = int(self.order_amount // buy_price)

                self.buy(data=self.datas[0], price=buy_price, size=size)

                # 记录板块信息,涨停
                sector = get_stock_sector(stock)
                self.limit_up_sections.add(sector)
                self.logger.info(f"Buy {stock} at {limit_up_price}, size {size}, sector {sector}")

    def _sell_positions(self):
        """次日卖出（策略步骤7实现）"""
        for stock in list(self.positions.keys()):
            self.close(data=self.datas[0]) #市价卖出
            self.logger.info(f"Sell {stock} at market price")

    def next(self):
        print(self.time)
        current_time = self.datas[0].datetime.time()
        current_date = self.datas[0].datetime.date()
        print(current_date, current_time)
        if self.current_date != current_date:
            self._init_daily_data()
            self.current_date = current_date
        print(f"current_time: {current_time}, is_trading_time: {is_trading_time(current_time)}")

        if is_trading_time(current_time):
            self._update_sector_pools()
            #tick_data = {data.p.dataname: get_tick(data.p.dataname) for data in self.datas}  # 获取所有股票的tick数据
            self._update_prepare_pool()
            self._execute_orders()

        if current_time >= parse_time(self.sell_time):
            self._sell_positions()


if __name__ == '__main__':
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    
    # 添加策略
    cerebro.addstrategy(SectorChaseStrategy(
        big_market_cap=300e8,
        weight_gain=0.03,
        sector_gain=0.08,
        avg_amount=3e8,
        tick_amount=2000e4,
        daily_amount=1e8,
        order_amount=100000,
        max_sectors=3,
        sell_time='09:40:00',
        max_positions=10,
        additional_stock_codes=[]
    ))

    # 添加数据源
    stock_list = ['600010.SH']  # 股票列表
    for stock_code in stock_list:
        data = bt.feeds.GenericCSVData(
            dataname=rf'a_share_data\tick\{stock_code}.csv',
            datetime=0,  # 时间列索引为0
            dtformat=timestamp_to_datetime,  # 时间格式为时间戳
            open=2,
            high=3,
            low=4,
            close=5,
            volume=7,
            openinterest=-1
        )
        cerebro.adddata(data)
    
    # 设置初始资金
    cerebro.broker.setcash(1000000.0)
    
    # 设置手续费
    cerebro.broker.setcommission(commission=0.0003)
    
    # 运行回测
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    cerebro.run()

    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    
    # 绘制结果
    #cerebro.plot()

def on_data(datas):
    for stock_code in datas:
        print(stock_code, datas[stock_code])

class SubscribeWholeQuote:
    def __init__(self):
        self.subscribe_id = subscribe_whole_quote(['SH', 'SZ'], callback=on_data)

if __name__ == '__main__':
    SubscribeWholeQuote()
