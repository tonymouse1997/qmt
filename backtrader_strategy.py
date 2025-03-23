import backtrader as bt
from datetime import datetime
from qmt_data_fetcher import get_basic_info_df, get_sectors_of_stocks
from utils import *
from factor_library import *
from xtquant import xtdata

class SectorChaseStrategy(bt.Strategy):
    params = (
        ('big_market_cap', 300e8),
        ('weight_gain', 0.03),
        ('sector_gain', 0.08),
        ('avg_amount', 3e8),
        ('tick_amount', 2000e4),
        ('daily_amount', 1e8),
        ('order_amount', 100000),
        ('max_sectors', 3),
        ('sell_time', '09:40:00'),
        ('max_positions', 10),
        ('max_allowed_in_sector', 2),
        ('additional_stock_codes', []),
        ('mode', 'backtest')
    )

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close
        self.order = None
        self.weights_pool = set()  # 权重池
        self.small_cap_pool = set()  # 小票池
        self.sector_pools = {}  # 板块池
        self.prepare_pool = set()  # 准备下单池
        self.limit_up_sections = set()  # 已涨停板块
        self.triggered_weights = set()  # 记录已经触发过的权重股票
        self.sectors_of_stocks = get_sectors_of_stocks(instrument_type='stock')
        self.basic_info_df = get_basic_info_df()

        # init weights_pool and small_cap_pool
        self._init_daily_data()

    def _init_daily_data(self):
        """每日开盘前初始化数据"""
        # 获取基础数据

        # 构建权重池和小票池
        self.weights_pool = {stock_code for stock_code in [index for index, row in self.basic_info_df.iterrows()] if get_float_amount(stock_code) >= self.p.big_market_cap}.union(set(self.p.additional_stock_codes))

        # 构建小票池
        self.small_cap_pool = set(
            stock_code for index, row in self.basic_info_df.iterrows()
            if (get_float_amount(stock_code) < self.p.big_market_cap and  # 流通市值小于300亿
                self._get_average_turnover(stock_code) >= self.p.avg_amount   # 平均成交额大于3亿
                and not is_kcb(stock_code)  # 排除科创板
                and not is_bj(stock_code)  # 排除北交所
                and not is_st(stock_code) # 排除ST股票
                # and (datetime.now() - row['list_date']).days > 365  # 排除次新股（上市不满1年）
            )
        )

        # 清空其他池
        self.sector_pools.clear()
        self.prepare_pool.clear()
        self.limit_up_sections.clear()
        self.triggered_weights.clear()

    def _get_average_turnover(self, stock_code):
        #TODO: 实现获取平均成交额的函数
        return 3e8



    def _update_sector_pools(self):
        """更新板块池（策略步骤2实现）
        根据权重池股票涨幅超过3%的情况，建立或更新板块池
        """
        # 记录涨幅超过3%的权重股票及其板块
        for stock_code in self.weights_pool:
            if stock_code in self.triggered_weights:
                continue
            #stock_data = datas[stock_code]
            #change_rate = calculate_change_rate(stock_data['current_price'], stock_data['preclose'])
            change_rate = 0.05 #TODO: 需要获取当前价格和昨收价

            if change_rate >= self.p.weight_gain:
                sectors = self.sectors_of_stocks[stock_code]  # 使用股票代码
                for sector in sectors:
                    if sector not in self.sector_pools:
                        #self.sector_pools[sector] = {'stocks': xtdata.get_stock_list_in_sector(sector),
                        self.sector_pools[sector] = {'stocks': ['600000.SH', '600004.SH'], #TODO: 需要获取板块股票列表
                                                    'trigger_stocks': [stock_code],
                                                    'limit_ups': 0,
                                                    'eliminated': False}
                    else:
                        if 'trigger_stocks' in self.sector_pools[sector]:
                            self.sector_pools[sector]['trigger_stocks'].append(stock_code)
                        else:
                            self.sector_pools[sector]['trigger_stocks'] = [stock_code]
                self.triggered_weights.add(stock_code)
        # 更新涨停数量
        for sector in self.sector_pools:
            limit_ups = 0
            if 'stocks' in self.sector_pools[sector]:
                for stock_code in self.sector_pools[sector]['stocks']:
                    if self._is_limit_up(stock_code):
                        limit_ups += 1
            self.sector_pools[sector]['limit_ups'] = limit_ups
            if 'eliminated' in self.sector_pools[sector]:
                self.sector_pools[sector]['eliminated'] = (limit_ups >= self.p.max_allowed_in_sector) if self.sector_pools[sector]['eliminated'] != 'bought' else 'bought'
            else:
                self.sector_pools[sector]['eliminated'] = (limit_ups >= self.p.max_allowed_in_sector)

    def _update_prepare_pool(self):
        """更新待打池（策略步骤3+4实现）"""
        # 步骤3：筛选符合条件的小票
        for sector, pool in self.sector_pools.items():
            if 'eliminated' in self.sector_pools[sector] and self.sector_pools[sector]['eliminated']:
                continue
            if 'stocks' in self.sector_pools[sector]:
                for stock_code in self.sector_pools[sector]['stocks']:
                    # 仅处理小票池中的股票
                    if stock_code in self.small_cap_pool:
                        #stock_data = datas[stock_code]
                        #change_rate = calculate_change_rate(stock_data['current_price'], stock_data['preclose'])
                        change_rate = 0.09 #TODO: 需要获取当前价格和昨收价

                        # 涨幅超过8%且属于小票池
                        if change_rate >= self.p.sector_gain:
                            self.prepare_pool.add(stock_code)

    def next(self):
        # Simply log the closing price of the series from the reference
        self.log('Close, %.2f' % self.dataclose[0])

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # _sell_stock_logic
        if self.datetime.time() >= parse_time(self.p.sell_time):
            for stock_code in self.position:
                self._sell_stock(stock_code)

        # _update_sector_pools
        self._update_sector_pools()

        # _update_prepare_pool
        self._update_prepare_pool()

        # _update_final_pool
        for stock_code in self.prepare_pool:
            if self._is_limit_up(stock_code) and self._get_average_turnover(stock_code) >= self.p.avg_amount:
                self._buy_stock(stock_code)

    def _buy_stock(self, stock_code, amount=None, proportion=None, order_type='limit'):
        """
        执行交易订单
        :param stock_code: 股票代码
        :param amount: 交易金额，与proportion二选一
        :param proportion: 交易比例，与amount二选一
        """
        if amount is not None and proportion is not None:
            raise ValueError("amount和proportion只能设置其中一个参数")
        if amount is None and proportion is None:
            raise ValueError("amount和proportion必须设置其中一个参数")
        """执行交易（策略步骤5实现）"""
        #TODO: 需要实现买入逻辑
        self.log('Buy Stock, %s' % stock_code)
        self.order = self.buy(data=stock_code, size=100)

    def _sell_stock(self, stock_code, amount=None, proportion=1, order_type='limit'):
        """
        执行交易订单
        :param stock_code: 股票代码
        :param amount: 交易金额，与proportion二选一
        :param proportion: 交易比例，与amount二选一
        """
        if amount is not None and proportion is not None:
            raise ValueError("amount和proportion只能设置其中一个参数")
        if amount is None and proportion is None:
            raise ValueError("amount和proportion必须设置其中一个参数")
        """执行交易（策略步骤5实现）"""
        #TODO: 需要实现卖出逻辑
        self.log('Sell Stock, %s' % stock_code)
        self.order = self.sell(data=stock_code, size=100)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, %.2f' % order.executed.price
                )

            elif order.issell():
                self.log(
                    'SELL EXECUTED, %.2f' % order.executed.price
                )

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Add a strategy
    cerebro.addstrategy(SectorChaseStrategy)

    # 获取所有股票的行情数据
    stock_list = xtdata.get_stock_list_in_sector('沪深A股')  # 示例股票代码列表
    start_date = '20250101'
    end_date = '20250228'

    stocks_data = xtdata.get_market_data_ex(
        stock_list=stock_list,
        start_time=start_date,
        end_time=end_date,
        period='tick',
        field_list=['lastPrice', 'volume', 'amount']
    )
    print(stocks_data)
    for stock_code, df in stocks_data:

        # 通过xtdata获取每只股票的历史数据        
        # 创建每只股票的数据源
        data = bt.feeds.PandasData(
            dataname=df,
            datetime=0,
            open=-1,
            high=-1,
            low=-1,
            close=1,
            volume=2,
            openinterest=-1
        )
        
        # 将每只股票的数据添加到cerebro中
        cerebro.adddata(data, name=stock_code)

    # Set our desired cash start
    cerebro.broker.setcash(100000.0)

    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Run over everything
    cerebro.run()

    # Print out the final result
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Plot the result
    cerebro.plot()
