from datetime import datetime, time
import pandas as pd
from utils import setup_logger, parse_time, is_trading_time, calculate_percentage_change
from qmt_data_fetcher import *
import datetime

class SectorChaseStrategy():
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
    def __init__(self, mode='backtest', big_market_cap=300e8, weight_gain=0.03, sector_gain=0.08, avg_amount=3e8, tick_amount=2000e4, daily_amount=1e8, order_amount=100000, max_sectors=3, sell_time='09:40:00', max_positions=10, max_allowed_in_sector=2, additional_stock_codes=[]):
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
        self.max_allowed_in_sector = max_allowed_in_sector
        self.mode = mode

        self.weights_pool = set()  # 权重池
        self.small_cap_pool = set()  # 小票池
        self.sector_pools = set()  # 板块池
        self.prepare_pool = set()  # 准备下单池
        self.limit_up_sections = set()  # 已涨停板块
        self.current_date = None
        self.triggered_weights = set()  # 记录已经触发过的权重股票

        self._init_daily_data()
        self.time = self.datas[0].datetime
        self.sectors_of_stocks = get_sectors_of_stocks(instrument_type='stock')

    def _init_daily_data(self):
        """每日开盘前初始化数据"""
        self.logger.info(f"Initializing data for {self.current_date}")

        # 获取基础数据
        self.basic_info_df = get_basic_info_df()

        # 构建权重池和小票池
        self.weights_pool = set(
            stock_code for index, row in basic_info_df.iterrows()
            if row['float_amount'] >= self.p.big_market_cap
        ).union(set(self.p.additional_stock_codes))

        # 构建小票池
        self.small_cap_pool = set(
            stock_code for index, row in basic_info_df.iterrows()
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
        self.sector_pools.clear()
        self.prepare_pool.clear()
        self.limit_up_sections.clear()
    
    def _update_sector_pools(self, datas):
        """更新板块池（策略步骤2实现）
        根据权重池股票涨幅超过3%的情况，建立或更新板块池
        """
        # 记录涨幅超过3%的权重股票及其板块
        for stock_code in self.weights_pool:
            if stock_code in triggered_weights:
                continue
            stock_data = datas[stock_code]
            change_rate = calculate_change_rate(stock_data['current_price'], stock_data['preclose'])

            if change_rate >= self.weight_gain:
                sectors = self.sectors_of_stocks[stock_code]  # 使用股票代码
                if sector not in self.sector_pools:
                    self.sector_pools[sector] = {'stocks': xtdata.get_stock_list_in_sector(sector),
                                                'trigger_stocks': stock_code,
                                                'limit_ups': None,
                                                'eliminated': None}
                else:
                    self.sector_pools[sector]['trigger_stocks'].add(stock_code)
                triggered_weights.add(stock_code)
        # 更新涨停数量
        for sector in self.sector_pools:
            sector['limit_ups'] = len([stock_code for stock_code in sector['stocks'] if self._is_limit_up(stock_code)])
            sector['eliminated'] = (sector['limit_ups'] >= self.max_allowed_in_sector) if sector['eliminated'] != 'bought' else 'bought'
    def _update_prepare_pool(self, datas):
        """更新待打池（策略步骤3+4实现）"""
        # 步骤3：筛选符合条件的小票
        for sector, pool in self.sector_pools.items():
            if self.sector_pools[sector]['eliminated']:
                continue
            for stock_code in pool['stocks']:
                # 仅处理小票池中的股票
                if stock_code in self.small_cap_pool:
                    stock_data = datas[stock_code]
                    change_rate = calculate_change_rate(stock_data['current_price'], stock_data['preclose'])

                    # 涨幅超过8%且属于小票池
                    if change_rate >= self.sector_gain:
                        self.prepare_pool.add(stock_code)

    def _is_limit_up(self, stock_code):
        return datas[stock_code]['lastPrice'] == self.basic_info_df[stock_code]['limit_up_price']
        # 步骤4：成交额筛选
    def _update_final_pool(self, datas):
        """根据成交额筛选股票"""
        final_pool = set()
        for stock_code in self.prepare_pool:
            if self._is_limit_up(stock_code) and self._get_average_turnover(stock_code) >= self.avg_amount:
                self._buy_stock(stock_code)

    def _sell_stock_logic(self, ):
        ## 到9:40卖出所有股票
        if self.time.time() >= parse_time(self.sell_time):
            for stock_code in self.portfolio.positions:
                self._sell_stock(stock_code)
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

        if self.mode == 'real_trade':
            pass
        elif self.mode == 'mock_trade':
            pass
        else:
            pass

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

        if self.mode == 'real_trade':
            pass
        elif self.mode == 'mock_trade':
            pass
        else:
            pass