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
        self.current_date = None

        # init weights_pool and small_cap_pool
        self._init_daily_data()

    def _init_daily_data(self):
        """每日开盘前初始化数据"""
        try:
            # 获取基础数据
            self.basic_info_df = get_basic_info_df()
            if self.basic_info_df.empty:
                self.logger.error("获取基础数据失败，跳过今日交易")
                return False

            # 构建权重池和小票池
            try:
                self.weights_pool = {stock_code for stock_code in [index for index, row in self.basic_info_df.iterrows()] 
                                   if get_float_amount(stock_code) >= self.p.big_market_cap}.union(set(self.p.additional_stock_codes))
                
                if not self.weights_pool:
                    self.logger.warning("权重池为空，请检查筛选条件")
                    return False
            except Exception as e:
                self.logger.error(f"构建权重池时发生错误: {str(e)}")
                return False

            # 构建小票池
            try:
                self.small_cap_pool = set(
                    stock_code for index, row in self.basic_info_df.iterrows()
                    if (get_float_amount(stock_code) < self.p.big_market_cap and  # 流通市值小于300亿
                        self._get_average_turnover(stock_code) >= self.p.avg_amount   # 平均成交额大于3亿
                        and not is_kcb(stock_code)  # 排除科创板
                        and not is_bj(stock_code)  # 排除北交所
                        and not is_st(stock_code) # 排除ST股票
                    )
                )
                
                if not self.small_cap_pool:
                    self.logger.warning("小票池为空，请检查筛选条件")
                    return False
            except Exception as e:
                self.logger.error(f"构建小票池时发生错误: {str(e)}")
                return False

            # 清空其他池
            self.sector_pools.clear()
            self.prepare_pool.clear()
            self.limit_up_sections.clear()
            self.triggered_weights.clear()
            
            self.logger.info(f"数据初始化完成: 权重池 {len(self.weights_pool)} 只股票, 小票池 {len(self.small_cap_pool)} 只股票")
            return True
            
        except Exception as e:
            self.logger.error(f"数据初始化过程中发生错误: {str(e)}")
            return False

    def _get_average_turnover(self, stock_code):
        #TODO: 实现获取平均成交额的函数
        return 3e8

    def _update_sector_pools(self):
        """更新板块池（策略步骤2实现）
        根据权重池股票涨幅超过3%的情况，建立或更新板块池
        """
        try:
            # 获取当前所有股票的tick数据
            tick_data = {}
            for stock_code in self.weights_pool:
                try:
                    tick = xtdata.get_full_tick([stock_code])
                    if stock_code in tick:
                        tick_data[stock_code] = tick[stock_code]
                except Exception as e:
                    self.logger.error(f"获取股票 {stock_code} 的tick数据失败: {str(e)}")
                    continue

            # 记录涨幅超过3%的权重股票及其板块
            for stock_code in self.weights_pool:
                if stock_code in self.triggered_weights:
                    continue
                    
                if stock_code not in tick_data:
                    continue
                    
                try:
                    # 计算涨幅
                    current_price = tick_data[stock_code]['lastPrice']
                    preclose = tick_data[stock_code]['preClose']
                    change_rate = (current_price - preclose) / preclose

                    if change_rate >= self.p.weight_gain:
                        # 获取股票所属板块
                        if stock_code not in self.sectors_of_stocks:
                            self.logger.warning(f"股票 {stock_code} 没有板块信息")
                            continue
                            
                        sectors = self.sectors_of_stocks[stock_code]
                        if not sectors:
                            self.logger.warning(f"股票 {stock_code} 的板块列表为空")
                            continue

                        # 更新板块池
                        for sector in sectors:
                            try:
                                # 获取板块内所有股票
                                sector_stocks = xtdata.get_stock_list_in_sector(sector)
                                if not sector_stocks:
                                    self.logger.warning(f"板块 {sector} 没有股票")
                                    continue

                                if sector not in self.sector_pools:
                                    self.sector_pools[sector] = {
                                        'stocks': sector_stocks,
                                        'trigger_stocks': [stock_code],
                                        'limit_ups': 0,
                                        'eliminated': False,
                                        'last_update': datetime.now()
                                    }
                                else:
                                    # 更新触发股票列表
                                    if stock_code not in self.sector_pools[sector]['trigger_stocks']:
                                        self.sector_pools[sector]['trigger_stocks'].append(stock_code)
                                    
                                    # 更新股票列表（以防板块成分股发生变化）
                                    self.sector_pools[sector]['stocks'] = sector_stocks
                                    self.sector_pools[sector]['last_update'] = datetime.now()

                                self.logger.info(f"板块 {sector} 被触发，触发股票: {stock_code}, 涨幅: {change_rate:.2%}")
                                
                            except Exception as e:
                                self.logger.error(f"处理板块 {sector} 时发生错误: {str(e)}")
                                continue

                        self.triggered_weights.add(stock_code)
                        
                except Exception as e:
                    self.logger.error(f"处理股票 {stock_code} 时发生错误: {str(e)}")
                    continue

            # 更新涨停数量
            for sector in list(self.sector_pools.keys()):
                try:
                    limit_ups = 0
                    if 'stocks' in self.sector_pools[sector]:
                        for stock_code in self.sector_pools[sector]['stocks']:
                            if stock_code in tick_data:
                                if self._is_limit_up(stock_code, tick_data[stock_code]['lastPrice']):
                                    limit_ups += 1
                    
                    self.sector_pools[sector]['limit_ups'] = limit_ups
                    
                    # 更新板块状态
                    if self.sector_pools[sector]['eliminated'] != 'bought':
                        self.sector_pools[sector]['eliminated'] = (limit_ups >= self.p.max_allowed_in_sector)
                        
                    if limit_ups >= self.p.max_allowed_in_sector:
                        self.logger.info(f"板块 {sector} 涨停股票数量达到上限: {limit_ups}")
                        
                except Exception as e:
                    self.logger.error(f"更新板块 {sector} 涨停数量时发生错误: {str(e)}")
                    continue

        except Exception as e:
            self.logger.error(f"更新板块池时发生错误: {str(e)}")
            # 发生错误时，清空板块池
            self.sector_pools.clear()

    def _update_prepare_pool(self):
        """更新待打池（策略步骤3+4实现）
        步骤3：筛选符合条件的小票
        步骤4：根据成交额筛选
        """
        try:
            # 获取所有相关股票的tick数据
            tick_data = {}
            all_stocks = set()
            for sector, pool in self.sector_pools.items():
                if pool.get('eliminated', True):
                    continue
                all_stocks.update(pool.get('stocks', []))
            
            if not all_stocks:
                return
                
            try:
                tick_data = xtdata.get_full_tick(list(all_stocks))
            except Exception as e:
                self.logger.error(f"获取tick数据失败: {str(e)}")
                return

            # 步骤3：筛选符合条件的小票
            for sector, pool in self.sector_pools.items():
                if pool.get('eliminated', True):
                    continue
                    
                for stock_code in pool.get('stocks', []):
                    # 仅处理小票池中的股票
                    if stock_code not in self.small_cap_pool:
                        continue
                        
                    if stock_code not in tick_data:
                        continue
                        
                    try:
                        # 计算涨幅
                        current_price = tick_data[stock_code]['lastPrice']
                        preclose = tick_data[stock_code]['preClose']
                        change_rate = (current_price - preclose) / preclose

                        # 涨幅超过8%且属于小票池
                        if change_rate >= self.p.sector_gain:
                            self.prepare_pool.add(stock_code)
                            self.logger.info(f"股票 {stock_code} 进入待打池，涨幅: {change_rate:.2%}")
                    except Exception as e:
                        self.logger.error(f"处理股票 {stock_code} 时发生错误: {str(e)}")
                        continue

            # 步骤4：成交额筛选
            final_pool = set()
            for stock_code in self.prepare_pool:
                if stock_code not in tick_data:
                    continue
                    
                try:
                    tick_info = tick_data[stock_code]
                    # 单tick成交额
                    tick_amount = tick_info['amount']
                    # 当日累计成交额
                    daily_amount = tick_info['amount']
                    
                    # 检查成交额条件
                    if (tick_amount >= self.p.tick_amount or 
                        daily_amount >= self.p.daily_amount):
                        
                        # 检查是否涨停
                        if self._is_limit_up(stock_code, tick_info['lastPrice']):
                            final_pool.add(stock_code)
                            self.logger.info(f"股票 {stock_code} 进入准备下单池，tick成交额: {tick_amount:.2f}万, 日成交额: {daily_amount:.2f}万")
                            
                except Exception as e:
                    self.logger.error(f"处理股票 {stock_code} 成交额时发生错误: {str(e)}")
                    continue

            # 更新准备下单池
            self.prepare_pool = final_pool
            
            if final_pool:
                self.logger.info(f"准备下单池更新完成，共 {len(final_pool)} 只股票")
                
        except Exception as e:
            self.logger.error(f"更新待打池时发生错误: {str(e)}")
            # 发生错误时，清空准备下单池
            self.prepare_pool.clear()

    def next(self):
        current_time = self.datas[0].datetime.time()
        current_date = self.datas[0].datetime.date()
        
        # 检查是否需要初始化数据
        if self.current_date != current_date:
            if not self._init_daily_data():
                self.logger.error("数据初始化失败，跳过今日交易")
                return
            self.current_date = current_date
            
        # 检查是否在交易时间内
        if not is_trading_time(current_time):
            return
            
        try:
            # 更新板块池
            self._update_sector_pools()
            
            # 更新待打池
            self._update_prepare_pool()
            
            # 执行交易
            self._execute_orders()
            
            # 检查是否需要卖出
            if current_time >= parse_time(self.p.sell_time):
                self._sell_positions()
                
        except Exception as e:
            self.logger.error(f"策略执行过程中发生错误: {str(e)}")
            # 发生错误时，清空所有持仓
            self._sell_positions()

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

    def _execute_orders(self):
        """执行交易（策略步骤5实现）
        1. 检查是否达到最大板块数量限制
        2. 检查是否达到最大持仓数量限制
        3. 执行买入订单
        """
        try:
            # 检查是否达到最大板块数量限制
            if len(self.limit_up_sections) >= self.p.max_sectors:
                self.logger.info(f"已达到最大板块数量限制: {self.p.max_sectors}")
                return

            # 检查是否达到最大持仓数量限制
            current_positions = len(self.broker.get_positions())
            if current_positions >= self.p.max_positions:
                self.logger.info(f"已达到最大持仓数量限制: {self.p.max_positions}")
                return

            # 获取所有相关股票的tick数据
            if not self.prepare_pool:
                return
                
            try:
                tick_data = xtdata.get_full_tick(list(self.prepare_pool))
            except Exception as e:
                self.logger.error(f"获取tick数据失败: {str(e)}")
                return

            # 执行买入订单
            for stock_code in self.prepare_pool:
                if stock_code not in tick_data:
                    continue
                    
                try:
                    tick_info = tick_data[stock_code]
                    current_price = tick_info['lastPrice']
                    limit_up_price = tick_info['highLimit']
                    
                    # 检查是否涨停
                    if current_price >= limit_up_price:
                        # 计算可买数量
                        buy_price = int(limit_up_price * 100) / 100  # 涨停价向下取整到分
                        size = int(self.p.order_amount // buy_price)
                        
                        if size > 0:
                            # 执行买入订单
                            self._buy_stock(stock_code, amount=self.p.order_amount)
                            
                            # 记录板块信息
                            for sector, pool in self.sector_pools.items():
                                if stock_code in pool.get('stocks', []):
                                    self.limit_up_sections.add(sector)
                                    self.logger.info(f"买入股票 {stock_code}，价格: {buy_price:.2f}，数量: {size}，板块: {sector}")
                                    break
                                    
                except Exception as e:
                    self.logger.error(f"处理股票 {stock_code} 买入时发生错误: {str(e)}")
                    continue

        except Exception as e:
            self.logger.error(f"执行交易时发生错误: {str(e)}")

    def _sell_positions(self):
        """次日卖出（策略步骤7实现）
        1. 获取所有持仓
        2. 获取当前行情数据
        3. 执行卖出订单
        """
        try:
            # 获取所有持仓
            positions = self.broker.get_positions()
            if not positions:
                return
                
            # 获取所有持仓股票的tick数据
            stock_list = [pos.data._name for pos in positions]
            try:
                tick_data = xtdata.get_full_tick(stock_list)
            except Exception as e:
                self.logger.error(f"获取tick数据失败: {str(e)}")
                return

            # 执行卖出订单
            for position in positions:
                stock_code = position.data._name
                if stock_code not in tick_data:
                    continue
                    
                try:
                    # 获取当前价格
                    current_price = tick_data[stock_code]['lastPrice']
                    
                    # 计算卖出数量
                    size = position.size
                    
                    if size > 0:
                        # 执行卖出订单
                        self._sell_stock(stock_code, proportion=1.0)
                        self.logger.info(f"卖出股票 {stock_code}，价格: {current_price:.2f}，数量: {size}")
                        
                except Exception as e:
                    self.logger.error(f"处理股票 {stock_code} 卖出时发生错误: {str(e)}")
                    continue

            # 清空相关池
            self.prepare_pool.clear()
            self.limit_up_sections.clear()
            self.triggered_weights.clear()
            
            self.logger.info("所有持仓已清空")
            
        except Exception as e:
            self.logger.error(f"执行卖出时发生错误: {str(e)}")

if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Add a strategy
    cerebro.addstrategy(SectorChaseStrategy)

    # 获取所有股票的行情数据
    stock_list = xtdata.get_stock_list_in_sector('沪深A股')  # 示例股票代码列表
    start_date = '20250321'
    end_date = '20250321'

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
