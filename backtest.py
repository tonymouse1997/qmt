import backtrader as bt
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import os
import numpy as np
from strategy import SectorChaseStrategy
from config import Config
from qmt_data_fetcher import DataFetcher
from utils import setup_logger


class QMTData(bt.feeds.PandasData):
    """QMT数据源适配器"""
    params = (
        ('datetime', 0),  # 日期时间列
        ('open', 1),       # 开盘价列
        ('high', 2),       # 最高价列
        ('low', 3),        # 最低价列
        ('close', 4),      # 收盘价列
        ('volume', 5),     # 成交量列
        ('amount', 6),     # 成交额列
    )


def load_market_cap_data():
    """加载市值数据"""
    market_cap_file = 'a_share_data/market_cap/market_cap.csv'
    if not os.path.exists(market_cap_file):
        raise FileNotFoundError(f"市值数据文件不存在: {market_cap_file}")
    
    market_cap_df = pd.read_csv(market_cap_file)
    # 将股票代码作为索引，市值作为值
    return dict(zip(market_cap_df['stock_code'], market_cap_df['market_cap']))


def load_sector_data():
    """加载板块数据"""
    sector_file = 'a_share_data/sector_info/sector_info.csv'
    if not os.path.exists(sector_file):
        raise FileNotFoundError(f"板块数据文件不存在: {sector_file}")
    
    sector_df = pd.read_csv(sector_file)
    # 将股票代码作为索引，板块作为值
    return dict(zip(sector_df['stock_code'], sector_df['sector']))


def prepare_backtest_data(stock_codes, start_date, end_date):
    """准备回测数据
    
    Args:
        stock_codes: 股票代码列表
        start_date: 开始日期，格式：'YYYY-MM-DD'
        end_date: 结束日期，格式：'YYYY-MM-DD'
        
    Returns:
        dict: 股票代码到数据源的映射
    """
    logger = setup_logger('DataPreparation')
    data_feeds = {}
    
    for stock in stock_codes:
        try:
            # 这里假设数据已经下载并保存为CSV格式
            # 实际应用中可能需要通过QMT API获取数据
            file_path = f'data/{stock}.csv'
            if not os.path.exists(file_path):
                logger.warning(f"数据文件不存在: {file_path}，跳过该股票")
                continue
                
            # 读取CSV数据
            df = pd.read_csv(file_path, parse_dates=['datetime'])
            df = df[(df['datetime'] >= start_date) & (df['datetime'] <= end_date)]
            
            if len(df) == 0:
                logger.warning(f"股票 {stock} 在指定时间段内没有数据，跳过")
                continue
                
            # 创建数据源
            data_feed = QMTData(
                dataname=df,
                fromdate=datetime.datetime.strptime(start_date, '%Y-%m-%d'),
                todate=datetime.datetime.strptime(end_date, '%Y-%m-%d')
            )
            data_feeds[stock] = data_feed
            logger.info(f"成功加载股票 {stock} 的数据")
            
        except Exception as e:
            logger.error(f"加载股票 {stock} 数据时出错: {str(e)}")
    
    return data_feeds


def run_backtest(data_feeds, strategy_class=SectorChaseStrategy, **kwargs):
    """运行回测
    
    Args:
        data_feeds: 数据源字典
        strategy_class: 策略类
        **kwargs: 策略参数
        
    Returns:
        cerebro: 回测引擎
    """
    if not data_feeds:
        raise ValueError("没有可用的数据源")
    
    # 创建回测引擎
    cerebro = bt.Cerebro()
    
    # 设置初始资金
    cerebro.broker.setcash(kwargs.get('initial_cash', 1000000))
    
    # 设置手续费
    cerebro.broker.setcommission(commission=kwargs.get('commission', 0.0003))
    
    # 添加数据源
    for stock, data in data_feeds.items():
        cerebro.adddata(data, name=stock)
    
    # 添加策略
    cerebro.addstrategy(strategy_class, **kwargs)
    
    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trade')
    
    # 运行回测
    results = cerebro.run()
    
    return cerebro, results[0]


def analyze_results(results):
    """分析回测结果
    
    Args:
        results: 回测结果
        
    Returns:
        dict: 回测指标
    """
    # 获取分析器结果
    sharpe = results.analyzers.sharpe.get_analysis()
    drawdown = results.analyzers.drawdown.get_analysis()
    returns = results.analyzers.returns.get_analysis()
    trade = results.analyzers.trade.get_analysis()
    
    # 计算关键指标
    metrics = {
        '夏普比率': sharpe.get('sharperatio', 0.0),
        '年化收益率': returns.get('rnorm100', 0.0),
        '最大回撤': drawdown.get('max', 0.0) * 100,
        '最大回撤周期': drawdown.get('maxlen', 0),
        '总交易次数': trade.get('total', {}).get('total', 0),
        '盈利交易次数': trade.get('won', {}).get('total', 0),
        '亏损交易次数': trade.get('lost', {}).get('total', 0),
        '胜率': trade.get('won', {}).get('total', 0) / max(trade.get('total', {}).get('total', 1), 1) * 100,
        '平均收益': trade.get('pnl', {}).get('net', {}).get('average', 0.0),
        '收益标准差': trade.get('pnl', {}).get('net', {}).get('stddev', 0.0),
    }
    
    return metrics


def plot_results(cerebro, title='策略回测结果'):
    """绘制回测结果
    
    Args:
        cerebro: 回测引擎
        title: 图表标题
    """
    plt.figure(figsize=(12, 8))
    cerebro.plot(style='candle', barup='red', bardown='green', 
                 volup='red', voldown='green', 
                 grid=True, plotdist=1, 
                 subplot=True)
    plt.title(title)
    plt.tight_layout()
    plt.savefig('backtest_result.png')
    plt.show()


def print_metrics(metrics):
    """打印回测指标
    
    Args:
        metrics: 回测指标字典
    """
    print("\n" + "=" * 50)
    print("回测结果分析")
    print("=" * 50)
    
    for key, value in metrics.items():
        if isinstance(value, float):
            print(f"{key}: {value:.2f}")
        else:
            print(f"{key}: {value}")
    
    print("=" * 50 + "\n")


def backtest_strategy(start_date, end_date, stock_list=None, **kwargs):
    """执行策略回测的主函数
    
    Args:
        start_date: 开始日期，格式：'YYYY-MM-DD'
        end_date: 结束日期，格式：'YYYY-MM-DD'
        stock_list: 股票代码列表，如果为None则使用全市场股票
        **kwargs: 策略参数
        
    Returns:
        tuple: (回测引擎, 回测结果, 回测指标)
    """
    logger = setup_logger('Backtest')
    logger.info(f"开始回测，时间段: {start_date} 至 {end_date}")
    
    # 如果没有指定股票列表，则使用全市场股票
    if stock_list is None:
        # 这里可以通过QMT API获取全市场股票列表
        # 或者从本地文件加载
        try:
            market_cap_data = load_market_cap_data()
            stock_list = list(market_cap_data.keys())
            logger.info(f"使用全市场股票，共 {len(stock_list)} 只")
        except Exception as e:
            logger.error(f"加载全市场股票失败: {str(e)}")
            return None, None, None
    
    # 准备回测数据
    data_feeds = prepare_backtest_data(stock_list, start_date, end_date)
    if not data_feeds:
        logger.error("没有可用的数据源，回测终止")
        return None, None, None
    
    # 运行回测
    try:
        cerebro, results = run_backtest(data_feeds, **kwargs)
        logger.info("回测完成")
        
        # 分析结果
        metrics = analyze_results(results)
        logger.info("结果分析完成")
        
        # 打印指标
        print_metrics(metrics)
        
        # 绘制结果
        plot_results(cerebro)
        
        return cerebro, results, metrics
        
    except Exception as e:
        logger.error(f"回测过程中出错: {str(e)}")
        return None, None, None


if __name__ == '__main__':
    # 回测参数
    params = {
        'initial_cash': 1000000,  # 初始资金
        'commission': 0.0003,      # 手续费率
        # 策略参数，可以覆盖Config中的默认值
        'big_market_cap': Config.BIG_MARKET_CAP,
        'weight_gain': Config.WEIGHT_GAIN_THRESHOLD,
        'sector_gain': Config.SECTOR_GAIN_THRESHOLD,
        'tick_amount': Config.TICK_AMOUNT_THRESHOLD,
        'daily_amount': Config.DAILY_AMOUNT_THRESHOLD,
        'order_amount': Config.ORDER_AMOUNT,
        'max_sectors': Config.MAX_SECTORS,
        'sell_time': Config.SELL_TIME,
        'max_positions': Config.MAX_POSITIONS
    }
    
    # 执行回测
    start_date = '2022-01-01'
    end_date = '2022-12-31'
    
    # 可以指定股票列表，也可以使用全市场股票
    # stock_list = ['600000.SH', '000001.SZ', ...]
    # backtest_strategy(start_date, end_date, stock_list, **params)
    
    # 使用全市场股票
    backtest_strategy(start_date, end_date, **params)


class Backtest:
    def __init__(self, start_date, end_date, initial_capital=1000000):
        """初始化回测系统
        
        Args:
            start_date (str): 回测开始日期，格式：'YYYY-MM-DD'
            end_date (str): 回测结束日期，格式：'YYYY-MM-DD'
            initial_capital (float): 初始资金，默认100万
        """
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.positions = {}
        self.trades = []
        self.daily_returns = []
        self.data_fetcher = DataFetcher()
        self.logger = setup_logger('Backtest')
        
    def run(self, strategy):
        """运行回测
        
        Args:
            strategy: 策略对象，需要实现 generate_signals 方法
            
        Returns:
            dict: 回测结果统计
        """
        self.logger.info(f'开始回测 {self.start_date} 至 {self.end_date}')
        
        # 获取回测期间的历史数据
        stock_list = self.data_fetcher.get_stock_list_in_sector('沪深A股')
        historical_data = self.data_fetcher.get_history_data(stock_list, self.start_date, self.end_date)
        
        # 按日期遍历
        dates = sorted(list(set([date for data in historical_data.values() for date in data['date']])))
        for date in dates:
            self.logger.info(f'正在回测交易日：{date}')
            
            # 获取当日可交易的股票列表
            tradable_stocks = self._get_tradable_stocks(historical_data, date)
            
            # 生成交易信号
            signals = strategy.generate_signals(historical_data, tradable_stocks, date)
            
            # 执行交易
            self._execute_trades(signals, historical_data, date)
            
            # 更新每日收益
            self._update_daily_returns(historical_data, date)
        
        # 计算回测结果
        results = self._calculate_results()
        self.logger.info('回测完成，统计结果：')
        for key, value in results.items():
            self.logger.info(f'{key}: {value:.2f}%')
            
        return results
    
    def _get_tradable_stocks(self, historical_data, date):
        """获取当日可交易的股票列表"""
        tradable_stocks = []
        for stock, data in historical_data.items():
            if date in data['date'].values:
                tradable_stocks.append(stock)
        return tradable_stocks
    
    def _execute_trades(self, signals, historical_data, date):
        """执行交易"""
        # 处理卖出信号
        for stock in list(self.positions.keys()):
            if signals.get(stock) == 'sell':
                self._sell_stock(stock, historical_data[stock], date)
        
        # 处理买入信号
        available_positions = self.initial_capital * 0.95 / 500000  # 假设每个持仓50万，留5%现金
        current_positions = len(self.positions)
        
        for stock, signal in signals.items():
            if signal == 'buy' and stock not in self.positions:
                if current_positions >= available_positions:
                    self.logger.info(f'达到最大持仓数量限制，跳过买入信号：{stock}')
                    continue
                self._buy_stock(stock, historical_data[stock], date)
                current_positions += 1
    
    def _buy_stock(self, stock, data, date):
        """买入股票"""
        try:
            price = data[data['date'] == date]['close'].values[0]
            # 计算可买入数量（每个持仓50万）
            target_amount = 500000
            shares = int(target_amount / price / 100) * 100  # 向下取整到100股
            
            if shares > 0:
                cost = shares * price
                self.current_capital -= cost
                self.positions[stock] = {
                    'shares': shares,
                    'cost': price
                }
                self.trades.append({
                    'date': date,
                    'stock': stock,
                    'type': 'buy',
                    'price': price,
                    'shares': shares,
                    'cost': cost
                })
                self.logger.info(f'买入 {stock}: {shares}股，价格：{price:.2f}，总成本：{cost:.2f}')
        except Exception as e:
            self.logger.error(f'买入{stock}时出错：{str(e)}')
    
    def _sell_stock(self, stock, data, date):
        """卖出股票"""
        try:
            if stock in self.positions:
                price = data[data['date'] == date]['close'].values[0]
                shares = self.positions[stock]['shares']
                revenue = shares * price
                self.current_capital += revenue
                
                self.trades.append({
                    'date': date,
                    'stock': stock,
                    'type': 'sell',
                    'price': price,
                    'shares': shares,
                    'revenue': revenue
                })
                
                profit = revenue - self.positions[stock]['shares'] * self.positions[stock]['cost']
                profit_rate = (profit / (self.positions[stock]['shares'] * self.positions[stock]['cost'])) * 100
                self.logger.info(f'卖出 {stock}: {shares}股，价格：{price:.2f}，收入：{revenue:.2f}，盈亏：{profit:.2f}（{profit_rate:.2f}%）')
                
                del self.positions[stock]
        except Exception as e:
            self.logger.error(f'卖出{stock}时出错：{str(e)}')
    
    def _update_daily_returns(self, historical_data, date):
        """更新每日收益"""
        try:
            total_value = self.current_capital
            for stock, position in self.positions.items():
                price = historical_data[stock][historical_data[stock]['date'] == date]['close'].values[0]
                total_value += position['shares'] * price
            
            self.daily_returns.append({
                'date': date,
                'total_value': total_value
            })
        except Exception as e:
            self.logger.error(f'更新每日收益时出错：{str(e)}')
    
    def _calculate_results(self):
        """计算回测结果"""
        try:
            df_returns = pd.DataFrame(self.daily_returns)
            df_returns['return'] = df_returns['total_value'].pct_change()
            
            results = {
                '总收益率': (self.daily_returns[-1]['total_value'] / self.initial_capital - 1) * 100,
                '年化收益率': self._calculate_annual_return(df_returns['return']),
                '最大回撤': self._calculate_max_drawdown(df_returns['total_value']),
                '夏普比率': self._calculate_sharpe_ratio(df_returns['return']),
                '胜率': self._calculate_win_rate(),
                '交易次数': len(self.trades)
            }
            
            return results
        except Exception as e:
            self.logger.error(f'计算回测结果时出错：{str(e)}')
            return {}
    
    def _calculate_annual_return(self, returns):
        """计算年化收益率"""
        total_days = len(returns)
        total_return = (1 + returns).prod() - 1
        annual_return = (1 + total_return) ** (252 / total_days) - 1
        return annual_return * 100
    
    def _calculate_max_drawdown(self, values):
        """计算最大回撤"""
        max_drawdown = 0
        peak = values[0]
        
        for value in values:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak
            max_drawdown = max(max_drawdown, drawdown)
        
        return max_drawdown * 100
    
    def _calculate_sharpe_ratio(self, returns):
        """计算夏普比率"""
        risk_free_rate = 0.03  # 假设无风险利率为3%
        excess_returns = returns - risk_free_rate / 252
        if len(excess_returns) > 0:
            sharpe_ratio = np.sqrt(252) * excess_returns.mean() / excess_returns.std()
            return sharpe_ratio
        return 0
    
    def _calculate_win_rate(self):
        """计算胜率"""
        if not self.trades:
            return 0
            
        profitable_trades = 0
        for i in range(0, len(self.trades), 2):
            if i + 1 < len(self.trades):
                buy_trade = self.trades[i]
                sell_trade = self.trades[i + 1]
                if sell_trade['revenue'] > buy_trade['cost']:
                    profitable_trades += 1
                    
        return (profitable_trades / (len(self.trades) // 2)) * 100