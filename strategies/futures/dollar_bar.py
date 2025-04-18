from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import backtrader as bt
from datetime import datetime
from feed.qmt_feed import QMTDataFeed
from core.trade_interface import TradeInterface
import matplotlib.pyplot as plt
import logging
import math
from hmmlearn.hmm import GaussianHMM
from scipy import stats
import os

# 配置日志
logger = logging.getLogger(__name__)

class RiskManager:
    """风险管理模块"""
    def __init__(self, max_position_size: float, max_drawdown: float):
        self.max_position_size = max_position_size
        self.max_drawdown = max_drawdown
    
    def check_position_limit(self, current_position: float, new_order_size: float) -> bool:
        """检查持仓限制"""
        return abs(current_position + new_order_size) <= self.max_position_size
    
    def check_drawdown(self, current_drawdown: float) -> bool:
        """检查回撤限制"""
        return current_drawdown <= self.max_drawdown

class DollarBarStrategy(bt.Strategy):
    """Dollar Bar策略主类"""
    params = (
        ('dollar_value', 1000000),  # 每个dollar bar的交易金额
        ('ma_fast', 20),  # 快速MA周期
        ('ma_slow', 40),  # 慢速MA周期
        ('rsi_period', 14),  # RSI周期
        ('rsi_overbought', 70),  # RSI超买阈值
        ('rsi_oversold', 30),  # RSI超卖阈值
        ('trade_size', 100),  # 固定交易量
        ('max_position_size', 1000),  # 最大持仓量
        ('max_drawdown', 0.1),  # 最大回撤限制
    )
    
    def __init__(self):
        # 初始化数据
        self.data = self.datas[0]
        self.order = None
        
        # 计算技术指标
        self.ma_fast = bt.indicators.EMA(
            self.data.close,
            period=self.params.ma_fast
        )
        self.ma_slow = bt.indicators.EMA(
            self.data.close,
            period=self.params.ma_slow
        )
        
        # RSI指标
        self.rsi = bt.indicators.RSI(
            self.data.close,
            period=self.params.rsi_period,
            safediv=True  # 使用安全除法
        )
        
        # 记录交易状态
        self.buyprice = None
        self.buycomm = None
        
    def notify_order(self, order):
        """订单状态更新通知"""
        if order.status in [order.Submitted, order.Accepted]:
            return
            
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'买入执行 价格: {order.executed.price:.2f}, 成本: {order.executed.value:.2f}, 手续费: {order.executed.comm:.2f}')
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log(f'卖出执行 价格: {order.executed.price:.2f}, 成本: {order.executed.value:.2f}, 手续费: {order.executed.comm:.2f}')
                
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('订单取消/保证金不足/拒绝')
            
        self.order = None
        
    def notify_trade(self, trade):
        """交易结果通知"""
        if not trade.isclosed:
            return
            
        self.log(f'交易利润: {trade.pnl:.2f}, 净利润: {trade.pnlcomm:.2f}')
        
    def log(self, txt, dt=None):
        """日志记录"""
        dt = dt or self.datas[0].datetime.date(0)
        logger.info(f'{dt.isoformat()} {txt}')
        
    def next(self):
        """策略核心逻辑"""
        # 检查是否有待执行订单
        if self.order:
            return
            
        # 检查数据是否有效
        if not self.data.close[0] > 0:
            return
            
        # 检查指标是否已经准备好
        if len(self.data) <= self.params.ma_slow:
            return
            
        # 检查指标值是否有效
        if math.isnan(self.ma_fast[0]) or math.isnan(self.ma_slow[0]) or math.isnan(self.rsi[0]):
            return
            
        # 检查是否持仓
        if not self.position:
            # 买入条件：快线上穿慢线且RSI超卖
            if (self.ma_fast[0] > self.ma_slow[0] and 
                self.ma_fast[-1] <= self.ma_slow[-1] and
                self.rsi[0] < self.params.rsi_oversold):
                
                # 使用固定交易量
                size = min(self.params.trade_size, self.params.max_position_size)
                
                self.log(f'买入信号, 价格: {self.data.close[0]:.2f}, 数量: {size}')
                self.order = self.buy(size=size)
                    
        else:
            # 卖出条件：快线下穿慢线且RSI超买
            if (self.ma_fast[0] < self.ma_slow[0] and 
                self.ma_fast[-1] >= self.ma_slow[-1] and
                self.rsi[0] > self.params.rsi_overbought):
                
                self.log(f'卖出信号, 价格: {self.data.close[0]:.2f}')
                self.order = self.sell()
                
    def stop(self):
        """策略停止时的处理"""
        self.log('策略停止')

def convert_to_dollar_bars(df: pd.DataFrame, dollar_value: float = 1000000, period: str = 'tick') -> pd.DataFrame:
    """将原始数据转换为dollar bar格式"""
    logger.info("="*50)
    logger.info("开始转换dollar bar")
    logger.info(f"输入数据大小: {len(df)} 行")
    logger.info(f"目标dollar value: {dollar_value}")
    logger.info(f"数据频率: {period}")
    
    # 检查数据是否为空
    if df is None or df.empty:
        logger.error("输入数据为空")
        raise ValueError("输入数据为空")
    
    # 确定价格字段
    price_field = 'lastPrice' if period == 'tick' else 'close'
    logger.info(f"使用价格字段: {price_field}")
    
    # 确保索引是日期时间格式
    if not isinstance(df.index, pd.DatetimeIndex):
        logger.info("转换输入数据索引为日期时间格式...")
        df.index = pd.to_datetime(df.index)
            
    # 处理NaN值
    logger.info("处理NaN值...")
    original_len = len(df)
    df = df.dropna(subset=[price_field, 'volume'])
    if len(df) < original_len:
        logger.warning(f"删除了 {original_len - len(df)} 行包含NaN的数据")
    if df.empty:
        logger.error("数据中所有行都包含NaN值")
        raise ValueError("数据中所有行都包含NaN值")
    
    # 计算每笔交易的金额
    logger.info("计算交易金额...")
    df['dollar_volume'] = df[price_field] * df['volume']
    df['cum_dollar_volume'] = df['dollar_volume'].cumsum()
    
    # 计算dollar bar的边界
    logger.info("计算dollar bar边界...")
    total_dollar_volume = df['cum_dollar_volume'].iloc[-1]
    if pd.isna(total_dollar_volume) or total_dollar_volume <= 0:
        logger.error("累计交易金额无效")
        raise ValueError("累计交易金额无效")
        
    bar_count = int(total_dollar_volume / dollar_value)
    if bar_count <= 0:
        logger.error("交易金额太小，无法生成dollar bar")
        raise ValueError("交易金额太小，无法生成dollar bar")
    
    logger.info(f"预计生成 {bar_count} 个dollar bars")
    bar_boundaries = np.linspace(0, total_dollar_volume, bar_count + 1)
    
    # 创建新的dollar bar数据
    logger.info("开始生成dollar bars...")
    dollar_bars = []
    current_bar = {
        'open': None,
        'high': -np.inf,
        'low': np.inf,
        price_field: None,
        'volume': 0,
        'datetime': None
    }
    
    current_boundary_idx = 0
    for idx, row in df.iterrows():
        if row['cum_dollar_volume'] >= bar_boundaries[current_boundary_idx + 1]:
            # 完成当前bar
            if current_bar['open'] is not None:
                dollar_bars.append(current_bar)
                logger.debug(f"完成第 {len(dollar_bars)} 个bar")
            
            # 开始新的bar
            current_bar = {
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                price_field: row[price_field],
                'volume': row['volume'],
                'datetime': idx
            }
            current_boundary_idx += 1
        else:
            # 更新当前bar
            if current_bar['open'] is None:
                current_bar['open'] = row['open']
                current_bar['datetime'] = idx
            
            current_bar['high'] = max(current_bar['high'], row['high'])
            current_bar['low'] = min(current_bar['low'], row['low'])
            current_bar[price_field] = row[price_field]
            current_bar['volume'] += row['volume']
    
    # 添加最后一个bar
    if current_bar['open'] is not None:
        dollar_bars.append(current_bar)
        logger.debug(f"完成最后一个bar，总共 {len(dollar_bars)} 个")
    
    if not dollar_bars:
        logger.error("无法生成任何dollar bar")
        raise ValueError("无法生成任何dollar bar")
    
    # 创建DataFrame并设置索引
    result_df = pd.DataFrame(dollar_bars)
    result_df.set_index('datetime', inplace=True)
    
    # 计算对数差分特征
    logger.info("计算对数差分特征...")
    fields = ['open', 'high', 'low', price_field, 'volume']
    
    # 创建特征字典
    feature_dict = {}
    
    for field in fields:
        # 计算对数价格
        log_price = np.log(result_df[field])
        
        # 计算1-10阶差分
        for i in range(1, 11):
            # 计算差分
            diff_name = f'{field}_{i}_diff'
            feature_dict[diff_name] = log_price.diff(i)
            
            # 使用HMM为差分数据打标签
            diff_data = feature_dict[diff_name].dropna().values.reshape(-1, 1)
            hmm = GaussianHMM(n_components=2, random_state=42)
            hmm.fit(diff_data)
            labels = hmm.predict(diff_data)
            
            # 将标签添加到特征字典中
            label_name = f'{field}_{i}_label'
            feature_dict[label_name] = pd.Series(index=result_df.index, dtype='float64')
            feature_dict[label_name].loc[~feature_dict[diff_name].isna()] = labels
            logger.debug(f"计算{label_name}完成")
    
    # 一次性将所有特征添加到DataFrame中
    result_df = pd.concat([result_df, pd.DataFrame(feature_dict, index=result_df.index)], axis=1)
    
    logger.info(f"成功生成 {len(result_df)} 个dollar bars")
    logger.info("="*50)
    return result_df

def analyze_normality(df: pd.DataFrame):
    """分析DataFrame中每个特征的正态性"""
    logger.info("开始分析特征的正态性...")
    
    # 获取数值型列
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    # 创建结果字典
    normality_results = {}
    
    # 对每个特征进行Shapiro-Wilk检验
    for col in numeric_cols:
        # 去除NaN值
        data = df[col].dropna()
        
        # 进行Shapiro-Wilk检验
        statistic, p_value = stats.shapiro(data)
        
        # 计算偏度和峰度
        skewness = stats.skew(data)
        kurtosis = stats.kurtosis(data)
        
        # 存储结果
        normality_results[col] = {
            'p_value': p_value,
            'is_normal': p_value > 0.05,  # p值大于0.05表示符合正态分布
            'skewness': skewness,
            'kurtosis': kurtosis
        }
        
        # 创建Q-Q图
        plt.figure(figsize=(10, 6))
        stats.probplot(data, dist="norm", plot=plt)
        plt.title(f'Q-Q Plot for {col}')
        plt.savefig(f'qq_plot_{col}.png')
        plt.close()
        
        logger.info(f"{col}:")
        logger.info(f"  Shapiro-Wilk检验 p值: {p_value:.4f}")
        logger.info(f"  是否服从正态分布: {'是' if p_value > 0.05 else '否'}")
        logger.info(f"  偏度: {skewness:.4f}")
        logger.info(f"  峰度: {kurtosis:.4f}")
    
    return normality_results

def plot_price_with_labels(df: pd.DataFrame, price_field: str = 'lastPrice', save_path: str = None):
    """
    绘制price_field列的折线图，并在图上用不同颜色标注label
    
    参数:
    df: 包含价格和标签数据的DataFrame
    price_field: 价格字段名称，默认为'lastPrice'
    save_path: 图表保存路径，默认为None（不保存）
    """
    logger.info(f"开始绘制{price_field}价格与标签的折线图...")
    
    # 检查price_field是否存在于DataFrame中
    if price_field not in df.columns:
        logger.error(f"价格字段 {price_field} 不存在于DataFrame中")
        raise ValueError(f"价格字段 {price_field} 不存在于DataFrame中")
    
    # 获取所有与price_field相关的标签列
    label_columns = [col for col in df.columns if col.startswith(f'{price_field}_') and col.endswith('_label')]
    
    if not label_columns:
        logger.error(f"未找到与 {price_field} 相关的标签列")
        raise ValueError(f"未找到与 {price_field} 相关的标签列")
    
    # 为每个一阶差分创建一个图表
    for i in range(1, 11):
        label_col = f'{price_field}_{i}_label'
        if label_col not in df.columns:
            logger.warning(f"标签列 {label_col} 不存在，跳过")
            continue
        
        # 创建图表
        plt.figure(figsize=(15, 8))
        
        # 绘制价格折线图
        plt.plot(df.index, df[price_field], label=price_field, color='black', alpha=0.7)
        
        # 获取唯一的标签值
        unique_labels = df[label_col].dropna().unique()
        
        # 为每个标签值创建散点图
        for label in unique_labels:
            # 获取该标签对应的数据点
            mask = df[label_col] == label
            plt.scatter(df.index[mask], df[price_field][mask], 
                       label=f'标签 {int(label)}', 
                       alpha=0.7)
        
        # 设置图表属性
        plt.title(f'{price_field} 价格与 {i} 阶差分标签')
        plt.xlabel('日期')
        plt.ylabel('价格')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # 保存图表
        if save_path:
            plt.savefig(f'{save_path}/{price_field}_{i}_label.png', dpi=300, bbox_inches='tight')
            logger.info(f"图表已保存至 {save_path}/{price_field}_{i}_label.png")
        
        # 显示图表
        plt.tight_layout()
        plt.show()
        
        logger.info(f"完成 {price_field} 价格与 {i} 阶差分标签的图表绘制")

def main():
    """主函数"""
    logger.info("="*50)
    logger.info("开始运行Dollar Bar策略")
    
    # 创建cerebro引擎
    logger.info("初始化Backtrader引擎...")
    cerebro = bt.Cerebro()
    
    # 创建QMT数据源
    logger.info("创建QMT数据源...")
    qmt_feed = QMTDataFeed()
    
    # 设置交易股票
    symbol = '600519.SH'  # 示例使用贵州茅台
    logger.info(f"目标股票: {symbol}")
    
    # 设置数据频率
    period = '1m'  # 可以是'tick', '1m', '5m', '15m', '30m', '1d'等
    logger.info(f"数据频率: {period}")
    
    # 创建保存图表的目录
    plots_dir = './plots'
    if not os.path.exists(plots_dir):
        os.makedirs(plots_dir)
        logger.info(f"创建图表保存目录: {plots_dir}")
    
    # 获取市场数据
    logger.info("获取市场数据...")
    market_data = qmt_feed.get_market_data(
        stock_list=[symbol],
        start_date='20250101',
        end_date='20250402',
        period=period,
        field_list=['open', 'high', 'low', 'lastPrice', 'close', 'volume'],
        auto_download=True
    )
    
    # 删除全为0的行
    if market_data and symbol in market_data:
        df = market_data[symbol]
        market_data[symbol] = df.loc[~(df == 0).all(axis=1)]
    
    if not market_data or symbol not in market_data:
        logger.error(f"无法获取股票 {symbol} 的数据")
        return
    
    # 转换原始数据为dollar bar
    logger.info("转换数据为Dollar Bar格式...")
    original_data = market_data[symbol]
    dollar_bar_data = convert_to_dollar_bars(original_data, period=period)
    analyze_normality(dollar_bar_data)
    
    # 绘制价格与标签的折线图
    price_field = 'lastPrice' if period == 'tick' else 'close'
    plot_price_with_labels(dollar_bar_data, price_field=price_field, save_path='./plots')
    
    # # 将原始数据转换为backtrader数据格式
    # logger.info("转换数据为Backtrader格式...")
    # original_bt_data = bt.feeds.PandasData(
    #     dataname=original_data,
    #     datetime=None,  # 使用索引作为日期时间
    #     open='open',
    #     high='high',
    #     low='low',
    #     close='lastPrice' if period == 'tick' else 'close',
    #     volume='volume',
    #     openinterest=-1
    # )
    
    # # 将dollar bar数据转换为backtrader数据格式
    # # 确保dollar bar数据的索引是日期时间格式
    # if not isinstance(dollar_bar_data.index, pd.DatetimeIndex):
    #     logger.info("转换dollar bar数据索引为日期时间格式...")
    #     dollar_bar_data.index = pd.to_datetime(dollar_bar_data.index)
    
    # dollar_bar_bt_data = bt.feeds.PandasData(
    #     dataname=dollar_bar_data,
    #     datetime=None,  # 使用索引作为日期时间
    #     open='open',
    #     high='high',
    #     low='low',
    #     close='lastPrice' if period == 'tick' else 'close',
    #     volume='volume',
    #     openinterest=-1
    # )
    
    # # 添加数据到cerebro
    # logger.info("添加数据到Backtrader...")
    # cerebro.adddata(original_bt_data, name='original')
    # cerebro.adddata(dollar_bar_bt_data, name='dollar_bar')
    
    # # 设置初始资金
    # initial_cash = 1000000.0
    # logger.info(f"设置初始资金: {initial_cash}")
    # cerebro.broker.setcash(initial_cash)
    
    # # 设置手续费
    # commission = 0.001
    # logger.info(f"设置手续费率: {commission}")
    # cerebro.broker.setcommission(commission=commission)
    
    # # 添加策略
    # logger.info("添加交易策略...")
    # cerebro.addstrategy(DollarBarStrategy)
    
    # # 添加分析器
    # logger.info("添加分析器...")
    # cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    # cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    
    # # 运行回测
    # logger.info("开始回测...")
    # logger.info(f'初始资金: {cerebro.broker.getvalue():.2f}')
    # results = cerebro.run()
    # final_value = cerebro.broker.getvalue()
    # logger.info(f'最终资金: {final_value:.2f}')
    # logger.info(f'收益率: {((final_value - initial_cash) / initial_cash * 100):.2f}%')
    
    # # 绘制结果
    # logger.info("生成回测图表...")
    
    # # 使用Backtrader的内置绘图功能
    # # 设置绘图参数，确保时间轴对齐
    # cerebro.plot(style='candle', 
    #             barup='red', bardown='green', 
    #             volup='red', voldown='green', 
    #             grid=True, volume=True,
    #             width=16, height=9,  # 设置图表大小
    #             dpi=100,  # 设置分辨率
    #             numfigs=1,
    #             plotmode=1,  # 使用滑动条模式
    #             plotdist=2)  # 增加K线间距，使图表更稀疏
    
    # logger.info("策略运行完成")
    # logger.info("="*50)

if __name__ == "__main__":
    # 配置日志格式
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    main()