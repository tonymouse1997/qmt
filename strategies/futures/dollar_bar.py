from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import backtrader as bt
from datetime import datetime
from feed.qmt_feed import QMTDataFeed
from core.trade_interface import TradeInterface

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
        ('ma_fast', 5),  # 快速MA周期
        ('ma_slow', 10),  # 慢速MA周期
        ('rsi_period', 14),  # RSI周期
        ('rsi_overbought', 70),  # RSI超买阈值
        ('rsi_oversold', 30),  # RSI超卖阈值
        ('vol_period', 20),  # 波动率计算周期
        ('max_position_size', 1000),  # 最大持仓量
        ('max_drawdown', 0.1),  # 最大回撤限制
    )
    
    def __init__(self):
        # 初始化数据
        self.data = self.datas[0]
        self.order = None
        
        # 计算技术指标
        self.ma_fast = bt.indicators.SMA(
            self.data.close, period=self.params.ma_fast
        )
        self.ma_slow = bt.indicators.SMA(
            self.data.close, period=self.params.ma_slow
        )
        
        # RSI指标
        self.rsi = bt.indicators.RSI(
            self.data.close, period=self.params.rsi_period
        )
        
        # 波动率指标
        self.volatility = bt.indicators.StandardDeviation(
            self.data.close, period=self.params.vol_period
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
        print(f'{dt.isoformat()} {txt}')
        
    def next(self):
        """策略核心逻辑"""
        # 检查是否有待执行订单
        if self.order:
            return
            
        # 检查是否持仓
        if not self.position:
            # 买入条件：快线上穿慢线且RSI超卖
            if (self.ma_fast[0] > self.ma_slow[0] and 
                self.ma_fast[-1] <= self.ma_slow[-1] and
                self.rsi[0] < self.params.rsi_oversold):
                
                # 计算交易量（考虑波动率）
                vol_factor = 1 / (1 + self.volatility[0])
                size = int(100 * vol_factor)
                
                # 检查持仓限制
                if size <= self.params.max_position_size:
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

def main():
    """主函数"""
    # 创建cerebro引擎
    cerebro = bt.Cerebro()
    
    # 创建QMT数据源
    qmt_feed = QMTDataFeed()
    
    # 设置交易股票
    symbol = '600519.SH'  # 示例使用贵州茅台
    
    # 获取市场数据
    market_data = qmt_feed.get_market_data(
        stock_list=[symbol],
        start_date='20250301',
        end_date='20250301',
        period='tick',
        field_list=['open', 'high', 'low', 'close', 'volume']
    )
    
    # 将数据转换为backtrader数据格式
    data = bt.feeds.PandasData(
        dataname=market_data[symbol],
        datetime=None,  # 使用索引作为日期
        open='open',
        high='high',
        low='low',
        close='close',
        volume='volume',
        openinterest=-1
    )
    cerebro.adddata(data)
    
    # 设置初始资金
    cerebro.broker.setcash(1000000.0)
    
    # 设置手续费
    cerebro.broker.setcommission(commission=0.001)
    
    # 添加策略
    cerebro.addstrategy(DollarBarStrategy)
    
    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    
    # 运行回测
    print('初始资金: %.2f' % cerebro.broker.getvalue())
    results = cerebro.run()
    print('最终资金: %.2f' % cerebro.broker.getvalue())
    
    # 绘制结果
    cerebro.plot()

if __name__ == "__main__":
    main()