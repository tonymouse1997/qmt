import backtrader as bt
import logging
from datetime import datetime, timedelta
from config.settings import setup_logging, BACKTEST_PARAMS, STRATEGY_PARAMS
from interfaces.qmt.data_feed import QMTDataFeed
from strategies.sector_chase import SectorChaseStrategy
from interfaces.backtrader.interface import BacktraderTradeInterface

def main():
    # 配置日志
    setup_logging()
    
    # 创建cerebro引擎
    cerebro = bt.Cerebro()
    
    # 创建数据源和交易接口
    data_feed = QMTDataFeed(avg_turnover_days=STRATEGY_PARAMS['avg_turnover_days'])
    
    # 获取市场数据
    stock_list = data_feed.get_sector_stocks('沪深A股')
    if not stock_list:
        logging.error("获取股票列表失败")
        return
        
    # 添加数据到cerebro
    for stock in stock_list:
        data = data_feed.get_market_data(
            stock_list=[stock],
            start_date=BACKTEST_PARAMS['start_date'],
            end_date=BACKTEST_PARAMS['end_date'],
            period=BACKTEST_PARAMS['period']
        )
        if stock in data:
            cerebro.adddata(data[stock], name=stock)
    
    # 设置初始资金
    cerebro.broker.setcash(BACKTEST_PARAMS['initial_cash'])
    
    # 创建交易接口
    trade_interface = BacktraderTradeInterface(cerebro.broker)
    
    # 添加策略
    cerebro.addstrategy(SectorChaseStrategy, 
                       data_feed=data_feed, 
                       trade_interface=trade_interface)
    
    # 运行回测
    cerebro.run()
    
    # 绘制结果
    cerebro.plot()

if __name__ == '__main__':
    main() 