import logging
from datetime import datetime, timedelta

# 日志配置
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

# 回测参数
BACKTEST_PARAMS = {
    'initial_cash': 1000000.0,
    'start_date': (datetime.now() - timedelta(days=365)).strftime('%Y%m%d'),
    'end_date': datetime.now().strftime('%Y%m%d'),
    'period': '1d'
}

# 策略参数
STRATEGY_PARAMS = {
    'avg_turnover_days': 5,
    'min_turnover': 1000000.0,
    'max_position_size': 0.1  # 单个股票最大仓位比例
} 