import logging
from datetime import datetime, timedelta
import pandas as pd

def setup_logger(name: str) -> logging.Logger:
    """设置日志记录器"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 控制台输出
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件输出
    file_handler = logging.FileHandler('strategy.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

def parse_time(time_str: str) -> datetime.time:
    """将字符串时间转换为datetime.time对象"""
    return datetime.strptime(time_str, '%H:%M:%S').time()

def is_trading_time(current_time: datetime.time) -> bool:
    """判断当前时间是否为交易时间"""
    return (current_time >= parse_time('09:30:00') and
            current_time <= parse_time('15:00:00'))

def format_tick_data(tick_data: pd.DataFrame) -> pd.DataFrame:
    """格式化tick数据"""
    # 实现数据格式化逻辑
    return tick_data

def handle_error(error: Exception, logger: logging.Logger):
    """统一错误处理"""
    logger.error(f"Error occurred: {str(error)}", exc_info=True)

def calculate_change_rate(current_price, previous_price):
    """
    计算涨跌幅

    参数:
        current_price (float): 当前价格
        previous_price (float): 前一个价格

    返回值:
        float: 涨跌幅，例如 0.1 表示 10% 的涨幅，-0.05 表示 5% 的跌幅
    """
    if previous_price == 0:
        return 0  # 避免除以零
    return (current_price - previous_price) / previous_price

