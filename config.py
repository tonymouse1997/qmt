from datetime import time

# 策略参数配置
class Config:
    # 市值筛选参数
    LARGE_CAP_THRESHOLD = 30_0000_0000  # 300亿
    SMALL_CAP_TURNOVER_THRESHOLD = 3_0000_0000  # 3亿
    
    # 涨幅筛选参数
    LARGE_CAP_INCREASE_THRESHOLD = 0.03  # 3%
    SMALL_CAP_INCREASE_THRESHOLD = 0.08  # 8%
    
    # 成交额筛选参数
    TICK_TURNOVER_THRESHOLD = 2000_0000  # 2000万
    DAILY_TURNOVER_THRESHOLD = 1_0000_0000  # 1亿
    
    # 交易参数
    ORDER_AMOUNT = 100000  # 单笔订单金额
    MAX_LIMIT_UP_SECTIONS = 3  # 最大涨停板块数
    
    # 交易时间
    SELL_TIME = time(9, 40)  # 次日卖出时间
    
    # 日志配置
    LOG_FILE = 'strategy.log'
    LOG_LEVEL = 'INFO'
    
    # QMT相关配置
    QMT_ACCOUNT = ''  # QMT账号
    QMT_PASSWORD = ''  # QMT密码
    QMT_TRADE_SERVER = ''  # QMT交易服务器地址
    QMT_DATA_SERVER = ''  # QMT数据服务器地址
