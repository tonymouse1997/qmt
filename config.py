# 策略参数配置
class Config:
    # 股票池参数
    BIG_MARKET_CAP = 30_0000_0000  # 3亿流通市值阈值（单位：元）
    SMALL_AMOUNT_THRESHOLD = 300_000_000  # 小票池成交额阈值（3亿）
    ADDITIONAL_STOCKS = []  # 自定义添加的股票代码列表
    
    # 触发条件参数 
    WEIGHT_GAIN_THRESHOLD = 0.03  # 权重股涨幅阈值（3%）
    SECTOR_GAIN_THRESHOLD = 0.08  # 板块个股涨幅阈值（8%）
    TICK_AMOUNT_THRESHOLD = 20_000_000  # 单tick成交额阈值（2000万）
    DAILY_AMOUNT_THRESHOLD = 100_000_000  # 当日成交额阈值（1亿）
    
    # 交易参数
    ORDER_AMOUNT = 100_000  # 单笔委托金额（元）
    MAX_SECTORS = 3  # 最大允许涨停板块数
    SELL_TIME = "09:40"  # 次日卖出时间
    
    # 风控参数
    MAX_POSITIONS = 5  # 最大持仓数量
    STOP_LOSS = 0.05
