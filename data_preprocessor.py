import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
from utils import setup_logger

class TickDataPreprocessor:
    """Tick数据预处理器，用于整合多个股票的tick数据"""
    
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.logger = setup_logger('TickDataPreprocessor')
        self.stock_data_cache = {}  # 股票数据缓存
        self.latest_tick_data = {}  # 最新tick数据
        
    def load_stock_data(self, stock_codes):
        """并行加载多个股票的tick数据"""
        self.logger.info(f"开始加载{len(stock_codes)}只股票的tick数据")
        
        def load_single_stock(stock_code):
            try:
                file_path = os.path.join(self.data_dir, f"{stock_code}_tick.csv")
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    return stock_code, df
                else:
                    self.logger.warning(f"未找到股票{stock_code}的tick数据文件")
                    return stock_code, None
            except Exception as e:
                self.logger.error(f"加载股票{stock_code}的tick数据时出错: {e}")
                return stock_code, None
        
        # 使用线程池并行加载数据
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(lambda code: load_single_stock(code), stock_codes)
            
        # 更新缓存
        for stock_code, df in results:
            if df is not None:
                self.stock_data_cache[stock_code] = df
                
        self.logger.info(f"成功加载{len(self.stock_data_cache)}只股票的tick数据")
        
    def get_current_tick_data(self, timestamp):
        """获取指定时间戳的所有股票tick数据"""
        result = {}
        
        for stock_code, df in self.stock_data_cache.items():
            # 找到最接近当前时间戳的tick数据
            closest_tick = df[df['timestamp'] <= timestamp].iloc[-1] if not df.empty else None
            
            if closest_tick is not None:
                result[stock_code] = {
                    'last': closest_tick['price'],
                    'price_change_pct': closest_tick['price_change_pct'],
                    'amount': closest_tick['amount'],
                    'daily_amount': closest_tick['daily_amount'],
                    'limit_up': closest_tick['limit_up'],
                    # 其他需要的字段...
                }
        
        self.latest_tick_data = result
        return result
    
    def update_pools(self, weights_pool, small_cap_pool):
        """更新权重池和小票池的数据"""
        # 确保所有池中的股票数据都已加载
        all_stocks = weights_pool.union(small_cap_pool)
        missing_stocks = all_stocks - set(self.stock_data_cache.keys())
        
        if missing_stocks:
            self.load_stock_data(missing_stocks)