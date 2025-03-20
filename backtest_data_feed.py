import pandas as pd
from xtquant import xtdata
from functools import lru_cache

class BacktestDataFeed:
    def __init__(self, stock_list: list[str], period: str, start_time: str, end_time: str, field_list: list[str] = None):
        self.stock_list = stock_list
        self.period = period
        self.start_time = start_time
        self.end_time = end_time
        self.field_list = field_list if field_list else []
        self._data_cache = {}
        self._market_data = None

    def _load_market_data(self):
        """Lazy load market data only when needed"""
        if self._market_data is None:
            try:
                self._market_data = xtdata.get_market_data_ex(
                    field_list=self.field_list,
                    stock_list=self.stock_list,
                    period=self.period,
                    start_time=self.start_time,
                    end_time=self.end_time,
                    count=-1,
                    dividend_type='none',
                    fill_data=True,
                )
            except Exception as e:
                print(f"Error loading market data: {e}")
                self._market_data = {}
        return self._market_data
        
    @lru_cache(maxsize=32)
    def _process_stock_data(self, stock_code):
        """Process and cache data for a specific stock"""
        market_data = self._load_market_data()
        if stock_code not in market_data:
            print(f"No data found for {stock_code}")
            return pd.DataFrame()
            
        df = pd.DataFrame(market_data[stock_code], dtype=float)
        if not df.empty:
            df = df.set_index('time', drop=True)
            df.index = pd.to_datetime(df.index)
            return df
        else:
            print(f"Empty dataframe for {stock_code}")
            return df
            
    def get_stock_data(self, stock_code):
        """Get data for a specific stock with caching"""
        if stock_code not in self._data_cache:
            self._data_cache[stock_code] = self._process_stock_data(stock_code)
        return self._data_cache[stock_code]

    def data_generator(self):
        """Generate data lazily, loading only when needed"""
        for stock_code in self.stock_list:
            df = self.get_stock_data(stock_code)
            if not df.empty:
                for index, row in df.iterrows():
                    yield {stock_code: row}

    def get_all_data(self):
        """Get all data, loading only when needed"""
        result = {}
        for stock_code in self.stock_list:
            df = self.get_stock_data(stock_code)
            if not df.empty:
                for index, row in df.iterrows():
                    result[stock_code] = row
        return result
        
    # @property
    # def data(self):
    #     """Backward compatibility for existing code"""
    #     if not self._data_cache:
    #         for stock_code in self.stock_list:
    #             self.get_stock_data(stock_code)
    #     return self._data_cache
