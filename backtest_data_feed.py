import pandas as pd
from xtquant import xtdata

class BacktestDataFeed:
    def __init__(self, stock_list: list[str], period: str, start_time: str, end_time: str, field_list: list[str] = None):
        self.stock_list = stock_list
        self.period = period
        self.start_time = start_time
        self.end_time = end_time
        self.field_list = field_list if field_list else []
        self.data = self._load_data()

    def _load_data(self) -> dict[str, pd.DataFrame]:
        data = {}
        try:
            market_data = xtdata.get_market_data_ex(
                field_list=self.field_list,
                stock_list=self.stock_list,
                period=self.period,
                start_time=self.start_time,
                end_time=self.end_time,
                count=-1,
                dividend_type='none',
                fill_data=True,
            )

            for stock_code in self.stock_list:
                df = pd.DataFrame(market_data[stock_code], dtype=float)
                if not df.empty:
                    df = df.set_index('time', drop=True)
                    df.index = pd.to_datetime(df.index)
                    data[stock_code] = df
                else:
                    print(f"No data found for {stock_code}")

        except Exception as e:
            print(f"Error loading data: {e}")
        return data

    def data_generator(self):
        for stock_code, df in self.data.items():
            for index, row in df.iterrows():
                yield {stock_code: row}

    def get_all_data(self):
        return {stock_code: row for stock_code, df in self.data.items() for index, row in df.iterrows()}
