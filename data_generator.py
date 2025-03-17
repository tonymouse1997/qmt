def generate_timestamp_data(data):
    '''
    从 xtdata.get_market_data_ex 返回的字典生成时间戳数据。

    Args:
        data (dict): xtdata.get_market_data_ex 返回的字典。

    Yields:
        tuple: (stock_code, timestamp_data) 的元组，其中 stock_code 是股票代码，timestamp_data 是时间戳数据。
    '''
    {stock_code: row for stock_code, df in data.items() for index, row in df.iterrows()}

