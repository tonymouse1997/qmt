from xtquant import xtdata
import pandas as pd
from datetime import datetime
import os
    
def get_sector_info():
    return {sector_name: xtdata.get_stock_list_in_sector(sector_name) for sector_name in xtdata.get_sector_list() if 'TGN' in sector_name or 'THY' in sector_name and '季报' not in sector_name and '年报' not in sector_name}

def get_sector_df(instrument_type='stock'):
    # 获取板块和股票代码的字典
    sector_dict = get_sector_info()
    data = [(sector, stock) for sector, stocks in sector_dict.items() for stock in stocks] #展开成二维数组
    sector_df = pd.DataFrame(data, columns=['sector', 'stock_code'])
    return sector_df

def get_stock_list_in_sector(sector_name):
    return xtdata.get_stock_list_in_sector(sector_name)

# def download_stocks_in_sectors():   
#     return pd.DataFrame.from_dict(sector_dict,orient='index').T
def get_sectors_of_stocks(instrument_type='stock'):
    # 获取板块和股票代码的字典
    sector_df = get_sector_df(instrument_type)
    
    # 按stock_code分组，将sector合并为列表
    merged_df = sector_df.groupby('stock_code')['sector'].agg(list).reset_index()
    return merged_df
def get_market_cap(stock_code):
    # Get all A-share stock codes # 股票代码
    floatVol = xtdata.get_instrument_detail(stock_code)["FloatVolume"]
    lastPrice = xtdata.get_full_tick([stock_code])[stock_code]['lastPrice']
    floatAmount = floatVol * lastPrice
    return floatAmount

def get_basic_info_df():
    # Get all A-share stock codes
    stock_list = xtdata.get_stock_list_in_sector('沪深A股')
    
    # 一次性获取所有股票的详细信息
    details = {stock: xtdata.get_instrument_detail(stock) for stock in stock_list}
    
    # 一次性获取所有股票的行情数据
    ticks = xtdata.get_full_tick(stock_list)
    
    # 使用列表推导式构建数据（比循环添加到字典更快）
    data = {
        'stock_code': stock_list,
        'float_volume': [details[stock]["FloatVolume"] for stock in stock_list],
        'last_price': [details[stock]['PreClose'] for stock in stock_list],
        'limit_up_price': [details[stock]['UpStopPrice'] for stock in stock_list],
        'limit_down_price': [details[stock]['DownStopPrice'] for stock in stock_list],
        'list_date': [details[stock]['OpenDate'] for stock in stock_list]
    }
    
    # 创建DataFrame
    df = pd.DataFrame(data)
    
    # 使用vectorized操作计算流通市值
    df['float_amount'] = df['float_volume'] * df['last_price']
    
    return df

if __name__ == '__main__':
    print(download_market_cap('600051.SH'))