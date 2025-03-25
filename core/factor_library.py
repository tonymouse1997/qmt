import pandas as pd
from qmt_data_fetcher import get_basic_info_df

basic_info_df = get_basic_info_df()

def is_kcb(stock_code):
    """判断是否为科创板股票"""
    return stock_code.startswith('688')

def is_limit_up(stock_code, current_price):
    """判断股票是否涨停"""
    limit_up_price = basic_info_df.loc[stock_code, 'limit_up_price']
    return current_price == limit_up_price

def is_st(stock_code):
    """判断是否为ST股票"""
    return stock_code.startswith('ST') or stock_code.startswith('*ST')

def is_bj(stock_code):
    """判断是否为北交所股票"""
    return stock_code.startswith('43')

def get_float_amount(stock_code):
    """获取流通市值"""
    return basic_info_df.loc[stock_code, 'float_amount']

def get_limit_up_price(stock_code):
    """获取涨停价"""
    return basic_info_df.loc[stock_code, 'limit_up_price']

def is_limit_up(stock_code):
    #TODO: 实现判断是否涨停的函数
    return 


if __name__ == '__main__':
    # 示例用法
    stock_code = '600000.SH'  # 示例股票代码
    current_price = 10.0  # 示例当前价格

    is_kcb_stock = is_kcb(stock_code)
    is_limit_up_stock = is_limit_up(stock_code, current_price)

    print(f"{stock_code} 是否为科创板股票: {is_kcb_stock}")
    print(f"{stock_code} 是否涨停: {is_limit_up_stock}")
