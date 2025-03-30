import pandas as pd
def is_kcb(stock_code):
    """判断是否为科创板股票"""
    return stock_code.startswith('688')

def is_st(stock_code):
    """判断是否为ST股票"""
    return stock_code.startswith('ST') or stock_code.startswith('*ST')

def is_bj(stock_code):
    """判断是否为北交所股票"""
    return stock_code.startswith('43')

if __name__ == '__main__':
    # 示例用法
    stock_code = '600000.SH'  # 示例股票代码
    current_price = 10.0  # 示例当前价格

    is_kcb_stock = is_kcb(stock_code)
    is_limit_up_stock = is_limit_up(stock_code, current_price)

    print(f"{stock_code} 是否为科创板股票: {is_kcb_stock}")
    print(f"{stock_code} 是否涨停: {is_limit_up_stock}")
