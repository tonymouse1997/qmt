from xtquant import xtdata
import pandas as pd
from datetime import datetime
import os
    
def get_sector_df():
    # 获取板块和股票代码的字典
    sector_dict = {sector_name: xtdata.get_stock_list_in_sector(sector_name) for sector_name in xtdata.get_sector_list('沪深A股')}
    # 直接从字典创建DataFrame
    return pd.DataFrame.from_dict(sector_dict,orient='index').T
def get_sector_info(stock_codes):
    """获取股票所属板块信息
    
    Args:
        stock_codes (str or list): 股票代码或股票代码列表
        
    Returns:
        dict: 包含每个股票板块信息的字典，格式为：
            {
                'stock_code': {
                    'industry': str,  # 所属行业
                    'concept': list   # 概念板块列表
                }
            }
    """
    if isinstance(stock_codes, str):
        stock_codes = [stock_codes]
        
    result = {}
    for stock in stock_codes:
        try:
            # 获取行业信息
            industry = xtdata.get_stock_industry(stock)
            
            # 获取概念板块信息
            concepts = xtdata.get_stock_concept(stock)
            
            if industry or concepts:
                result[stock] = {
                    'industry': industry[0] if industry else '',
                    'concept': concepts if concepts else []
                }
                
        except Exception as e:
            print(f'获取{stock}的板块信息时出错：{str(e)}')
            continue
            
    return result
    # 实现从QMT获取数据的逻辑
    pass
    
def get_tick_data(self, stock_codes):
    """获取股票的tick数据"""
    # 实现从QMT获取数据的逻辑
    pass

def get_market_cap_data(stock_code):
    # Get all A-share stock codes # 股票代码
    floatVol = xtdata.get_instrument_detail(stock_code)["FloatVolume"]
    lastPrice = xtdata.get_full_tick([stock_code])[stock_code]['lastPrice']
    floatAmount = floatVol * lastPrice
    return floatAmount

if __name__ == '__main__':
    print(get_market_cap_data('600051.SH'))