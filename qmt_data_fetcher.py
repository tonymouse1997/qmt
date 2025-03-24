from xtquant import xtdata
import pandas as pd
from datetime import datetime
import os
import logging
    
def validate_stock_data(stock_code: str, data: dict) -> bool:
    """验证单个股票数据的有效性"""
    required_fields = ['FloatVolume', 'PreClose', 'UpStopPrice', 'DownStopPrice', 'OpenDate']
    
    # 检查必需字段是否存在
    for field in required_fields:
        if field not in data:
            logging.error(f"股票 {stock_code} 缺少必需字段 {field}")
            return False
            
    # 检查数值的有效性
    if data['FloatVolume'] <= 0 or data['PreClose'] <= 0:
        logging.error(f"股票 {stock_code} 的流通量或收盘价无效")
        return False
        
    if data['UpStopPrice'] <= data['PreClose'] or data['DownStopPrice'] >= data['PreClose']:
        logging.error(f"股票 {stock_code} 的涨跌停价格异常")
        return False
        
    return True

def validate_sector_data(sector_name: str, stock_list: list) -> bool:
    """验证板块数据的有效性"""
    if not stock_list:
        logging.error(f"板块 {sector_name} 没有包含任何股票")
        return False
        
    if len(stock_list) < 2:
        logging.warning(f"板块 {sector_name} 包含的股票数量过少")
        
    return True

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
    """获取并验证基础数据"""
    try:
        # Get all A-share stock codes
        stock_list = xtdata.get_stock_list_in_sector('沪深A股')
        if not stock_list:
            logging.error("获取股票列表失败")
            return pd.DataFrame()
        
        # 一次性获取所有股票的详细信息
        details = {}
        for stock in stock_list:
            try:
                detail = xtdata.get_instrument_detail(stock)
                if validate_stock_data(stock, detail):
                    details[stock] = detail
            except Exception as e:
                logging.error(f"获取股票 {stock} 详细信息失败: {str(e)}")
                continue
        
        if not details:
            logging.error("没有获取到有效的股票详细信息")
            return pd.DataFrame()
        
        # 一次性获取所有股票的行情数据
        try:
            ticks = xtdata.get_full_tick(list(details.keys()))
        except Exception as e:
            logging.error(f"获取行情数据失败: {str(e)}")
            return pd.DataFrame()
        
        # 使用列表推导式构建数据
        data = {
            'stock_code': list(details.keys()),
            'float_volume': [details[stock]["FloatVolume"] for stock in details],
            'last_price': [details[stock]['PreClose'] for stock in details],
            'limit_up_price': [details[stock]['UpStopPrice'] for stock in details],
            'limit_down_price': [details[stock]['DownStopPrice'] for stock in details],
            'list_date': [details[stock]['OpenDate'] for stock in details]
        }
        
        # 创建DataFrame
        df = pd.DataFrame(data)
        
        # 使用vectorized操作计算流通市值
        df['float_amount'] = df['float_volume'] * df['last_price']
        
        # 验证DataFrame的有效性
        if df.empty:
            logging.error("生成的DataFrame为空")
            return pd.DataFrame()
            
        if df.isnull().any().any():
            logging.warning("DataFrame中包含空值")
            df = df.dropna()
            
        return df
        
    except Exception as e:
        logging.error(f"获取基础数据时发生错误: {str(e)}")
        return pd.DataFrame()

if __name__ == '__main__':
    print(download_market_cap('600051.SH'))