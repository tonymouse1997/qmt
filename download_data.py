from xtquant import xtdata
import pandas as pd
import os
from datetime import datetime, timedelta
# Get all A-share stock codes
def download_A_share_data(sector_name='沪深A股', period='1d', start_time=(datetime.now() - timedelta(days=365)).strftime('%Y%m%d'), end_time=datetime.now().strftime('%Y%m%d')):
    start_time, end_time = ('', '') if period != 'tick' else start_time, end_time
    stock_list = xtdata.get_stock_list_in_sector(sector_name)
    # Create data directory
    os.makedirs('A_share_data', exist_ok=True)

    for stock in stock_list:
        xtdata.download_history_data(stock, period=period, start_time=start_time, end_time=end_time, incrementally = True)
        print(f'{stock} {period}数据下载完成')
    # # Fetch and save daily data for each stock
    # os.makedirs(f'a_share_data/{period}', exist_ok=True)
    # for stock_code in stock_list:
    #     try:
    #         # Get daily data
    #         data = xtdata.get_market_data_ex(
    #             stock_list=[stock_code],
    #             period=period,
    #             start_time='',
    #             end_time='',
    #             count=-1,
    #             dividend_type='none'
    #         )
            
    #         # Convert to DataFrame and save
    #         # Convert to DataFrame and handle multi-index
    #         if isinstance(data, dict):
    #             data[stock_code].to_csv(f'a_share_data/{period}/{stock_code}.csv', index=False, float_format='%.2f')
    #             print(f'{stock_code} data saved')
    #         else:
    #             print(f'Error fetching data for {stock_code}: {str(e)}')
    #             continue
            
    #     except Exception as e:
    #         print(f'Error fetching data for {stock_code}: {str(e)}')

    # print(f'All {sector_name} data downloaded successfully')

if __name__ == '__main__':
    download_A_share_data(sector_name='沪深A股', period='tick')