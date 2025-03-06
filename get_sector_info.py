from xtquant import xtdata
import pandas as pd
import os

def get_sector_info():
    # Get all A-share stock codes
    stock_list = xtdata.get_stock_list_in_sector('沪深A股')
    
    # Create data directory if not exists
    os.makedirs('a_share_data/sector_info', exist_ok=True)
    
    # Fetch sector info for each stock
    sector_data = []
    for stock in stock_list:
        try:
            # Get sector information
            sector_info = xtdata.get_stock_sector_info(stock)
            if sector_info:
                sector_data.append({
                    'stock_code': stock,
                    'sector': sector_info['sector'],
                    'industry': sector_info['industry']
                })
        except Exception as e:
            print(f'Error fetching sector info for {stock}: {str(e)}')
    
    # Convert to DataFrame and save
    df = pd.DataFrame(sector_data)
    df.to_csv('a_share_data/sector_info/sector_info.csv', index=False)
    print('Sector info data saved successfully')

if __name__ == '__main__':
    pass