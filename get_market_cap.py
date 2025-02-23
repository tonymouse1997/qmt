from xtquant import xtdata
import pandas as pd
import os
from datetime import datetime

def get_market_cap_data():
    # Get all A-share stock codes
    stock_list = xtdata.get_stock_list_in_sector('沪深A股')
    
    # Create data directory if not exists
    os.makedirs('a_share_data/market_cap', exist_ok=True)
    
    # Fetch market cap data for each stock
    market_cap_data = []
    for stock in stock_list:
        try:
            # Get market cap data
            cap_data = xtdata.get_market_cap(stock)
            if cap_data:
                market_cap_data.append({
                    'stock_code': stock,
                    'market_cap': cap_data['total_market_cap']
                })
        except Exception as e:
            print(f'Error fetching market cap for {stock}: {str(e)}')
    
    # Convert to DataFrame and save
    df = pd.DataFrame(market_cap_data)
    df.to_csv('a_share_data/market_cap/market_cap.csv', index=False)
    print('Market cap data saved successfully')

if __name__ == '__main__':
    get_market_cap_data()
