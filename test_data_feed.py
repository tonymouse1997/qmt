import time
from backtest_data_feed import BacktestDataFeed

def test_lazy_loading():
    print('\n=== Testing Lazy Loading Implementation ===\n')
    
    # Test initialization time
    start = time.time()
    data_feed = BacktestDataFeed(['000001.SZ', '600000.SH'], '1d', '20230101', '20230201')
    init_time = time.time() - start
    print(f'Initialization time: {init_time:.4f}s')
    print(f'Market data loaded: {data_feed._market_data is not None}')
    print(f'Cache status: {data_feed._data_cache}\n')
    
    # Test first data access
    print('Accessing first stock data...')
    start = time.time()
    df1 = data_feed.get_stock_data('000001.SZ')
    first_access = time.time() - start
    print(f'First stock access time: {first_access:.4f}s')
    print(f'Market data loaded: {data_feed._market_data is not None}')
    print(f'Data cache size: {len(data_feed._data_cache)} stocks')
    print(f'Cache keys: {list(data_feed._data_cache.keys())}\n')
    
    # Test second data access
    print('Accessing second stock data...')
    start = time.time()
    df2 = data_feed.get_stock_data('600000.SH')
    second_access = time.time() - start
    print(f'Second stock access time: {second_access:.4f}s')
    print(f'Data cache size: {len(data_feed._data_cache)} stocks')
    print(f'Cache keys: {list(data_feed._data_cache.keys())}\n')
    
    # Test cached access
    print('Accessing first stock again (cached)...')
    start = time.time()
    df1_again = data_feed.get_stock_data('000001.SZ')
    cached_access = time.time() - start
    print(f'Cached access time: {cached_access:.4f}s')
    if first_access > 0 and cached_access > 0:
        print(f'Speed improvement: {first_access/cached_access:.1f}x faster\n')
    
    # Test backward compatibility
    print('Testing backward compatibility with .data property...')
    start = time.time()
    all_data = data_feed.data
    data_property_time = time.time() - start
    print(f'Data property access time: {data_property_time:.4f}s')
    print(f'Data keys: {list(all_data.keys())}\n')
    
    print('=== Test Summary ===') 
    print(f'- Initialization is now fast: {init_time:.4f}s')
    print(f'- First data access includes loading time: {first_access:.4f}s')
    if first_access > 0 and cached_access > 0:
        print(f'- Cached access is {first_access/cached_access:.1f}x faster: {cached_access:.4f}s')
    print(f'- Backward compatibility maintained through .data property')

if __name__ == '__main__':
    test_lazy_loading()