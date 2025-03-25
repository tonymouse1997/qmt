import pandas as pd
import logging
from datetime import datetime
from core.data_feed import DataFeed
import xtdata

class QMTDataFeed(DataFeed):
    """QMT数据源实现"""
    
    def __init__(self, avg_turnover_days=5):
        """
        初始化数据源
        :param avg_turnover_days: 计算平均成交额的天数
        """
        self.avg_turnover_days = avg_turnover_days
        self._xtdata = xtdata  # 使用组合方式存储xtdata实例

    def __getattr__(self, name):
        """
        当访问不存在的属性时，尝试从xtdata中获取
        这样可以动态访问xtdata的所有方法，而不需要预先复制
        """
        if hasattr(self._xtdata, name):
            return getattr(self._xtdata, name)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def _get_sector_info(self):
        """获取板块信息"""
        return {sector_name: self._xtdata.get_stock_list_in_sector(sector_name) 
                for sector_name in self._xtdata.get_sector_list() 
                if 'TGN' in sector_name or 'THY' in sector_name 
                and '季报' not in sector_name and '年报' not in sector_name}

    def _get_sector_df(self, instrument_type='stock'):
        """获取板块DataFrame"""
        sector_dict = self._get_sector_info()
        data = [(sector, stock) for sector, stocks in sector_dict.items() for stock in stocks]
        return pd.DataFrame(data, columns=['sector', 'stock_code'])

    def _get_sectors_of_stocks(self, instrument_type='stock'):
        """获取股票所属板块信息"""
        sector_df = self._get_sector_df(instrument_type)
        merged_df = sector_df.groupby('stock_code')['sector'].agg(list).reset_index()
        return dict(zip(merged_df['stock_code'], merged_df['sector']))

    def get_market_cap(self, stock_code):
        """获取股票市值"""
        try:
            float_vol = self._xtdata.get_instrument_detail(stock_code)["FloatVolume"]
            last_price = self._xtdata.get_full_tick([stock_code])[stock_code]['lastPrice']
            return float_vol * last_price
        except Exception as e:
            logging.error(f"获取股票 {stock_code} 市值失败: {str(e)}")
            return 0.0

    def get_basic_info_df(self):
        """获取并验证基础数据"""
        try:
            # 获取所有A股股票代码
            stock_list = self.get_sector_stocks('沪深A股')
            if not stock_list:
                logging.error("获取股票列表失败")
                return pd.DataFrame()
            
            # 一次性获取所有股票的详细信息
            details = {}
            for stock in stock_list:
                try:
                    detail = self._xtdata.get_instrument_detail(stock)
                    if self._validate_stock_data(stock, detail):
                        details[stock] = detail
                except Exception as e:
                    logging.error(f"获取股票 {stock} 详细信息失败: {str(e)}")
                    continue
            
            if not details:
                logging.error("没有获取到有效的股票详细信息")
                return pd.DataFrame()
            
            # 一次性获取所有股票的行情数据
            try:
                ticks = self._xtdata.get_full_tick(list(details.keys()))
            except Exception as e:
                logging.error(f"获取行情数据失败: {str(e)}")
                return pd.DataFrame()
            
            # 构建数据
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
            df['float_amount'] = df['float_volume'] * df['last_price']
            
            # 验证DataFrame
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

    def get_turnover_data(self, stock_list):
        """
        批量获取股票的成交额数据
        :param stock_list: 股票代码列表
        :return: dict, key为股票代码,value为DataFrame包含成交额数据
        """
        try:
            data = self._xtdata.get_market_data_ex(
                stock_list=stock_list,
                start_time=(datetime.now() - timedelta(days=self.avg_turnover_days)).strftime('%Y%m%d'),
                end_time=datetime.now().strftime('%Y%m%d'),
                period='1d',
                field_list=['close', 'volume']
            )
            return data
        except Exception as e:
            logging.error(f"批量获取成交额数据失败: {str(e)}")
            return {}

    def calculate_avg_turnover(self, stock_list):
        """
        批量计算股票的平均成交额
        :param stock_list: 股票代码列表
        :return: dict, key为股票代码,value为平均成交额
        """
        try:
            data_dict = self.get_turnover_data(stock_list)
            result = {}
            
            for stock_code in stock_list:
                try:
                    if stock_code not in data_dict:
                        result[stock_code] = 0.0
                        continue
                        
                    df = data_dict[stock_code]
                    if df is None or df.empty:
                        result[stock_code] = 0.0
                        continue

                    df['turnover'] = df['close'] * df['volume']
                    result[stock_code] = df['turnover'].mean()
                    
                except Exception as e:
                    logging.error(f"计算股票 {stock_code} 的平均成交额时发生错误: {str(e)}")
                    result[stock_code] = 0.0
                    
            return result

        except Exception as e:
            logging.error(f"批量计算平均成交额时发生错误: {str(e)}")
            return {stock_code: 0.0 for stock_code in stock_list}

    def get_single_stock_turnover(self, stock_code):
        """
        获取单只股票的平均成交额（使用缓存）
        :param stock_code: 股票代码
        :return: float 平均成交额
        """
        try:
            # 检查缓存是否存在
            if not hasattr(self, '_turnover_cache'):
                self._turnover_cache = {}
            if not hasattr(self, '_last_update_date'):
                self._last_update_date = None

            # 检查缓存是否需要更新
            if (stock_code not in self._turnover_cache or 
                self._last_update_date != datetime.now().date()):
                # 批量计算成交额
                new_data = self.calculate_avg_turnover([stock_code])
                self._turnover_cache.update(new_data)
                self._last_update_date = datetime.now().date()
            
            return self._turnover_cache.get(stock_code, 0.0)
            
        except Exception as e:
            logging.error(f"获取股票 {stock_code} 的平均成交额失败: {str(e)}")
            return 0.0

    def get_batch_turnover(self, stock_list):
        """
        批量获取股票的平均成交额（使用缓存）
        :param stock_list: 股票代码列表
        :return: dict, key为股票代码,value为平均成交额
        """
        try:
            # 检查缓存是否存在
            if not hasattr(self, '_turnover_cache'):
                self._turnover_cache = {}
            if not hasattr(self, '_last_update_date'):
                self._last_update_date = None

            # 检查缓存是否需要更新
            if self._last_update_date != datetime.now().date():
                # 批量计算成交额
                new_data = self.calculate_avg_turnover(stock_list)
                self._turnover_cache.update(new_data)
                self._last_update_date = datetime.now().date()
            
            # 返回所有请求的股票的成交额
            return {stock: self._turnover_cache.get(stock, 0.0) for stock in stock_list}
            
        except Exception as e:
            logging.error(f"批量获取股票平均成交额失败: {str(e)}")
            return {stock: 0.0 for stock in stock_list}

    def clear_turnover_cache(self):
        """清除成交额缓存"""
        if hasattr(self, '_turnover_cache'):
            self._turnover_cache.clear()
        if hasattr(self, '_last_update_date'):
            self._last_update_date = None

    def get_sector_stocks(self, sector):
        """
        获取板块内的股票列表
        :param sector: 板块名称
        :return: list 股票代码列表
        """
        try:
            return self._xtdata.get_stock_list_in_sector(sector)
        except Exception as e:
            logging.error(f"获取板块 {sector} 的股票列表失败: {str(e)}")
            return []

    def get_market_data(self, stock_list, start_date, end_date, period='tick', fields=None):
        """
        获取市场数据
        :param stock_list: 股票代码列表
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param period: 周期
        :param fields: 字段列表
        :return: dict, key为股票代码,value为DataFrame
        """
        try:
            if fields is None:
                fields = ['lastPrice', 'volume', 'amount']
            return self._xtdata.get_market_data_ex(
                stock_list=stock_list,
                start_time=start_date,
                end_time=end_date,
                period=period,
                field_list=fields
            )
        except Exception as e:
            logging.error(f"获取市场数据失败: {str(e)}")
            return {}

    def get_tick_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取tick数据"""
        try:
            data = self.get_market_data(
                stock_list=[stock_code],
                start_date=start_date,
                end_date=end_date,
                period='tick'
            )
            return data.get(stock_code, pd.DataFrame())
        except Exception as e:
            logging.error(f"获取股票 {stock_code} 的tick数据失败: {str(e)}")
            return pd.DataFrame() 