from xtquant import xtdata
from xtquant.xttrader import XtQuantTrader
from datetime import datetime, time

class ScalpingStrategy:
    def __init__(self, market_cap_min):
        self.trader = XtQuantTrader()  # 初始化交易接口
        self.positions = {}            # 持仓记录
        self.trade_log = []            # 交易日志
        self.blacklist = set()         # 暂停交易股票池
        self.market_cap_min = market_cap_min
        
    #------------------ 核心逻辑 ------------------#
    def select_stocks(self):
        """
        选股模块（每日9:25-10:30运行）
        """
        candidates = []
        all_stocks = xtdata.get_stock_list_in_sector('沪深A股')
        
        for stock in all_stocks:
            # 基础筛选
            if self._is_valid_stock(stock):
                # 获取实时数据
                tick = xtdata.get_full_tick([stock])[stock]
                if self._is_qualified_limit_up(stock, tick):
                    candidates.append(stock)
        
        # 板块效应筛选
        final_picks = []
        for stock in candidates:
            if self._check_sector_strength(stock):
                final_picks.append(stock)
        
        return final_picks[:5]  # 每日最多5只

    def execute_trading(self):
        """
        交易执行模块
        """
        # 买入逻辑
        if datetime.now().time() < time(10,30):
            for stock in self.select_stocks():
                if self._position_available():
                    self._place_order(stock, 'buy')

        # 卖出逻辑
        for stock in list(self.positions.keys()):
            self._execute_sell_rules(stock)

    #------------------ 工具函数 ------------------#
    def _is_valid_stock(self, stock):
        """基础筛选条件"""
        info = xtdata.get_instrument_detail(stock)
        # 排除ST/次新/高价股
        if 'ST' in info['stock_name']: return False
        if (datetime.now() - info['list_date']).days < 365: return False
        if xtdata.get_market_data([], [stock], 'price')[-1][0] > 20: return False
        # 流通市值过滤
        float_mv = xtdata.get_market_data(['float_market_value'], [stock], period='1d')[-1][0]
        return 30e8 <= float_mv <= 100e8

    def _is_qualified_limit_up(self, stock, tick):
        """涨停板质量判断"""
        # 涨停时间判断
        if tick['time'] > '10:30:00': return False
        
        # 封单强度
        limit_price = round(xtdata.get_market_data([], [stock], 'pre_close')[-1][0] * 1.1, 2)
        order_vol = tick['bidVol1']  # 买一量
        order_amount = order_vol * limit_price
        daily_amount = tick['amount']  # 当日成交额
        return order_amount > daily_amount * 0.2

    def _execute_sell_rules(self, stock):
        """卖出规则执行"""
        open_price = xtdata.get_market_data([], [stock], 'open', period='1m')[-1][0]
        position = self.positions[stock]

        # 高开处理
        if open_price / position['cost'] > 1.03:
            self._place_order(stock, 'sell', position['qty']//2)
            # 剩余部分观察10分钟...

        # 低开止损
        elif open_price / position['cost'] < 0.98:
            self._place_order(stock, 'sell', position['qty'])

    #------------------ 风控模块 ------------------#
    def risk_management(self):
        """每日收盘后执行"""
        # 单笔止损
        for trade in self.trade_log[-1]:
            if trade['pnl'] < -0.02:
                self.blacklist.add(trade['stock'])
                
        # 月度风控
        if self._calc_monthly_drawdown() > 0.1:
            self.stop_trading()

    def _place_order(self, stock, side, qty=None):
        account = self.trader.get_account()
        if not qty:
            cash = account.cash
            price = xtdata.get_price(stock)[-1]
            qty = min(cash * 0.1 // price, 10000)  # 单票≤10%仓位
            
        order = {
            'stock_code': stock,
            'order_type': 'LIMIT',
            'price': xtdata.get_price(stock)[-1],
            'quantity': qty
        }
        self.trader.order(**order)

    def _check_sector_strength(self, stock):
        # 获取同板块股票
        sector = xtdata.get_stock_sector(stock)
        sector_stocks = xtdata.get_stock_list_in_sector(sector)
        
        # 统计涨停数量
        limit_up_count = 0
        for s in sector_stocks:
            if self._is_qualified_limit_up(s):
                limit_up_count += 1
        return limit_up_count >= 3

param_grid = {
'封单比例': [0.15, 0.2, 0.25],
'卖出观察时长': [10, 15, 20],  # 分钟
'板块涨停数阈值': [3, 5]
}

# 实时监控面板
monitor_params = {
    '当日交易数': len(self.trade_log),
    '当前持仓': self.positions,
    '暂停交易列表': self.blacklist,
    '当月回撤': self._calc_monthly_drawdown()
}

# 使用DQN动态调整参数
state = [market_volatility, sector_hot, account_balance]
action = self.dqn.predict(state)  # 输出仓位比例/卖出时机

# 分析Level2数据
order_book = xtdata.get_order_book(stock)
imbalance = order_book['ask_vol'] / order_book['bid_vol']