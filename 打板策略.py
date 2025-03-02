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
        self.limit_up_history = {}     # 记录股票涨停历史
        
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
        # if xtdata.get_market_data([], [stock], 'price')[-1][0] > 20: return False
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
        
        # 首板判断
        if not self._is_first_limit_up(stock):
            return False
            
        # 封单强度判断 - 首板要求更高的封单比例
        return order_amount > daily_amount * self.params.get('首板封单比例', 0.3)

    def _execute_sell_rules(self, stock):
        """卖出规则执行"""
        current_price = xtdata.get_market_data([], [stock], 'price', period='1m')[-1][0]
        position = self.positions[stock]
        cost = position['cost']
        profit_ratio = (current_price / cost) - 1
        
        # 首板特殊卖出规则
        # 1. 止盈 - 首板涨幅达到设定比例时分批卖出
        if profit_ratio >= self.params.get('首板止盈比例', 0.05):
            # 分批卖出策略 - 达到5%卖出一半
            self._place_order(stock, 'sell', position['qty']//2)
            print(f"{stock} 首板止盈卖出一半，盈利: {profit_ratio:.2%}")
            
        # 2. 止损 - 首板回撤超过设定比例
        elif profit_ratio <= -self.params.get('首板止损比例', 0.03):
            # 全部卖出
            self._place_order(stock, 'sell', position['qty'])
            print(f"{stock} 首板止损卖出全部，亏损: {profit_ratio:.2%}")
            
        # 3. 尾盘清仓 - 首板当天必须清仓
        current_time = datetime.now().time()
        if current_time > time(14, 45) and position['qty'] > 0:
            self._place_order(stock, 'sell', position['qty'])
            print(f"{stock} 首板尾盘清仓")

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
        """下单函数
        Args:
            stock: 股票代码
            side: 交易方向，'buy'或'sell'
            qty: 交易数量，如果为None则自动计算
        """
        try:
            account = self.trader.get_account()
            price = xtdata.get_price(stock)[-1]
            
            if not qty and side == 'buy':
                cash = account.cash
                # 使用参数中设置的单笔资金比例
                single_order_ratio = self.params.get('单笔资金比例', 0.1)
                max_amount = self.params.get('最大单笔金额', 100000)
                
                # 计算数量，考虑资金比例和最大金额限制
                qty = min(int(cash * single_order_ratio // price), int(max_amount // price), 10000)  # 单票≤10%仓位且不超过最大金额
            
            order = {
                'stock_code': stock,
                'order_type': 'LIMIT',
                'price': price,
                'quantity': qty,
                'side': 'BUY' if side == 'buy' else 'SELL'
            }
            
            # 执行下单
            order_id = self.trader.order(**order)
            
            # 记录交易
            trade_record = {
                'date': datetime.now(),
                'stock': stock,
                'side': side,
                'price': price,
                'quantity': qty,
                'order_id': order_id
            }
            
            # 更新持仓信息
            if side == 'buy':
                if stock not in self.positions:
                    self.positions[stock] = {'qty': qty, 'cost': price}
                else:
                    # 计算新的持仓成本
                    old_qty = self.positions[stock]['qty']
                    old_cost = self.positions[stock]['cost']
                    new_qty = old_qty + qty
                    new_cost = (old_qty * old_cost + qty * price) / new_qty
                    self.positions[stock] = {'qty': new_qty, 'cost': new_cost}
            else:  # sell
                if stock in self.positions:
                    self.positions[stock]['qty'] -= qty
                    if self.positions[stock]['qty'] <= 0:
                        del self.positions[stock]
            
            print(f"下单成功: {side} {stock} {qty}股 价格:{price}")
            return order_id
        except Exception as e:
            print(f"下单失败: {side} {stock} - 错误: {str(e)}")
            return None

    def _is_first_limit_up(self, stock):
        """判断是否为首板"""
        try:
            # 获取最近10个交易日的数据
            df = xtdata.get_market_data(['close', 'high', 'low', 'pre_close'], [stock], start_time=None, end_time=None, period='1d', count=10)
            
            # 计算每日涨停价
            df['limit_price'] = df['pre_close'] * 1.1
            df['limit_price'] = df['limit_price'].apply(lambda x: round(x, 2))
            
            # 判断是否涨停（收盘价或最高价达到涨停价）
            df['is_limit_up'] = (df['close'] >= df['limit_price'] - 0.01) | (df['high'] >= df['limit_price'] - 0.01)
            
            # 检查最近10个交易日是否有涨停记录（不包括今天）
            historical_limit_ups = df['is_limit_up'].iloc[1:].sum()
            
            # 如果历史上没有涨停记录，则为首板
            return historical_limit_ups == 0
            
        except Exception as e:
            print(f"检查{stock}首板状态出错: {str(e)}")
            return False
            
    def _is_limit_up(self, stock, tick):
        """判断股票是否涨停（不考虑其他条件）"""
        try:
            # 计算涨停价
            limit_price = round(xtdata.get_market_data([], [stock], 'pre_close')[-1][0] * 1.1, 2)
            # 判断当前价格是否达到涨停价
            current_price = tick['last']
            return abs(current_price - limit_price) < 0.01
        except Exception as e:
            print(f"检查{stock}涨停状态出错: {str(e)}")
            return False
            
    def _check_sector_strength(self, stock):
        # 获取同板块股票
        sector = xtdata.get_stock_sector(stock)
        sector_stocks = xtdata.get_stock_list_in_sector(sector)
        
        # 统计涨停数量
        limit_up_count = 0
        for s in sector_stocks:
            try:
                # 获取实时数据
                tick = xtdata.get_full_tick([s])[s]
                if self._is_limit_up(s, tick):  # 使用新方法，只判断是否涨停
                    limit_up_count += 1
            except Exception as e:
                print(f"获取{s}数据出错: {str(e)}")
                continue
        return limit_up_count >= self.params.get('板块涨停数阈值', 3)

    def _position_available(self):
        """检查是否有可用仓位"""
        # 检查当前持仓数量是否已达上限
        max_positions = 5  # 最大持仓数量
        if len(self.positions) >= max_positions:
            return False
            
        # 检查可用资金是否足够
        account = self.trader.get_account()
        if account.cash < 100000:  # 确保至少有10万可用资金
            return False
            
        return True
        
    def _calc_monthly_drawdown(self):
        """计算当月最大回撤"""
        if not self.trade_log:
            return 0
            
        # 获取当月交易记录
        current_month = datetime.now().month
        monthly_trades = []
        for daily_trades in self.trade_log:
            for trade in daily_trades:
                if trade['date'].month == current_month:
                    monthly_trades.append(trade)
        
        if not monthly_trades:
            return 0
            
        # 计算累计收益曲线
        cumulative_returns = [0]
        for trade in monthly_trades:
            cumulative_returns.append(cumulative_returns[-1] + trade['pnl'])
            
        # 计算最大回撤
        max_return = 0
        max_drawdown = 0
        for ret in cumulative_returns:
            max_return = max(max_return, ret)
            drawdown = max_return - ret
            max_drawdown = max(max_drawdown, drawdown)
            
        # 相对回撤率
        initial_capital = 1000000  # 假设初始资金100万
        relative_drawdown = max_drawdown / initial_capital
        
        return relative_drawdown
        
    def stop_trading(self):
        """停止交易"""
        print("触发风控，停止交易")
        # 清空所有持仓
        for stock in list(self.positions.keys()):
            self._place_order(stock, 'sell', self.positions[stock]['qty'])
        # 标记停止交易状态
        self.trading_enabled = False
        
    def get_monitor_params(self):
        """获取监控参数"""
        return {
            '当日交易数': len(self.trade_log[-1]) if self.trade_log else 0,
            '当前持仓': self.positions,
            '暂停交易列表': self.blacklist,
            '当月回撤': self._calc_monthly_drawdown()
        }
        
    def set_parameters(self, params=None):
        """设置策略参数"""
        # 默认参数
        self.params = {
            '首板封单比例': 0.3,      # 首板要求更高的封单比例
            '卖出观察时长': 15,      # 分钟
            '板块涨停数阈值': 3,     # 板块强度判断
            '最大持仓数': 5,         # 最大持仓数量
            '单笔资金比例': 0.1,     # 占总资金比例
            '最大单笔金额': 100000,  # 元
            '首板止盈比例': 0.05,    # 首板止盈点位
            '首板止损比例': 0.03     # 首板止损点位
        }
        
        # 更新自定义参数
        if params:
            self.params.update(params)
            
    def analyze_level2_data(self, stock):
        """分析Level2数据"""
        try:
            order_book = xtdata.get_order_book(stock)
            # 计算买卖比例失衡度
            imbalance = order_book['bid_vol'] / (order_book['ask_vol'] + 1e-6)  # 避免除零
            return {
                'imbalance': imbalance,
                'order_book': order_book
            }
        except Exception as e:
            print(f"获取{stock}的Level2数据出错: {str(e)}")
            return None

# 示例使用
if __name__ == "__main__":
    # 初始化策略
    strategy = ScalpingStrategy(market_cap_min=30e8)
    
    # 设置自定义参数
    custom_params = {
        '封单比例': 0.25,
        '板块涨停数阈值': 5
    }
    strategy.set_parameters(custom_params)
    
    # 执行策略
    strategy.execute_trading()
    
    # 风控检查
    strategy.risk_management()