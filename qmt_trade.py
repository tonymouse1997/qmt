#coding=utf-8
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount
from xtquant import xtconstant


class MyXtQuantTraderCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        """
        连接断开
        :return:
        """
        print("connection lost")
    def on_stock_order(self, order):
        """
        委托回报推送
        :param order: XtOrder对象
        :return:
        """
        print("on order callback:")
        print(order.stock_code, order.order_status, order.order_sysid)
    def on_stock_trade(self, trade):
        """
        成交变动推送
        :param trade: XtTrade对象
        :return:
        """
        print("on trade callback")
        print(trade.account_id, trade.stock_code, trade.order_id)
    def on_order_error(self, order_error):
        """
        委托失败推送
        :param order_error:XtOrderError 对象
        :return:
        """
        print("on order_error callback")
        print(order_error.order_id, order_error.error_id, order_error.error_msg)
    def on_cancel_error(self, cancel_error):
        """
        撤单失败推送
        :param cancel_error: XtCancelError 对象
        :return:
        """
        print("on cancel_error callback")
        print(cancel_error.order_id, cancel_error.error_id, cancel_error.error_msg)
    def on_order_stock_async_response(self, response):
        """
        异步下单回报推送
        :param response: XtOrderResponse 对象
        :return:
        """
        print("on_order_stock_async_response")
        print(response.account_id, response.order_id, response.seq)
    def on_account_status(self, status):
        """
        :param response: XtAccountStatus 对象
        :return:
        """
        print("on_account_status")
        print(status.account_id, status.account_type, status.status)

class QmtTrade:
    def __init__(self, path, session_id, account_id):
        self.path = path
        self.session_id = session_id
        self.account_id = account_id
        self.xt_trader = XtQuantTrader(path, session_id)
        self.acc = StockAccount(account_id)
        self.callback = MyXtQuantTraderCallback()
        self.xt_trader.register_callback(self.callback)
        self.xt_trader.start()
        self.connect_result = self.xt_trader.connect()
        print(self.connect_result)
        self.subscribe_result = self.xt_trader.subscribe(self.acc)
        print(self.subscribe_result)

    def order_stock(self, stock_code, volume, price_type, price, strategy_name, remark):
        print("order using the fix price:")
        fix_result_order_id = self.xt_trader.order_stock(self.acc, stock_code, xtconstant.STOCK_BUY, volume, price_type, price, strategy_name, remark)
        print(fix_result_order_id)
        return fix_result_order_id

    def cancel_order(self, order_id):
        print("cancel order:")
        cancel_order_result = self.xt_trader.cancel_order_stock(self.acc, order_id)
        print(cancel_order_result)
        return cancel_order_result

    def order_stock_async(self, stock_code, volume, price_type, price, strategy_name, remark):
        print("order using async api:")
        async_seq = self.xt_trader.order_stock_async(self.acc, stock_code, xtconstant.STOCK_BUY, volume, price_type, price, strategy_name, remark)
        print(async_seq)
        return async_seq

    def query_asset(self):
        print("query asset:")
        asset = self.xt_trader.query_stock_asset(self.acc)
        if asset:
            print("asset:")
            print("cash {0}".format(asset.cash))
        return asset

    def query_order(self, order_id):
        print("query order:")
        order = self.xt_trader.query_stock_order(self.acc, order_id)
        if order:
            print("order:")
            print("order {0}".format(order.order_id))
        return order

    def query_orders(self):
        print("query orders:")
        orders = self.xt_trader.query_stock_orders(self.acc)
        print("orders:", len(orders))
        if len(orders) != 0:
            print("last order:")
            print("{0} {1} {2}".format(orders[-1].stock_code, orders[-1].order_volume, orders[-1].price))
        return orders

    def query_trades(self):
        print("query trade:")
        trades = self.xt_trader.query_stock_trades(self.acc)
        print("trades:", len(trades))
        if len(trades) != 0:
            print("last trade:")
            print("{0} {1} {2}".format(trades[-1].stock_code, trades[-1].traded_volume, trades[-1].traded_price))
        return trades

    def query_positions(self):
        print("query positions:")
        positions = self.xt_trader.query_stock_positions(self.acc)
        print("positions:", len(positions))
        if len(positions) != 0:
            print("last position:")
            print("{0} {1} {2}".format(positions[-1].account_id, positions[-1].stock_code, positions[-1].volume))
        return positions

    def query_position(self, stock_code):
        print("query position:")
        position = self.xt_trader.query_stock_position(self.acc, stock_code)
        if position:
            print("position:")
            print("{0} {1} {2}".format(position.account_id, position.stock_code, position.volume))
        return position

    def run_forever(self):
        self.xt_trader.run_forever()

if __name__ == "__main__":
    print("demo test")
    # path为mini qmt客户端安装目录下userdata_mini路径
    path = r'C:\国金证券QMT交易端\userdata_mini'
    # session_id为会话编号，策略使用方对于不同的Python策略需要使用不同的会话编号
    session_id = 123456
    account_id = '8886671220'
    qmt_trade = QmtTrade(path, session_id, account_id)
    stock_code = '600000.SH'
    # 使用指定价下单，接口返回订单编号，后续可以用于撤单操作以及查询委托状态
    fix_result_order_id = qmt_trade.order_stock(stock_code, 200, xtconstant.FIX_PRICE, 10.5, 'strategy_name', 'remark')
    # 使用订单编号撤单
    qmt_trade.cancel_order(fix_result_order_id)
    # 使用异步下单接口，接口返回下单请求序号seq，seq可以和on_order_stock_async_response的委托反馈response对应起来
    qmt_trade.order_stock_async(stock_code, 200, xtconstant.FIX_PRICE, 10.5, 'strategy_name', 'remark')
    # 查询证券资产
    qmt_trade.query_asset()
    # 根据订单编号查询委托
    qmt_trade.query_order(fix_result_order_id)
    # 查询当日所有的委托
    qmt_trade.query_orders()
    # 查询当日所有的成交
    qmt_trade.query_trades()
    # 查询当日所有的持仓
    qmt_trade.query_positions()
    # 根据股票代码查询对应持仓
    qmt_trade.query_position(stock_code)
    # 阻塞线程，接收交易推送
    qmt_trade.run_forever()
