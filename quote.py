import sys

print("Python 版本：", sys.version)


import time
import pandas as pd
from xtquant import xtdatacenter as xtdc
from xtquant import xtdata
'''  
设置用于登录行情服务的token，此接口应该先于 init_quote 调用

token可以从投研用户中心获取
https://xuntou.net/#/userInfo
'''
xtdc.set_token('4e195837c5a3836c48c1a6d64ad4ee4caab74c4f')

'''
设置连接池,使服务器只在连接池内优选

建议将VIP服务器设为连接池
'''
addr_list = [
    '115.231.218.73:55310', 
    '115.231.218.79:55310', 
    '42.228.16.211:55300',
    '42.228.16.210:55300',
    '36.99.48.20:55300',
    '36.99.48.21:55300'
    ]
xtdc.set_allow_optmize_address(addr_list)

xtdc.set_kline_mirror_enabled(True) # 开启K线全推功能(vip),以获取全市场实时K线数据


"""
初始化
"""
xtdc.init()
## 监听端口
port = xtdc.listen(port = 58621) # 指定固定端口进行连接
# port = xtdc.listen(port = (58620, 58630))[1] 通过指定port范围，可以让xtdc在范围内自动寻找可用端口

xtdata.connect(port=port)

print('-----连接上了------')
print(xtdata.data_dir)



servers = xtdata.get_quote_server_status()
# print(servers)
for k, v in servers.items():
    print(k, v)

xtdata.run()