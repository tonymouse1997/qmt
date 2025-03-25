# QMT量化交易框架

这是一个基于QMT的量化交易框架，支持策略回测和实盘交易。

## 项目结构

```
qmt/
├── README.md                 # 项目说明文档
├── requirements.txt          # 项目依赖
├── config/                   # 配置文件目录
│   ├── __init__.py
│   └── settings.py          # 全局配置
├── core/                     # 核心功能模块
│   ├── __init__.py
│   ├── data_feed.py         # 数据源接口
│   ├── trade_interface.py   # 交易接口
│   └── strategy.py          # 策略基类
├── interfaces/              # 具体接口实现
│   ├── __init__.py
│   ├── backtrader/         # Backtrader相关实现
│   │   ├── __init__.py
│   │   ├── interface.py    # Backtrader交易接口
│   │   └── data_feed.py    # Backtrader数据适配器
│   └── qmt/                # QMT相关实现
│       ├── __init__.py
│       └── data_feed.py    # QMT数据源实现
├── strategies/             # 具体策略实现
│   ├── __init__.py
│   └── sector_chase.py     # 板块轮动策略
├── utils/                  # 工具函数
│   ├── __init__.py
│   └── logger.py          # 日志工具
└── main.py                # 主程序入口
```

## 功能特点

1. 模块化设计
   - 数据源接口与实现分离
   - 交易接口与实现分离
   - 策略逻辑独立封装

2. 可扩展性
   - 支持添加新的数据源
   - 支持添加新的交易接口
   - 支持添加新的策略

3. 配置灵活
   - 集中管理配置参数
   - 支持不同环境配置

## 使用方法

1. 安装依赖
```bash
pip install -r requirements.txt
```

2. 运行回测
```bash
python main.py
```

## 开发新策略

1. 继承 `Strategy` 基类
2. 实现 `on_bar` 和 `on_tick` 方法
3. 使用 `data_feed` 获取数据
4. 使用 `trade_interface` 执行交易

## 注意事项

1. 确保QMT环境配置正确
2. 回测时注意数据质量和完整性
3. 实盘交易前充分测试策略 