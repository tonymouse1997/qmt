import logging
from feed.qmt_feed import QMTDataFeed

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # 输出到控制台
        logging.FileHandler('download.log')  # 输出到文件
    ]
)

data_fetcher = QMTDataFeed()

data_fetcher.download_history_data(period='tick')