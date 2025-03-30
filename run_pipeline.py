import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from strategies.pipeline import MultiFactorPipeline
from feed.qmt_feed import QMTDataFeed

def main():
    # 创建数据源
    data_feed = QMTDataFeed()
    
    # 下载历史数据
    print("开始下载历史数据...")
    data_feed.download_history_data(
        sector_name='沪深A股',
        period='tick',
        start_time='20240101',
        end_time='20240329'
    )
    print("历史数据下载完成")
    
    # 创建pipeline
    pipeline = MultiFactorPipeline(data_feed=data_feed)
    
    # 训练模型
    evaluation_report = pipeline.train(
        start_date="20240101",
        end_date="20240328",
        symbols="000001.SZ",
        target_time="10:00:00"
    )
    
    # 输出结果
    print("训练评估报告:", evaluation_report)

if __name__ == "__main__":
    main() 