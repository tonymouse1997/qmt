from _01_data_processor import DataProcessor
from _02_factor_builder import FactorBuilder
from _03_feature_selector import FeatureSelector
from _04_model_trainer import ModelTrainer
from _05_model_evaluator import ModelEvaluator
from _06_predictor import Predictor
from _08_multi_factor_pipeline import MultiFactorPipeline
from _09_qmt_data_feed import QMTDataFeed

if __name__ == "__main__":
    # 创建数据源
    data_feed = QMTDataFeed()
    
    # 创建pipeline
    pipeline = MultiFactorPipeline(data_feed=data_feed)
    
    # 训练模型
    evaluation_report = pipeline.train(
        start_date="2023-01-01",
        end_date="2023-12-31",
        frequencies=['tick', 'daily'],
        symbols=['000001.SZ', '600000.SH'],
        target_time="10:00:00"
    )
    
    # 输出结果
    print("训练评估报告:", evaluation_report) 