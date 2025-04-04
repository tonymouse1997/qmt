import pandas as pd
import numpy as np
from typing import Dict, Any, List
import logging
from ._01_data_processor import DataProcessor
from ._02_factor_builder import FactorBuilder
from ._03_feature_selector import FeatureSelector
from ._04_model_trainer import ModelTrainer
from ._05_model_evaluator import ModelEvaluator
from ._06_predictor import Predictor
from feed.qmt_feed import QMTDataFeed
from core.data_feed import DataFeed

class MultiFactorPipeline:
    def __init__(self, data_feed: DataFeed, threshold: float = 0.0):
        """
        初始化多因子预测流水线
        
        Args:
            data_feed: 数据源对象
            threshold: 预测阈值
        """
        self.logger = logging.getLogger(__name__)
        self.data_feed = data_feed
        
        # 初始化各个组件
        self.data_processor = DataProcessor(data_feed=data_feed)
        self.factor_builder = FactorBuilder()
        self.feature_selector = FeatureSelector()
        self.model_trainer = ModelTrainer()
        self.model_evaluator = ModelEvaluator()
        self.predictor = Predictor(self.model_trainer, threshold)
        
        # 注册默认因子
        self._register_default_factors()
        
    def _register_default_factors(self):
        """注册默认因子"""
        self.factor_builder.register_factor('technical', self.factor_builder.create_technical_factors)
        self.factor_builder.register_factor('price', self.factor_builder.create_price_factors)
        self.factor_builder.register_factor('volume', self.factor_builder.create_volume_factors)
        self.factor_builder.register_factor('microstructure', self.factor_builder.create_market_microstructure_factors)
        self.factor_builder.register_factor('combined', self.factor_builder.create_combined_factors)
        
    def train(self, 
              start_date: str, 
              end_date: str, 
              symbols: str,
              target_time: str='10:00:00',
              cv_splits: int=5):
        """训练模型
        
        Args:
            start_date: 训练数据开始日期
            end_date: 训练数据结束日期
            symbols: 股票代码
            target_time: 目标时间点
            cv_splits: 交叉验证折数
        """
        try:
            # 验证股票代码格式
            if not isinstance(symbols, str):
                raise ValueError("股票代码必须是字符串类型")
            
            # 1. 获取训练数据
            print("\n" + "="*50)
            print("阶段1: 获取训练数据")
            print("="*50)
            processed_data = self.data_processor.prepare_data(
                stock_list=[symbols],
                start_date=start_date,
                end_date=end_date
            )
            
            # 2. 构建因子
            print("\n" + "="*50)
            print("阶段2: 构建因子")
            print("="*50)
            factor_data = self.factor_builder.build_factors(processed_data)
            
            # 3. 准备训练数据
            print("\n" + "="*50)
            print("阶段3: 准备训练数据")
            print("="*50)
            X, y = self._prepare_training_data(factor_data, target_time)
            
            # 4. 特征选择
            print("\n" + "="*50)
            print("阶段4: 特征选择")
            print("="*50)
            X_selected = self.feature_selector.select_features(X, y)
            
            # 5. 训练模型
            print("\n" + "="*50)
            print("阶段5: 训练模型")
            print("="*50)
            self.model_trainer.train_models(X_selected, y, cv_splits)
            
            # 6. 评估模型
            print("\n" + "="*50)
            print("阶段6: 评估模型")
            print("="*50)
            y_pred = self.model_trainer.predict(X_selected)
            evaluation_report = self.model_evaluator.generate_report(
                y, y_pred, processed_data
            )
            
            self.logger.info("模型训练完成")
            self.logger.info(f"评估报告: {evaluation_report}")
            
            return evaluation_report
            
        except Exception as e:
            self.logger.error(f"训练过程中出错: {str(e)}")
            raise
    
    def _prepare_training_data(self, 
                             market_data: Dict[str, pd.DataFrame], 
                             target_time: str) -> tuple:
        """准备训练数据
        
        Args:
            market_data: 处理后的市场数据
            target_time: 目标时间点
            
        Returns:
            X: 特征数据
            y: 目标变量
        """
        # 计算因子
        X = self.factor_builder.build_factors(market_data)
        
        # 计算目标变量
        # 获取第一个股票的数据
        first_stock = list(market_data.keys())[0]
        tick_data = market_data[first_stock]
        next_day_data = tick_data.shift(-1)
        next_day_target = next_day_data.between_time(target_time, target_time)
        y = next_day_target['lastprice'] - tick_data['lastprice']
        
        # 删除无效数据
        valid_mask = ~(X.isna().any(axis=1) | y.isna())
        invalid_count = (~valid_mask).sum()
        if invalid_count > 0:
            self.logger.warning(f"发现 {invalid_count} 条无效数据将被删除")
        X = X[valid_mask]
        y = y[valid_mask]
        
        return X, y
    
    def predict(self, 
                start_date: str, 
                end_date: str, 
                symbols: str) -> Dict[str, Any]:
        """生成预测
        
        Args:
            start_date: 预测数据开始日期
            end_date: 预测数据结束日期
            symbols: 股票代码
            
        Returns:
            预测结果字典
        """
        try:
            # 验证股票代码格式
            if not isinstance(symbols, str):
                raise ValueError("股票代码必须是字符串类型")
            
            # 1. 获取预测数据
            print("\n" + "="*50)
            print("阶段1: 获取预测数据")
            print("="*50)
            processed_data = self.data_processor.prepare_data(
                stock_list=[symbols],
                start_date=start_date,
                end_date=end_date,
                period='tick'
            )
            
            # 2. 构建因子
            print("\n" + "="*50)
            print("阶段2: 构建因子")
            print("="*50)
            factor_data = self.factor_builder.build_factors(processed_data)
            
            # 3. 特征选择
            print("\n" + "="*50)
            print("阶段3: 特征选择")
            print("="*50)
            X_selected = self.feature_selector.transform(factor_data)
            
            # 4. 生成预测
            print("\n" + "="*50)
            print("阶段4: 生成预测")
            print("="*50)
            prediction_result = self.predictor.predict(X_selected)
            
            # 5. 获取预测摘要
            print("\n" + "="*50)
            print("阶段5: 获取预测摘要")
            print("="*50)
            summary = self.predictor.get_prediction_summary()
            
            return {
                'prediction': prediction_result,
                'summary': summary
            }
            
        except Exception as e:
            self.logger.error(f"预测过程中出错: {str(e)}")
            raise
    
    def update_market_condition(self, market_data: Dict[str, pd.DataFrame]):
        """更新市场状态
        
        Args:
            market_data: 市场数据
        """
        # 获取第一个股票的数据
        first_stock = list(market_data.keys())[0]
        tick_data = market_data[first_stock]
        
        # 计算市场波动率
        volatility = tick_data['lastprice'].rolling(20).std().iloc[-1]
        
        # 计算市场趋势
        trend = tick_data['lastprice'].rolling(20).mean().pct_change().iloc[-1]
        
        # 调整预测阈值
        self.predictor.adjust_threshold(volatility, trend)
        
    def get_feature_importance(self) -> pd.DataFrame:
        """获取特征重要性
        
        Returns:
            特征重要性DataFrame
        """
        return self.model_trainer.get_feature_importance()

def main():
    # 1. 初始化
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # 输出到控制台
            logging.FileHandler('download.log')  # 输出到文件
        ]
    )

    data_feed = QMTDataFeed()
    pipeline = MultiFactorPipeline(data_feed=data_feed)
    
    # 下载历史数据（仅下载几只测试用的股票）
    test_stocks = ['600519.SH', '000858.SZ', '601318.SH']  # 茅台、五粮液、中国平安
    data_feed.download_data(
        stock_list=test_stocks,  # 使用测试股票列表
        period='tick',
        start_time='20250101',
        end_time='20250329'
    )
    
    # 2. 训练模型
    evaluation_report = pipeline.train(
        start_date="20250101",
        end_date="20250328",
        symbols="600519.SH",  # 使用贵州茅台的股票代码
        target_time="10:00:00"
    )
    
    # 3. 生成预测
    prediction_result = pipeline.predict(
        start_date="20240329",
        end_date="20240329",
        symbols="600519.SH"
    )
    
    # 4. 输出结果
    print("训练评估报告:", evaluation_report)
    print("预测结果:", prediction_result['prediction'])
    print("预测摘要:", prediction_result['summary'])
    print("特征重要性:", pipeline.get_feature_importance())

if __name__ == "__main__":
    main() 