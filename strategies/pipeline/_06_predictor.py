import pandas as pd
import numpy as np
import logging
from typing import Dict, Any
from ._04_model_trainer import ModelTrainer

class Predictor:
    """预测器类"""
    
    def __init__(self, model_trainer: ModelTrainer, threshold: float = 0.0):
        """
        初始化预测器
        
        Args:
            model_trainer: 模型训练器
            threshold: 预测阈值
        """
        self.logger = logging.getLogger(__name__)
        self.model_trainer = model_trainer
        self.threshold = threshold
        self.prediction_summary = None
        
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """生成预测
        
        Args:
            X: 特征数据
            
        Returns:
            预测结果
        """
        try:
            # 获取模型预测
            predictions = self.model_trainer.predict(X)
            
            # 确保预测结果是numpy数组
            predictions = np.array(predictions)
            
            # 应用阈值
            predictions = np.where(
                np.abs(predictions) > self.threshold,
                predictions,
                0
            )
            
            # 更新预测摘要
            self._update_prediction_summary(predictions)
            
            return predictions
            
        except Exception as e:
            self.logger.error(f"生成预测时出错: {str(e)}")
            raise
            
    def adjust_threshold(self, volatility: float, trend: float):
        """调整预测阈值
        
        Args:
            volatility: 市场波动率
            trend: 市场趋势
        """
        # 根据市场波动率和趋势动态调整阈值
        base_threshold = 0.01  # 基础阈值
        volatility_factor = 1 + volatility  # 波动率因子
        trend_factor = 1 + abs(trend)  # 趋势因子
        
        self.threshold = base_threshold * volatility_factor * trend_factor
        self.logger.info(f"预测阈值已调整为: {self.threshold:.4f}")
        
    def _update_prediction_summary(self, predictions: np.ndarray):
        """更新预测摘要
        
        Args:
            predictions: 预测结果
        """
        self.prediction_summary = {
            'total_predictions': len(predictions),
            'positive_predictions': np.sum(predictions > 0),
            'negative_predictions': np.sum(predictions < 0),
            'zero_predictions': np.sum(predictions == 0),
            'mean_prediction': np.mean(predictions),
            'std_prediction': np.std(predictions),
            'max_prediction': np.max(predictions),
            'min_prediction': np.min(predictions)
        }
        
    def get_prediction_summary(self) -> Dict[str, Any]:
        """获取预测摘要
        
        Returns:
            预测摘要字典
        """
        if self.prediction_summary is None:
            raise ValueError("需要先生成预测")
        return self.prediction_summary 