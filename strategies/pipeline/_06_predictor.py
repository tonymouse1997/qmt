import pandas as pd
import numpy as np
from typing import Dict, Any, List
import logging
from sklearn.preprocessing import StandardScaler

class Predictor:
    def __init__(self, model_trainer: Any, threshold: float = 0.0):
        """
        初始化预测器
        
        Args:
            model_trainer: 训练好的模型训练器
            threshold: 预测阈值
        """
        self.logger = logging.getLogger(__name__)
        self.model_trainer = model_trainer
        self.threshold = threshold
        self.scaler = StandardScaler()
        self.prediction_history = []
        
    def predict(self, X: pd.DataFrame) -> Dict[str, Any]:
        """生成预测结果
        
        Args:
            X: 特征数据
            
        Returns:
            预测结果字典
        """
        try:
            # 1. 获取模型预测
            prediction = self.model_trainer.predict(X)
            
            # 2. 获取集成预测
            ensemble_prediction = self.model_trainer.get_model_ensemble(X)
            
            # 3. 计算预测置信度
            confidence = self._calculate_confidence(prediction, ensemble_prediction)
            
            # 4. 生成预测信号
            signal = self._generate_signal(prediction, confidence)
            
            # 5. 更新预测历史
            self._update_history(prediction, ensemble_prediction, confidence, signal)
            
            return {
                'prediction': prediction,
                'ensemble_prediction': ensemble_prediction,
                'confidence': confidence,
                'signal': signal
            }
            
        except Exception as e:
            self.logger.error(f"预测时出错: {str(e)}")
            raise
    
    def _calculate_confidence(self, 
                            prediction: pd.Series, 
                            ensemble_prediction: pd.Series) -> pd.Series:
        """计算预测置信度
        
        Args:
            prediction: 单个模型预测
            ensemble_prediction: 集成预测
            
        Returns:
            置信度序列
        """
        # 计算预测一致性
        consistency = 1 - np.abs(prediction - ensemble_prediction) / np.abs(ensemble_prediction)
        
        # 计算预测稳定性
        if self.prediction_history:
            stability = self._calculate_stability(prediction)
        else:
            stability = pd.Series(0.5, index=prediction.index)
            
        # 综合置信度
        confidence = 0.7 * consistency + 0.3 * stability
        
        return confidence.clip(0, 1)
    
    def _calculate_stability(self, current_prediction: pd.Series) -> pd.Series:
        """计算预测稳定性
        
        Args:
            current_prediction: 当前预测
            
        Returns:
            稳定性得分
        """
        # 使用最近5次预测计算稳定性
        recent_predictions = pd.DataFrame(self.prediction_history[-5:])
        
        # 计算预测变化率
        changes = recent_predictions.diff().abs()
        
        # 计算稳定性得分
        stability = 1 - changes.mean() / current_prediction.abs()
        
        return stability.clip(0, 1)
    
    def _generate_signal(self, 
                        prediction: pd.Series, 
                        confidence: pd.Series) -> pd.Series:
        """生成交易信号
        
        Args:
            prediction: 预测值
            confidence: 置信度
            
        Returns:
            交易信号序列
        """
        # 根据预测值和置信度生成信号
        signal = pd.Series(0, index=prediction.index)
        
        # 高置信度信号
        high_conf_mask = confidence > 0.8
        signal[high_conf_mask & (prediction > self.threshold)] = 1
        signal[high_conf_mask & (prediction < -self.threshold)] = -1
        
        # 中等置信度信号
        medium_conf_mask = (confidence > 0.6) & (confidence <= 0.8)
        signal[medium_conf_mask & (prediction > self.threshold * 1.5)] = 1
        signal[medium_conf_mask & (prediction < -self.threshold * 1.5)] = -1
        
        return signal
    
    def _update_history(self, 
                       prediction: pd.Series, 
                       ensemble_prediction: pd.Series, 
                       confidence: pd.Series, 
                       signal: pd.Series):
        """更新预测历史
        
        Args:
            prediction: 预测值
            ensemble_prediction: 集成预测
            confidence: 置信度
            signal: 交易信号
        """
        self.prediction_history.append({
            'prediction': prediction,
            'ensemble_prediction': ensemble_prediction,
            'confidence': confidence,
            'signal': signal
        })
        
        # 只保留最近100次预测
        if len(self.prediction_history) > 100:
            self.prediction_history = self.prediction_history[-100:]
    
    def adjust_threshold(self, 
                        market_volatility: float, 
                        market_trend: float):
        """根据市场条件调整预测阈值
        
        Args:
            market_volatility: 市场波动率
            market_trend: 市场趋势
        """
        # 根据波动率调整阈值
        volatility_factor = 1 + market_volatility
        
        # 根据趋势调整阈值
        trend_factor = 1 + abs(market_trend)
        
        # 更新阈值
        self.threshold *= volatility_factor * trend_factor
        
        self.logger.info(f"调整预测阈值: {self.threshold:.4f}")
    
    def get_prediction_summary(self) -> Dict[str, Any]:
        """获取预测摘要
        
        Returns:
            预测摘要字典
        """
        if not self.prediction_history:
            return {}
            
        recent_predictions = pd.DataFrame(self.prediction_history[-10:])
        
        summary = {
            'mean_prediction': recent_predictions['prediction'].mean(),
            'mean_confidence': recent_predictions['confidence'].mean(),
            'signal_distribution': recent_predictions['signal'].value_counts().to_dict(),
            'prediction_std': recent_predictions['prediction'].std()
        }
        
        return summary 