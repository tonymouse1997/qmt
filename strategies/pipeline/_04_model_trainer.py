import pandas as pd
import numpy as np
from typing import Dict, Any, List
import logging
from sklearn.model_selection import TimeSeriesSplit
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
import xgboost as xgb
import lightgbm as lgb
from sklearn.linear_model import ElasticNet
from sklearn.svm import SVR
from sklearn.neural_network import MLPRegressor

class ModelTrainer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.models = {}
        self.best_model = None
        self.best_score = float('-inf')
        self.last_training_date = None
        
    def train_models(self, X: pd.DataFrame, y: pd.Series, cv_splits: int = 5):
        """训练多个模型并选择最佳模型
        
        Args:
            X: 特征数据
            y: 目标变量
            cv_splits: 交叉验证折数
        """
        # 记录训练期的最后一天
        self.last_training_date = X.index[-1]
        
        # 定义要训练的模型
        models = {
            'rf': RandomForestRegressor(n_estimators=100, random_state=42),
            'gb': GradientBoostingRegressor(random_state=42),
            'xgb': xgb.XGBRegressor(random_state=42),
            'lgb': lgb.LGBMRegressor(random_state=42),
            'elastic_net': ElasticNet(random_state=42),
            'svr': SVR(),
            'mlp': MLPRegressor(random_state=42)
        }
        
        # 使用时间序列交叉验证
        tscv = TimeSeriesSplit(n_splits=cv_splits)
        
        # 训练和评估每个模型
        for name, model in models.items():
            try:
                # 交叉验证
                cv_scores = []
                for train_idx, val_idx in tscv.split(X):
                    X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
                    y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
                    
                    model.fit(X_train, y_train)
                    y_pred = model.predict(X_val)
                    score = r2_score(y_val, y_pred)
                    cv_scores.append(score)
                
                # 计算平均分数
                mean_score = np.mean(cv_scores)
                self.logger.info(f"模型 {name} 的平均 R2 分数: {mean_score:.4f}")
                
                # 更新最佳模型
                if mean_score > self.best_score:
                    self.best_score = mean_score
                    self.best_model = model
                    self.logger.info(f"更新最佳模型: {name}")
                
                # 保存模型
                self.models[name] = model
                
            except Exception as e:
                self.logger.error(f"训练模型 {name} 时出错: {str(e)}")
                
        # 使用全部数据训练最佳模型
        if self.best_model is not None:
            self.best_model.fit(X, y)
            self.logger.info("使用全部数据训练最佳模型完成")
            
    def evaluate_model(self, model: Any, X: pd.DataFrame, y: pd.Series) -> Dict[str, float]:
        """评估模型性能
        
        Args:
            model: 要评估的模型
            X: 特征数据
            y: 目标变量
            
        Returns:
            评估指标字典
        """
        y_pred = model.predict(X)
        
        metrics = {
            'r2': r2_score(y, y_pred),
            'mae': mean_absolute_error(y, y_pred),
            'rmse': np.sqrt(mean_squared_error(y, y_pred))
        }
        
        return metrics
    
    def predict(self, X: pd.DataFrame) -> pd.Series:
        """使用最佳模型进行预测
        
        Args:
            X: 特征数据
            
        Returns:
            预测结果
        """
        if self.best_model is None:
            raise ValueError("尚未训练模型")
            
        return pd.Series(self.best_model.predict(X), index=X.index)
    
    def get_feature_importance(self) -> pd.DataFrame:
        """获取特征重要性
        
        Returns:
            特征重要性DataFrame
        """
        if self.best_model is None:
            raise ValueError("尚未训练模型")
            
        # 获取特征重要性
        if hasattr(self.best_model, 'feature_importances_'):
            importance = self.best_model.feature_importances_
        elif hasattr(self.best_model, 'coef_'):
            importance = np.abs(self.best_model.coef_)
        else:
            raise ValueError("模型不支持特征重要性")
            
        # 创建特征重要性DataFrame
        importance_df = pd.DataFrame({
            'feature': self.best_model.feature_names_in_,
            'importance': importance
        })
        
        return importance_df.sort_values('importance', ascending=False)
    
    def get_model_ensemble(self, X: pd.DataFrame) -> pd.Series:
        """使用模型集成进行预测
        
        Args:
            X: 特征数据
            
        Returns:
            集成预测结果
        """
        if not self.models:
            raise ValueError("尚未训练模型")
            
        # 获取所有模型的预测结果
        predictions = pd.DataFrame(index=X.index)
        for name, model in self.models.items():
            predictions[name] = model.predict(X)
            
        # 计算平均预测结果
        return predictions.mean(axis=1) 