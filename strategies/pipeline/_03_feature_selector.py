import pandas as pd
import numpy as np
import logging
from typing import Dict, Any
from sklearn.feature_selection import SelectKBest, f_regression

class FeatureSelector:
    """特征选择器类"""
    
    def __init__(self, n_features: int = 10):
        """
        初始化特征选择器
        
        Args:
            n_features: 要选择的特征数量
        """
        self.logger = logging.getLogger(__name__)
        self.n_features = n_features
        self.selector = SelectKBest(score_func=f_regression, k=n_features)
        self.selected_features = None
        
    def select_features(self, X: pd.DataFrame, y: pd.Series) -> pd.DataFrame:
        """选择特征
        
        Args:
            X: 特征数据
            y: 目标变量
            
        Returns:
            选择后的特征数据
        """
        try:
            # 处理缺失值
            X = X.fillna(method='ffill').fillna(method='bfill')
            y = y.fillna(method='ffill').fillna(method='bfill')
            
            # 删除非数值列
            numeric_columns = X.select_dtypes(include=[np.number]).columns
            X = X[numeric_columns]
            
            # 删除常量列
            X = X.loc[:, X.std() != 0]
            
            # 选择特征
            X_selected = self.selector.fit_transform(X, y)
            
            # 保存选中的特征名称
            self.selected_features = X.columns[self.selector.get_support()].tolist()
            
            # 创建新的DataFrame
            X_selected_df = pd.DataFrame(
                X_selected,
                index=X.index,
                columns=self.selected_features
            )
            
            self.logger.info(f"选择了 {len(self.selected_features)} 个特征")
            self.logger.info(f"选中的特征: {self.selected_features}")
            
            return X_selected_df
            
        except Exception as e:
            self.logger.error(f"特征选择过程中出错: {str(e)}")
            raise
            
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """转换数据
        
        Args:
            X: 特征数据
            
        Returns:
            转换后的特征数据
        """
        if self.selected_features is None:
            raise ValueError("需要先调用select_features方法")
            
        return X[self.selected_features] 