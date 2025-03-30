import pandas as pd
import numpy as np
from typing import Dict, Any, List
import logging
from sklearn.feature_selection import SelectKBest, f_regression
from sklearn.decomposition import PCA
from sklearn.linear_model import LassoCV
from sklearn.ensemble import RandomForestRegressor

class FeatureSelector:
    def __init__(self, n_features: int = 20):
        """
        初始化特征选择器
        
        Args:
            n_features: 要选择的特征数量
        """
        self.logger = logging.getLogger(__name__)
        self.n_features = n_features
        self.selected_features = None
        self.pca = None
        self.lasso = None
        
    def select_features(self, X: pd.DataFrame, y: pd.Series) -> pd.DataFrame:
        """选择特征
        
        Args:
            X: 特征数据
            y: 目标变量
            
        Returns:
            选择后的特征数据
        """
        try:
            # 1. 相关性分析
            X = self._remove_correlated_features(X)
            
            # 2. 重要性评分
            X = self._select_by_importance(X, y)
            
            # 3. PCA降维
            X = self._apply_pca(X)
            
            # 4. LASSO特征选择
            X = self._apply_lasso(X, y)
            
            self.selected_features = X.columns.tolist()
            return X
            
        except Exception as e:
            self.logger.error(f"特征选择时出错: {str(e)}")
            raise
    
    def _remove_correlated_features(self, X: pd.DataFrame, threshold: float = 0.95) -> pd.DataFrame:
        """移除高度相关的特征
        
        Args:
            X: 特征数据
            threshold: 相关性阈值
            
        Returns:
            移除相关特征后的数据
        """
        corr_matrix = X.corr().abs()
        upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        
        # 找出要删除的特征
        to_drop = [column for column in upper.columns if any(upper[column] > threshold)]
        
        return X.drop(columns=to_drop)
    
    def _select_by_importance(self, X: pd.DataFrame, y: pd.Series) -> pd.DataFrame:
        """使用随机森林选择重要特征
        
        Args:
            X: 特征数据
            y: 目标变量
            
        Returns:
            选择后的特征数据
        """
        rf = RandomForestRegressor(n_estimators=100, random_state=42)
        rf.fit(X, y)
        
        # 获取特征重要性
        importance = pd.DataFrame({
            'feature': X.columns,
            'importance': rf.feature_importances_
        })
        
        # 选择重要性最高的特征
        selected_features = importance.nlargest(self.n_features, 'importance')['feature'].tolist()
        
        return X[selected_features]
    
    def _apply_pca(self, X: pd.DataFrame, n_components: int = 10) -> pd.DataFrame:
        """应用PCA降维
        
        Args:
            X: 特征数据
            n_components: 保留的主成分数量
            
        Returns:
            降维后的特征数据
        """
        self.pca = PCA(n_components=min(n_components, X.shape[1]))
        pca_result = self.pca.fit_transform(X)
        
        # 创建新的特征名称
        pca_features = [f'pca_{i+1}' for i in range(pca_result.shape[1])]
        
        return pd.DataFrame(pca_result, index=X.index, columns=pca_features)
    
    def _apply_lasso(self, X: pd.DataFrame, y: pd.Series) -> pd.DataFrame:
        """使用LASSO进行特征选择
        
        Args:
            X: 特征数据
            y: 目标变量
            
        Returns:
            选择后的特征数据
        """
        self.lasso = LassoCV(cv=5, random_state=42)
        self.lasso.fit(X, y)
        
        # 获取非零系数的特征
        selected_features = X.columns[self.lasso.coef_ != 0].tolist()
        
        return X[selected_features]
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """转换新数据（使用已训练的特征选择器）
        
        Args:
            X: 新的特征数据
            
        Returns:
            转换后的特征数据
        """
        if self.selected_features is None:
            raise ValueError("特征选择器尚未训练")
            
        try:
            # 1. 移除相关特征
            X = self._remove_correlated_features(X)
            
            # 2. 选择重要特征
            X = X[self.selected_features]
            
            # 3. 应用PCA
            if self.pca is not None:
                X = pd.DataFrame(
                    self.pca.transform(X),
                    index=X.index,
                    columns=[f'pca_{i+1}' for i in range(X.shape[1])]
                )
            
            # 4. 应用LASSO
            if self.lasso is not None:
                X = X[self.lasso.coef_ != 0]
                
            return X
            
        except Exception as e:
            self.logger.error(f"转换特征数据时出错: {str(e)}")
            raise 