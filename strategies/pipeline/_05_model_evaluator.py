import pandas as pd
import numpy as np
from typing import Dict, Any, List
import logging
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
import matplotlib.pyplot as plt
import seaborn as sns

class ModelEvaluator:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def evaluate(self, y_true: pd.Series, y_pred: pd.Series) -> Dict[str, float]:
        """评估模型性能
        
        Args:
            y_true: 真实值
            y_pred: 预测值
            
        Returns:
            评估指标字典
        """
        metrics = {
            'r2': r2_score(y_true, y_pred),
            'mae': mean_absolute_error(y_true, y_pred),
            'rmse': np.sqrt(mean_squared_error(y_true, y_pred)),
            'mape': np.mean(np.abs((y_true - y_pred) / y_true)) * 100
        }
        
        return metrics
    
    def analyze_errors(self, y_true: pd.Series, y_pred: pd.Series) -> Dict[str, Any]:
        """分析预测误差
        
        Args:
            y_true: 真实值
            y_pred: 预测值
            
        Returns:
            误差分析结果
        """
        errors = y_true - y_pred
        
        analysis = {
            'mean_error': errors.mean(),
            'std_error': errors.std(),
            'max_error': errors.max(),
            'min_error': errors.min(),
            'error_skew': errors.skew(),
            'error_kurtosis': errors.kurtosis()
        }
        
        return analysis
    
    def plot_error_distribution(self, y_true: pd.Series, y_pred: pd.Series, save_path: str = None):
        """绘制误差分布图
        
        Args:
            y_true: 真实值
            y_pred: 预测值
            save_path: 保存路径
        """
        errors = y_true - y_pred
        
        plt.figure(figsize=(10, 6))
        sns.histplot(errors, kde=True)
        plt.title('预测误差分布')
        plt.xlabel('误差')
        plt.ylabel('频数')
        
        if save_path:
            plt.savefig(save_path)
        plt.close()
    
    def plot_prediction_vs_actual(self, y_true: pd.Series, y_pred: pd.Series, save_path: str = None):
        """绘制预测值vs实际值散点图
        
        Args:
            y_true: 真实值
            y_pred: 预测值
            save_path: 保存路径
        """
        plt.figure(figsize=(10, 6))
        plt.scatter(y_true, y_pred, alpha=0.5)
        plt.plot([y_true.min(), y_true.max()], [y_true.min(), y_true.max()], 'r--')
        plt.title('预测值 vs 实际值')
        plt.xlabel('实际值')
        plt.ylabel('预测值')
        
        if save_path:
            plt.savefig(save_path)
        plt.close()
    
    def analyze_by_time(self, y_true: pd.Series, y_pred: pd.Series) -> pd.DataFrame:
        """按时间分析预测性能
        
        Args:
            y_true: 真实值
            y_pred: 预测值
            
        Returns:
            按时间统计的性能指标
        """
        # 确保索引是datetime类型
        if not isinstance(y_true.index, pd.DatetimeIndex):
            raise ValueError("时间序列索引必须是datetime类型")
            
        # 按小时统计
        hourly_stats = pd.DataFrame({
            'hour': y_true.index.hour,
            'error': y_true - y_pred
        }).groupby('hour').agg({
            'error': ['mean', 'std', 'count']
        })
        
        return hourly_stats
    
    def analyze_by_market_condition(self, 
                                  y_true: pd.Series, 
                                  y_pred: pd.Series, 
                                  market_data: pd.DataFrame) -> pd.DataFrame:
        """按市场条件分析预测性能
        
        Args:
            y_true: 真实值
            y_pred: 预测值
            market_data: 市场数据
            
        Returns:
            按市场条件统计的性能指标
        """
        # 计算市场条件指标
        volatility = market_data['close'].rolling(20).std()
        trend = market_data['close'].rolling(20).mean().pct_change()
        
        # 创建市场条件分类
        conditions = pd.DataFrame({
            'volatility': pd.qcut(volatility, q=3, labels=['low', 'medium', 'high']),
            'trend': pd.qcut(trend, q=3, labels=['down', 'sideways', 'up'])
        })
        
        # 计算每种条件下的预测误差
        errors = y_true - y_pred
        stats = pd.DataFrame({
            'error': errors,
            'volatility': conditions['volatility'],
            'trend': conditions['trend']
        }).groupby(['volatility', 'trend']).agg({
            'error': ['mean', 'std', 'count']
        })
        
        return stats
    
    def generate_report(self, y_true: pd.Series, y_pred: np.ndarray, market_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """生成评估报告
        
        Args:
            y_true: 真实值
            y_pred: 预测值
            market_data: 市场数据
            
        Returns:
            评估报告字典
        """
        try:
            # 确保数据类型正确
            y_true = pd.Series(y_true)
            y_pred = pd.Series(y_pred, index=y_true.index)
            
            # 计算基础指标
            mse = mean_squared_error(y_true, y_pred)
            rmse = np.sqrt(mse)
            mae = mean_absolute_error(y_true, y_pred)
            r2 = r2_score(y_true, y_pred)
            
            # 计算方向准确率
            direction_true = np.sign(y_true)
            direction_pred = np.sign(y_pred)
            direction_accuracy = np.mean(direction_true == direction_pred)
            
            # 计算夏普比率
            returns = pd.Series(y_true, index=y_true.index)
            sharpe_ratio = np.sqrt(252) * returns.mean() / returns.std() if returns.std() != 0 else 0
            
            # 计算最大回撤
            cumulative_returns = (1 + returns).cumprod()
            rolling_max = cumulative_returns.expanding().max()
            drawdowns = cumulative_returns / rolling_max - 1
            max_drawdown = drawdowns.min()
            
            # 生成报告
            report = {
                'metrics': {
                    'mse': mse,
                    'rmse': rmse,
                    'mae': mae,
                    'r2': r2,
                    'direction_accuracy': direction_accuracy,
                    'sharpe_ratio': sharpe_ratio,
                    'max_drawdown': max_drawdown
                },
                'summary': {
                    'total_samples': len(y_true),
                    'positive_predictions': np.sum(y_pred > 0),
                    'negative_predictions': np.sum(y_pred < 0),
                    'zero_predictions': np.sum(y_pred == 0)
                }
            }
            
            # 记录评估结果
            self.logger.info("模型评估报告:")
            self.logger.info(f"MSE: {mse:.4f}")
            self.logger.info(f"RMSE: {rmse:.4f}")
            self.logger.info(f"MAE: {mae:.4f}")
            self.logger.info(f"R2: {r2:.4f}")
            self.logger.info(f"方向准确率: {direction_accuracy:.2%}")
            self.logger.info(f"夏普比率: {sharpe_ratio:.2f}")
            self.logger.info(f"最大回撤: {max_drawdown:.2%}")
            
            return report
            
        except Exception as e:
            self.logger.error(f"生成评估报告时出错: {str(e)}")
            raise 