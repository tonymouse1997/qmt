import pandas as pd
import numpy as np
from typing import Dict, Any, Union, List
import logging
from sklearn.preprocessing import StandardScaler

class DataProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def process_data(self, market_data: Union[pd.DataFrame, List[pd.DataFrame], Dict[str, pd.DataFrame]]) -> pd.DataFrame:
        """处理市场数据
        
        Args:
            market_data: 市场数据，可能是DataFrame、DataFrame列表或字典
            
        Returns:
            处理后的DataFrame
        """
        try:
            if isinstance(market_data, pd.DataFrame):
                # 如果是单个DataFrame，直接处理
                cleaned_data = self._clean_data(market_data)
                time_features = self._add_time_features(cleaned_data)
                return pd.concat([cleaned_data, time_features], axis=1)
                
            elif isinstance(market_data, list):
                # 如果是DataFrame列表，需要合并
                if not all(isinstance(df, pd.DataFrame) for df in market_data):
                    raise ValueError("列表中的元素必须是DataFrame类型")
                
                # 合并所有DataFrame
                merged_data = pd.concat(market_data, axis=1)
                
                # 检查是否有NaN值
                nan_cols = merged_data.columns[merged_data.isna().any()].tolist()
                if nan_cols:
                    self.logger.error(f"合并数据时发现NaN值，可能由于不同时间频率导致:")
                    self.logger.error(f"包含NaN的列: {nan_cols}")
                    self.logger.error("请检查数据的时间对齐情况")
                    raise ValueError("数据合并后存在NaN值，请检查时间对齐")
                
                # 处理合并后的数据
                cleaned_data = self._clean_data(merged_data)
                time_features = self._add_time_features(cleaned_data)
                return pd.concat([cleaned_data, time_features], axis=1)
                
            elif isinstance(market_data, dict):
                # 如果是字典，需要处理不同频率的数据
                if not all(isinstance(df, pd.DataFrame) for df in market_data.values()):
                    raise ValueError("字典中的值必须是DataFrame类型")
                
                # 合并所有频率的数据
                merged_data = pd.concat(market_data.values(), axis=1)
                
                # 检查是否有NaN值
                nan_cols = merged_data.columns[merged_data.isna().any()].tolist()
                if nan_cols:
                    self.logger.error(f"合并数据时发现NaN值，可能由于不同时间频率导致:")
                    self.logger.error(f"包含NaN的列: {nan_cols}")
                    self.logger.error("请检查数据的时间对齐情况")
                    raise ValueError("数据合并后存在NaN值，请检查时间对齐")
                
                # 处理合并后的数据
                cleaned_data = self._clean_data(merged_data)
                time_features = self._add_time_features(cleaned_data)
                return pd.concat([cleaned_data, time_features], axis=1)
                
            else:
                raise ValueError(f"输入必须是DataFrame、DataFrame列表或字典，当前类型: {type(market_data)}")
                
        except Exception as e:
            self.logger.error(f"处理市场数据时出错: {str(e)}")
            raise
    
    def _clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """数据清洗
        
        Args:
            data: 原始数据
            
        Returns:
            清洗后的数据
        """
        # 1. 处理缺失值
        # data = data.fillna(method='ffill').fillna(method='bfill')
        
        # 2. 检测异常值并记录日志
        for col in data.columns:
            if data[col].dtype in ['float64', 'int64']:
                # 使用中位数和四分位数范围(IQR)来检测异常值
                Q1 = data[col].quantile(0.25)
                Q3 = data[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 3 * IQR
                upper_bound = Q3 + 3 * IQR
                
                # 检测异常值
                outliers = data[col][(data[col] < lower_bound) | (data[col] > upper_bound)]
                if not outliers.empty:
                    self.logger.warning(f"列 {col} 中发现异常值:")
                    self.logger.warning(f"异常值数量: {len(outliers)}")
                    self.logger.warning(f"异常值范围: [{outliers.min():.2f}, {outliers.max():.2f}]")
                    self.logger.warning(f"异常值时间点: {outliers.index.tolist()}")
                
        return data
    
    def _add_time_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """添加时间特征
        
        Args:
            data: 清洗后的数据
            
        Returns:
            添加时间特征后的数据
        """
        time_features = pd.DataFrame(index=data.index)
        
        # 判断时间格式并转换为datetime
        if isinstance(data.index[0], (int, float)):
            # 处理时间戳格式(包括末尾带3个0的格式)
            timestamps = data.index.astype('int64')
            if len(str(timestamps[0])) > 13:
                timestamps = timestamps // 1000  # 去掉末尾3个0
            time_index = pd.to_datetime(timestamps, unit='ms')
        else:
            # 已经是datetime格式
            time_index = data.index
            
        # 提取时间特征
        time_features['day_of_week'] = time_index.dayofweek
        # 添加上午/下午标记
        time_features['is_morning'] = (time_index.hour < 12).astype(int)
        
        # 计算开盘后的分钟数
        # 计算与开盘时间的时间差
        morning_open = pd.Timedelta(hours=9, minutes=0)
        afternoon_open = pd.Timedelta(hours=13)
        
        # 获取每个时间点的时分
        time_of_day = pd.Timedelta(hours=time_index.hour, minutes=time_index.minute)
        
        # 计算距离开盘的分钟数
        time_features['minutes_from_open'] = np.where(
            time_index.hour < 12,
            (time_of_day - morning_open).total_seconds() / 60,
            (time_of_day - afternoon_open).total_seconds() / 60 + 120  # 加上上午的120分钟
        )

        return time_features