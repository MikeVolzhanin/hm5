import pandas as pd
import numpy as np
from typing import Dict
from .base_handler import BaseHandler
from config import config


class DateHandler(BaseHandler):
    """Обработчик дат"""
    
    def handle(self, data: pd.DataFrame) -> pd.DataFrame:
        """Обработать даты"""
        
        X_processed = data.copy()
        
        for col in config.DATE_COLUMNS:
            if col in X_processed.columns:
                # Преобразуем в datetime
                X_processed[col] = pd.to_datetime(
                    X_processed[col],
                    errors='coerce',
                    dayfirst=True
                )
                
                # Извлекаем признаки из даты
                X_processed[f'{col}_year'] = X_processed[col].dt.year
                X_processed[f'{col}_month'] = X_processed[col].dt.month
                X_processed[f'{col}_day'] = X_processed[col].dt.day
                X_processed[f'{col}_dayofweek'] = X_processed[col].dt.dayofweek
                
                # Время с последнего обновления
                current_date = config.CURRENT_DATE
                X_processed[f'{col}_days_since'] = (
                    current_date - X_processed[col]
                ).dt.days
                
                # Удаляем исходную колонку
                X_processed = X_processed.drop(columns=[col])
        
        # Заполняем пропуски в числовых колонках
        numeric_cols = X_processed.select_dtypes(include=[np.number]).columns
        X_processed[numeric_cols] = X_processed[numeric_cols].fillna(
            X_processed[numeric_cols].median()
        )
        
        # Передаем дальше по цепочке
        if self._next_handler:
            return self._next_handler.handle(X_processed)
        
        return X_processed