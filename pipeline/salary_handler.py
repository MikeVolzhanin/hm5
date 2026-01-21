import pandas as pd
import numpy as np
from typing import Tuple
from .base_handler import BaseHandler
from config import config


class SalaryHandler(BaseHandler):
    """Обработчик для извлечения и очистки целевой переменной (зарплаты)"""
    
    def handle(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, np.ndarray]:
        """Обработать данные, выделив целевую переменную"""
        
        # Извлекаем целевую переменную
        y = self._extract_salary(data[config.TARGET_COLUMN])
        
        # Удаляем целевую колонку из признаков
        X = data.drop(columns=[config.TARGET_COLUMN])
        
        # Передаем дальше по цепочке
        if self._next_handler:
            X = self._next_handler.handle(X)
        
        return X, y
    
    def _extract_salary(self, salary_series: pd.Series) -> np.ndarray:
        """Извлечь числовое значение зарплаты из строк"""
        salaries = []
        
        for salary in salary_series:
            if pd.isna(salary):
                salaries.append(np.nan)
                continue
                
            # Ищем числовое значение
            match = config.SALARY_PATTERN.search(str(salary))
            if match:
                # Удаляем пробелы и преобразуем в число
                salary_value = match.group(1).replace(' ', '')
                try:
                    salaries.append(float(salary_value))
                except ValueError:
                    salaries.append(np.nan)
            else:
                salaries.append(np.nan)
        
        return np.array(salaries)