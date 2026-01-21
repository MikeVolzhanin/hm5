from typing import Tuple
import pandas as pd
import numpy as np

from .salary_handler import SalaryHandler
from .text_handler import TextHandler
from .categorical_handler import CategoricalHandler
from .date_handler import DateHandler


class PipelineFactory:
    """Фабрика для создания цепочки обработчиков"""
    
    @staticmethod
    def create_pipeline() -> Tuple[SalaryHandler, pd.DataFrame, np.ndarray]:
        """Создать цепочку обработчиков"""
        
        # Создаем обработчики
        salary_handler = SalaryHandler()
        text_handler = TextHandler()
        categorical_handler = CategoricalHandler()
        date_handler = DateHandler()
        
        # Собираем цепочку ответственности
        salary_handler.set_next(text_handler) \
                     .set_next(categorical_handler) \
                     .set_next(date_handler)
        
        return salary_handler
    
    @staticmethod
    def save_results(X_processed: pd.DataFrame, y: np.ndarray):
        """Сохранить обработанные данные"""
        from config import config
        
        # Преобразуем в numpy массивы
        X_array = X_processed.to_numpy()
        y_array = y
        
        # Сохраняем
        np.save(config.OUTPUT_X_FILE, X_array)
        np.save(config.OUTPUT_Y_FILE, y_array)
        
        print(f"✅ Данные успешно сохранены:")
        print(f"   X shape: {X_array.shape}")
        print(f"   y shape: {y_array.shape}")
        print(f"   Файлы: {config.OUTPUT_X_FILE}, {config.OUTPUT_Y_FILE}")