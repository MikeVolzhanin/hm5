import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from typing import Dict, List
from .base_handler import BaseHandler
from config import config


class CategoricalHandler(BaseHandler):
    """Обработчик категориальных колонок"""
    
    def __init__(self):
        super().__init__()
        self.encoders: Dict[str, OneHotEncoder] = {}
        self.column_categories: Dict[str, List[str]] = {}
        
    def handle(self, data: pd.DataFrame) -> pd.DataFrame:
        """Обработать категориальные колонки"""
        
        X_processed = data.copy()
        
        for col in config.CATEGORICAL_COLUMNS:
            if col in X_processed.columns:
                # Заполняем пропуски
                X_processed[col] = X_processed[col].fillna('Не указано')
                
                # Для колонки "Пол, возраст" извлекаем отдельные признаки
                if col == "Пол, возраст":
                    X_processed = self._extract_gender_age(X_processed)
                    X_processed = X_processed.drop(columns=[col])
                    continue
                
                # Создаем и применяем OneHotEncoder
                if col not in self.encoders:
                    self.encoders[col] = OneHotEncoder(
                        sparse_output=False,
                        handle_unknown='ignore'
                    )
                    
                    # Обучаем encoder
                    self.encoders[col].fit(X_processed[[col]])
                    self.column_categories[col] = self.encoders[col].get_feature_names_out([col])
                
                # Преобразуем категориальную колонку
                encoded = self.encoders[col].transform(X_processed[[col]])
                
                # Создаем новые колонки
                encoded_df = pd.DataFrame(
                    encoded,
                    columns=self.column_categories[col],
                    index=X_processed.index
                )
                
                # Добавляем закодированные колонки
                X_processed = pd.concat([X_processed, encoded_df], axis=1)
                
                # Удаляем исходную колонку
                X_processed = X_processed.drop(columns=[col])
        
        # Передаем дальше по цепочке
        if self._next_handler:
            return self._next_handler.handle(X_processed)
        
        return X_processed
    
    def _extract_gender_age(self, data: pd.DataFrame) -> pd.DataFrame:
        """Извлечь пол и возраст из колонки 'Пол, возраст'"""
        df = data.copy()
        
        genders = []
        ages = []
        
        for value in df["Пол, возраст"]:
            if pd.isna(value):
                genders.append('Не указано')
                ages.append(np.nan)
                continue
                
            # Извлекаем пол
            if 'Мужчина' in str(value):
                genders.append('Мужчина')
            elif 'Женщина' in str(value):
                genders.append('Женщина')
            else:
                genders.append('Не указано')
            
            # Извлекаем возраст
            age_match = None
            try:
                # Ищем паттерн "XX года" или "XX год" или "XX лет"
                import re
                age_pattern = re.compile(r'(\d+)\s*(?:год|года|лет)')
                match = age_pattern.search(str(value))
                if match:
                    ages.append(int(match.group(1)))
                else:
                    ages.append(np.nan)
            except:
                ages.append(np.nan)
        
        df['Пол'] = genders
        df['Возраст'] = ages
        
        return df