import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from typing import List
from .base_handler import BaseHandler
from config import config


class TextHandler(BaseHandler):
    """Обработчик текстовых колонок с TF-IDF и SVD"""
    
    def __init__(self):
        super().__init__()
        self.vectorizers = {}
        self.svd_models = {}
        
    def handle(self, data: pd.DataFrame) -> pd.DataFrame:
        """Обработать текстовые колонки"""
        
        X_processed = data.copy()
        
        for col in config.TEXT_COLUMNS:
            if col in X_processed.columns:
                # Заполняем пропуски
                X_processed[col] = X_processed[col].fillna('')
                
                # Создаем и применяем TF-IDF
                if col not in self.vectorizers:
                    self.vectorizers[col] = TfidfVectorizer(
                        max_features=config.MAX_TEXT_FEATURES,
                        min_df=config.MIN_TEXT_DF,
                        stop_words=None  # Убрали русские стоп-слова
                    )
                
                tfidf_matrix = self.vectorizers[col].fit_transform(X_processed[col])
                
                # Применяем SVD для уменьшения размерности
                if col not in self.svd_models:
                    n_components = min(10, config.MAX_TEXT_FEATURES)
                    self.svd_models[col] = TruncatedSVD(
                        n_components=n_components,
                        random_state=42
                    )
                
                svd_features = self.svd_models[col].fit_transform(tfidf_matrix)
                
                # Добавляем SVD признаки в датафрейм
                for i in range(svd_features.shape[1]):
                    X_processed[f'{col}_svd_{i}'] = svd_features[:, i]
                
                # Удаляем исходную текстовую колонку
                X_processed = X_processed.drop(columns=[col])
        
        # Передаем дальше по цепочке
        if self._next_handler:
            return self._next_handler.handle(X_processed)
        
        return X_processed