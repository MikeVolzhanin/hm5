import re
from dataclasses import dataclass, field
from typing import List, Pattern
from datetime import datetime


@dataclass
class Config:
    """Конфигурация проекта"""
    # Целевая колонка
    TARGET_COLUMN: str = "ЗП"
    
    # Паттерны для извлечения числовых значений
    SALARY_PATTERN: Pattern = field(default_factory=lambda: re.compile(r'(\d+[\s\d]*)\s*руб\.?'))
    
    # Колонки для обработки
    TEXT_COLUMNS: List[str] = field(default_factory=lambda: [
        "Опыт (двойное нажатие для полной версии)",
        "Последенее/нынешнее место работы",
        "Последеняя/нынешняя должность",  # Добавлено
        "Образование и ВУЗ",
        "Ищет работу на должность:"
    ])
    
    CATEGORICAL_COLUMNS: List[str] = field(default_factory=lambda: [
        "Пол, возраст",
        "Город",
        "Занятость",
        "График",
        "Авто"
    ])
    
    DATE_COLUMNS: List[str] = field(default_factory=lambda: ["Обновление резюме"])
    
    # Настройки предобработки текста
    MAX_TEXT_FEATURES: int = 100
    MIN_TEXT_DF: float = 0.01
    
    # Имена выходных файлов
    OUTPUT_X_FILE: str = "x_data.npy"
    OUTPUT_Y_FILE: str = "y_data.npy"
    
    # Прочие настройки
    CURRENT_DATE: datetime = field(default_factory=datetime.now)


config = Config()