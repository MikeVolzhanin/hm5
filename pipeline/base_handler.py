from abc import ABC, abstractmethod
from typing import Optional, Any
import pandas as pd


class BaseHandler(ABC):
    """Абстрактный базовый класс для обработчиков в цепочке"""
    
    def __init__(self):
        self._next_handler: Optional['BaseHandler'] = None
        
    def set_next(self, handler: 'BaseHandler') -> 'BaseHandler':
        """Установить следующий обработчик в цепочке"""
        self._next_handler = handler
        return handler
    
    @abstractmethod
    def handle(self, data: Any) -> Any:
        """Обработать данные"""
        if self._next_handler:
            return self._next_handler.handle(data)
        return data
    
    def __str__(self):
        return f"{self.__class__.__name__} -> {self._next_handler}"