"""
Пайплайн обработки данных с использованием паттерна "Цепочка ответственности"
"""

from .base_handler import BaseHandler
from .salary_handler import SalaryHandler
from .text_handler import TextHandler
from .categorical_handler import CategoricalHandler
from .date_handler import DateHandler
from .pipeline_factory import PipelineFactory

__all__ = [
    'BaseHandler',
    'SalaryHandler',
    'TextHandler',
    'CategoricalHandler',
    'DateHandler',
    'PipelineFactory'
]