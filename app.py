#!/usr/bin/env python3
"""
HeadHunter CSV Data Processor
Использует паттерн "Цепочка ответственности" для обработки данных
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import sys

from config import config
from pipeline.pipeline_factory import PipelineFactory


def validate_file(file_path: str) -> bool:
    """Проверить существование файла"""
    path = Path(file_path)
    if not path.exists():
        print(f"❌ Файл не найден: {file_path}")
        return False
    if not path.suffix.lower() == '.csv':
        print(f"❌ Файл должен быть в формате CSV: {file_path}")
        return False
    return True


def load_data(file_path: str) -> pd.DataFrame:
    """Загрузить данные из CSV файла"""
    try:
        # Пробуем разные кодировки
        encodings = ['utf-8', 'cp1251', 'latin1']
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                print(f"✓ Файл загружен с кодировкой {encoding}")
                return df
            except UnicodeDecodeError:
                continue
        
        # Если ни одна кодировка не подошла
        raise ValueError("Не удалось определить кодировку файла")
        
    except Exception as e:
        print(f"❌ Ошибка при загрузке файла: {e}")
        sys.exit(1)


def main():
    """Основная функция приложения"""
    
    # Настройка парсера аргументов
    parser = argparse.ArgumentParser(
        description='HeadHunter CSV Data Processor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python app.py hh.csv
  python app.py /path/to/hh.csv
        """
    )
    
    parser.add_argument(
        'csv_path',
        type=str,
        help='Путь к CSV файлу с данными HeadHunter'
    )
    
    parser.add_argument(
        '--encoding',
        type=str,
        default='auto',
        help='Кодировка файла (utf-8, cp1251, latin1)'
    )
    
    args = parser.parse_args()
    
    # Валидация файла
    if not validate_file(args.csv_path):
        sys.exit(1)
    
    print("=" * 60)
    print("HeadHunter Data Processor")
    print("=" * 60)
    
    # Загрузка данных
    print("\n📥 Загрузка данных...")
    df = load_data(args.csv_path)
    
    print(f"   Загружено {len(df)} строк, {len(df.columns)} колонок")
    print(f"   Колонки: {', '.join(df.columns.tolist())}")
    
    # Проверка целевой колонки
    if config.TARGET_COLUMN not in df.columns:
        print(f"❌ Целевая колонка '{config.TARGET_COLUMN}' не найдена в данных")
        print(f"   Доступные колонки: {', '.join(df.columns.tolist())}")
        sys.exit(1)
    
    # Создание пайплайна
    print("\n🔧 Создание пайплайна обработки...")
    pipeline = PipelineFactory.create_pipeline()
    print(f"   Цепочка обработчиков: {pipeline}")
    
    # Обработка данных
    print("\n⚙️  Обработка данных...")
    try:
        X_processed, y = pipeline.handle(df)
        
        # Удаляем строки с пропущенными значениями в целевой переменной
        valid_indices = ~np.isnan(y)
        X_processed = X_processed[valid_indices]
        y = y[valid_indices]
        
        print(f"   После очистки: {len(X_processed)} строк")
        print(f"   Признаков после обработки: {X_processed.shape[1]}")
        
        # Сохранение результатов
        print("\n💾 Сохранение результатов...")
        PipelineFactory.save_results(X_processed, y)
        
    except Exception as e:
        print(f"❌ Ошибка при обработке данных: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    print("\n✅ Обработка завершена успешно!")
    print("=" * 60)


if __name__ == "__main__":
    main()
