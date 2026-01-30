from __future__ import annotations

from .pipeline import Pipeline
from .handlers import (
    NormalizeColumnsHandler,
    CleanTextColumnsHandler,
    ParseGenderAgeHandler,
    ParseSalaryHandler,
    ParseCityHandler,
    ParseExperienceHandler,
    ParseEducationHandler,
    ParseCarHandler,
    SelectXYHandler,
)


def build_pipeline(target: str = "salary_rub", drop_missing_target: bool = False) -> Pipeline:
    first = NormalizeColumnsHandler()
    h = first
    h = h.set_next(
        CleanTextColumnsHandler(
            columns=[
                "Пол, возраст",
                "ЗП",
                "Ищет работу на должность:",
                "Город",
                "Занятость",
                "График",
                "Опыт (двойное нажатие для полной версии)",
                "Последенее/нынешнее место работы",
                "Последеняя/нынешняя должность",
                "Образование и ВУЗ",
                "Обновление резюме",
                "Авто",
            ]
        )
    )
    h = h.set_next(ParseGenderAgeHandler())
    h = h.set_next(ParseSalaryHandler())
    h = h.set_next(ParseCityHandler())
    h = h.set_next(ParseExperienceHandler())
    h = h.set_next(ParseEducationHandler())
    h = h.set_next(ParseCarHandler())
    h = h.set_next(SelectXYHandler(target=target, drop_missing_target=drop_missing_target))
    return Pipeline(first)
