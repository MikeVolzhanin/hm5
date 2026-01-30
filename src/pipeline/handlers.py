from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Iterable

import pandas as pd

from .base import BaseHandler, PipelineContext

_NBSP = "\u00A0"
_WS_RE = re.compile(r"\s+")


def _clean_text(s: str) -> str:
    s = s.replace(_NBSP, " ")
    s = s.replace("\u2009", " ")  # thin space
    s = s.strip()
    s = _WS_RE.sub(" ", s)
    return s


def _to_str(x: object) -> str:
    if pd.isna(x):
        return ""
    return str(x)


def _parse_gender_age(text: str) -> tuple[Optional[str], Optional[int]]:
    t = _clean_text(text).lower()
    gender = None
    if "муж" in t:
        gender = "M"
    elif "жен" in t:
        gender = "F"

    age = None
    m = re.search(r"(\d+)\s*год", t)
    if m:
        try:
            age = int(m.group(1))
        except ValueError:
            age = None
    return gender, age


_CURRENCY_MAP = {
    "руб": "RUB",
    "rub": "RUB",
    "р.": "RUB",
    "usd": "USD",
    "$": "USD",
    "eur": "EUR",
    "€": "EUR",
}


def _parse_salary(text: str) -> tuple[Optional[int], Optional[str]]:
    """Parse salary from strings like '27 000 руб.' or 'по договоренности'."""
    t = _clean_text(text).lower()
    if not t or "не указ" in t or "договор" in t:
        return None, None

    currency = None
    for k, v in _CURRENCY_MAP.items():
        if k in t:
            currency = v
            break

    nums = [int(n) for n in re.findall(r"\d+", t)]
    if not nums:
        return None, currency

    if "от" in t and len(nums) >= 1:
        val = nums[0]
    elif "до" in t and len(nums) >= 1:
        val = nums[0]
    elif len(nums) >= 2:
        val = int(round((nums[0] + nums[1]) / 2))
    else:
        val = nums[0]

    if currency == "RUB" or currency is None:
        return val, currency or "RUB"

    # non-RUB: do not convert here
    return val, currency


def _parse_city_flags(text: str) -> tuple[Optional[str], Optional[bool], Optional[bool]]:
    t = _clean_text(text)
    if not t:
        return None, None, None
    parts = [p.strip() for p in t.split(",") if p.strip()]
    city = parts[0] if parts else None

    tl = t.lower()
    relocation = None
    trips = None
    if "готов к переезду" in tl:
        relocation = True
    elif "не готов к переезду" in tl:
        relocation = False

    if "готов к командировкам" in tl:
        trips = True
    elif "не готов к командировкам" in tl:
        trips = False

    return city, relocation, trips


def _parse_total_experience_months(text: str) -> Optional[int]:
    t = _clean_text(text).lower()
    if not t:
        return None
    m = re.search(r"опыт работы\s+(\d+)\s*лет?\s*(\d+)?\s*(месяц|мес)?", t)
    if not m:
        return None
    years = int(m.group(1))
    months = 0
    if m.group(2):
        try:
            months = int(m.group(2))
        except ValueError:
            months = 0
    return years * 12 + months


def _education_level(text: str) -> Optional[str]:
    t = _clean_text(text).lower()
    if not t or "не указ" in t:
        return None
    if "высшее" in t:
        return "higher"
    if "среднее специальное" in t:
        return "secondary_special"
    if "среднее" in t:
        return "secondary"
    return "other"


def _has_car(text: str) -> Optional[bool]:
    t = _clean_text(text).lower()
    if not t or "не указ" in t:
        return None
    if "имеется" in t or "собственн" in t:
        return True
    return False


@dataclass
class NormalizeColumnsHandler(BaseHandler):
    def handle(self, ctx: PipelineContext) -> PipelineContext:
        df = ctx.df.copy()
        drop_cols = [c for c in df.columns if not str(c).strip() or str(c).lower().startswith("unnamed")]
        if drop_cols:
            df = df.drop(columns=drop_cols)
        df.columns = [str(c).strip() for c in df.columns]
        ctx.df = df
        return super().handle(ctx)


@dataclass
class CleanTextColumnsHandler(BaseHandler):
    columns: Iterable[str]

    def handle(self, ctx: PipelineContext) -> PipelineContext:
        df = ctx.df.copy()
        for col in self.columns:
            if col in df.columns:
                df[col] = df[col].map(_to_str).map(_clean_text)
        ctx.df = df
        return super().handle(ctx)


class ParseGenderAgeHandler(BaseHandler):
    def handle(self, ctx: PipelineContext) -> PipelineContext:
        df = ctx.df.copy()
        col = "Пол, возраст"
        if col in df.columns:
            parsed = df[col].map(_to_str).map(_parse_gender_age)
            df["gender"] = parsed.map(lambda x: x[0])
            df["age"] = parsed.map(lambda x: x[1])
        ctx.df = df
        return super().handle(ctx)


class ParseSalaryHandler(BaseHandler):
    def handle(self, ctx: PipelineContext) -> PipelineContext:
        df = ctx.df.copy()
        col = "ЗП"
        if col in df.columns:
            parsed = df[col].map(_to_str).map(_parse_salary)
            df["salary_value"] = parsed.map(lambda x: x[0])
            df["salary_currency"] = parsed.map(lambda x: x[1])
            df["salary_rub"] = df["salary_value"].where(df["salary_currency"].fillna("RUB") == "RUB", other=pd.NA)
        ctx.df = df
        return super().handle(ctx)


class ParseCityHandler(BaseHandler):
    def handle(self, ctx: PipelineContext) -> PipelineContext:
        df = ctx.df.copy()
        col = "Город"
        if col in df.columns:
            parsed = df[col].map(_to_str).map(_parse_city_flags)
            df["city"] = parsed.map(lambda x: x[0])
            df["relocation_ready"] = parsed.map(lambda x: x[1])
            df["business_trips_ready"] = parsed.map(lambda x: x[2])
        ctx.df = df
        return super().handle(ctx)


class ParseExperienceHandler(BaseHandler):
    def handle(self, ctx: PipelineContext) -> PipelineContext:
        df = ctx.df.copy()
        col = "Опыт (двойное нажатие для полной версии)"
        if col in df.columns:
            df["experience_total_months"] = df[col].map(_to_str).map(_parse_total_experience_months)
        ctx.df = df
        return super().handle(ctx)


class ParseEducationHandler(BaseHandler):
    def handle(self, ctx: PipelineContext) -> PipelineContext:
        df = ctx.df.copy()
        col = "Образование и ВУЗ"
        if col in df.columns:
            df["education_level"] = df[col].map(_to_str).map(_education_level)
        ctx.df = df
        return super().handle(ctx)


class ParseCarHandler(BaseHandler):
    def handle(self, ctx: PipelineContext) -> PipelineContext:
        df = ctx.df.copy()
        col = "Авто"
        if col in df.columns:
            df["has_car"] = df[col].map(_to_str).map(_has_car)
        ctx.df = df
        return super().handle(ctx)


@dataclass
class SelectXYHandler(BaseHandler):
    target: str
    drop_missing_target: bool = False

    def handle(self, ctx: PipelineContext) -> PipelineContext:
        df = ctx.df.copy()

        if self.target not in df.columns:
            raise KeyError(
                f"Target column '{self.target}' not found after parsing. Available: {list(df.columns)}"
            )

        y = df[self.target]
        if self.drop_missing_target:
            mask = ~y.isna()
            df = df.loc[mask].copy()
            y = y.loc[mask]

        keep = [
            "gender",
            "age",
            "city",
            "relocation_ready",
            "business_trips_ready",
            "Занятость",
            "График",
            "experience_total_months",
            "education_level",
            "has_car",
            "Ищет работу на должность:",
            "Последенее/нынешнее место работы",
            "Последеняя/нынешняя должность",
            "Обновление резюме",
        ]
        features = [c for c in keep if c in df.columns]
        X = df[features].copy()

        # Normalize multi-valued strings
        for c in ("Занятость", "График"):
            if c in X.columns:
                X[c] = X[c].astype("string").str.lower().str.replace(r"\s+", " ", regex=True).str.strip()

        if "Обновление резюме" in X.columns:
            X["resume_updated_at"] = pd.to_datetime(
                X["Обновление резюме"],
                errors="coerce",
                dayfirst=True,
            )
            X = X.drop(columns=["Обновление резюме"])

        ctx.X = X
        ctx.y = y.rename(self.target)
        return super().handle(ctx)
