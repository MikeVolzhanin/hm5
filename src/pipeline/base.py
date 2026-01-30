from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, Optional, Any

import pandas as pd


@dataclass
class PipelineContext:
    """State passed through the chain for a single chunk."""

    raw: pd.DataFrame
    df: pd.DataFrame
    X: Optional[pd.DataFrame] = None
    y: Optional[pd.Series] = None
    notes: dict[str, Any] = field(default_factory=dict)


class Handler(Protocol):
    def set_next(self, nxt: "Handler") -> "Handler": ...
    def handle(self, ctx: PipelineContext) -> PipelineContext: ...


class BaseHandler:
    """Base class implementing chain plumbing.

    Important: dataclass наследники не вызывают BaseHandler.__init__ автоматически,
    поэтому _next может отсутствовать. Используем getattr/setattr.
    """

    def __init__(self) -> None:
        self._next: Optional["BaseHandler"] = None

    def set_next(self, nxt: "BaseHandler") -> "BaseHandler":
        setattr(self, "_next", nxt)
        return nxt

    def handle(self, ctx: PipelineContext) -> PipelineContext:
        nxt = getattr(self, "_next", None)
        if nxt is None:
            return ctx
        return nxt.handle(ctx)

