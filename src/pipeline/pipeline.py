from __future__ import annotations

import pandas as pd

from .base import PipelineContext, BaseHandler


class Pipeline:
    def __init__(self, first: BaseHandler) -> None:
        self._first = first

    def process_chunk(self, chunk: pd.DataFrame) -> PipelineContext:
        ctx = PipelineContext(raw=chunk, df=chunk.copy())
        return self._first.handle(ctx)
