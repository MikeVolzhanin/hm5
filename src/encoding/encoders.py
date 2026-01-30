from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd


def _norm_cat(x: Any) -> str:
    if pd.isna(x):
        return "__NA__"
    s = str(x).strip()
    return s if s else "__NA__"


@dataclass
class FitState:
    """Holds fitted categorical mappings to allow consistent encoding across chunks."""

    # columns that are treated as categorical (mapped to int codes)
    cat_cols: set[str] = field(default_factory=set)
    # mapping per column: value -> code
    maps: dict[str, dict[str, int]] = field(default_factory=dict)

    def init_columns(self, feature_names: list[str]) -> None:
        # Anything non-numeric will be considered categorical; we decide during fit_chunk too.
        self.cat_cols = set(feature_names)

    def fit_chunk(self, X: pd.DataFrame) -> None:
        for col in X.columns:
            ser = X[col]
            # numeric columns: leave as numeric (remove from cat_cols)
            if pd.api.types.is_numeric_dtype(ser) or pd.api.types.is_bool_dtype(ser) or pd.api.types.is_datetime64_any_dtype(ser):
                self.cat_cols.discard(col)
                continue

            # categorical
            self.maps.setdefault(col, {"__NA__": 0})
            m = self.maps[col]
            for v in ser.map(_norm_cat).unique():
                if v not in m:
                    m[v] = len(m)

    def transform_chunk(self, X: pd.DataFrame, y: pd.Series, feature_names: list[str]) -> tuple[np.ndarray, np.ndarray]:
        """Return (X_arr, y_arr) as float32 arrays."""
        X = X.reindex(columns=feature_names)

        out = np.empty((len(X), len(feature_names)), dtype=np.float32)

        for j, col in enumerate(feature_names):
            ser = X[col] if col in X.columns else pd.Series([pd.NA] * len(X))

            if col in self.cat_cols:
                mapping = self.maps.get(col, {"__NA__": 0})
                codes = ser.map(_norm_cat).map(lambda v: mapping.get(v, 0)).astype("int32").to_numpy()
                out[:, j] = codes.astype(np.float32)
            else:
                if pd.api.types.is_datetime64_any_dtype(ser):
                    # seconds since epoch, NaT -> -1
                    dt = pd.to_datetime(ser, errors="coerce")
                    # dt.astype("int64") gives ns since epoch; NaT becomes the min int64
                    ns = dt.astype("int64")
                    sec = (ns // 1_000_000_000).astype("int64")
                    sec = sec.where(dt.notna(), -1)
                    out[:, j] = sec.to_numpy(dtype=np.float32)
                elif pd.api.types.is_bool_dtype(ser):
                    out[:, j] = ser.fillna(False).astype("int8").to_numpy(dtype=np.float32)
                else:
                    out[:, j] = pd.to_numeric(ser, errors="coerce").fillna(np.nan).to_numpy(dtype=np.float32)

        y_arr = pd.to_numeric(y, errors="coerce").to_numpy(dtype=np.float32)
        return out, y_arr
