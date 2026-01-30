from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np


@dataclass
class NpyWriter:
    outdir: Path
    n_rows: int
    feature_names: list[str]

    def __post_init__(self) -> None:
        self.outdir.mkdir(parents=True, exist_ok=True)
        self.x_path = self.outdir / "x_data.npy"
        self.y_path = self.outdir / "y_data.npy"
        self.feat_path = self.outdir / "feature_names.txt"

        # npy memmaps (proper .npy format, writable without loading whole array)
        self._X = np.lib.format.open_memmap(
            self.x_path, mode="w+", dtype=np.float32, shape=(self.n_rows, len(self.feature_names))
        )
        self._y = np.lib.format.open_memmap(self.y_path, mode="w+", dtype=np.float32, shape=(self.n_rows,))

        self.feat_path.write_text("\n".join(self.feature_names), encoding="utf-8")

    def write(self, offset: int, X_arr: np.ndarray, y_arr: np.ndarray) -> None:
        n = X_arr.shape[0]
        self._X[offset : offset + n, :] = X_arr
        self._y[offset : offset + n] = y_arr

    def close(self) -> None:
        # flush memmaps
        self._X.flush()
        self._y.flush()
