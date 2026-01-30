#!/usr/bin/env python3
"""
HH CSV preprocessing pipeline (Chain of Responsibility) -> x_data.npy, y_data.npy

Usage:
    python app.py --input hh.csv --outdir data/processed --chunksize 50000 --drop-missing-target

Output (in outdir):
  - x_data.npy  (float32, shape [n_rows, n_features])
  - y_data.npy  (float32, shape [n_rows])

Notes:
- Designed for large CSV: runs in 2 passes.
  1) Fit categorical encoders + count rows after filtering
  2) Transform and write to .npy via numpy.open_memmap (no full dataset in RAM)
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.encoding.encoders import FitState
from src.io.readers import iter_csv_chunks
from src.io.writers import NpyWriter
from src.pipeline.builder import build_pipeline


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="HH CSV preprocessing -> x_data.npy, y_data.npy")
    p.add_argument("--input", "-i", required=True, help="Path to hh.csv")
    p.add_argument("--outdir", "-o", default="data/processed", help="Directory to write outputs")
    p.add_argument("--chunksize", "-c", type=int, default=50_000, help="Rows per chunk")
    p.add_argument("--encoding", default=None, help="Force CSV encoding (optional)")
    p.add_argument("--delimiter", default=None, help="Force delimiter (optional)")
    p.add_argument("--target", default="salary_rub", help="Target column after parsing (default: salary_rub)")
    p.add_argument("--drop-missing-target", action="store_true", help="Drop rows where target is missing")
    p.add_argument("--loglevel", default="INFO", help="Logging level")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.loglevel.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("app")

    input_path = Path(args.input).expanduser().resolve()
    outdir = Path(args.outdir).expanduser().resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    pipeline = build_pipeline(target=args.target, drop_missing_target=args.drop_missing_target)

    # -------- pass 1: fit encoders + count rows --------
    fit = FitState()

    total_in = 0
    total_kept = 0
    feature_names: list[str] | None = None

    for chunk in iter_csv_chunks(
        input_path=input_path,
        chunksize=args.chunksize,
        encoding=args.encoding,
        delimiter=args.delimiter,
    ):
        total_in += len(chunk)
        ctx = pipeline.process_chunk(chunk)

        if ctx.X is None or ctx.y is None:
            continue

        if feature_names is None:
            feature_names = list(ctx.X.columns)
            fit.init_columns(feature_names)

        # Fit categorical mappings incrementally
        fit.fit_chunk(ctx.X)

        total_kept += len(ctx.X)

    if feature_names is None:
        raise RuntimeError("No data produced by pipeline. Check input/filters/target parsing.")

    log.info("Pass1 done. Read rows=%s, kept rows=%s, n_features=%s", total_in, total_kept, len(feature_names))

    # -------- pass 2: transform + write to npy memmaps --------
    writer = NpyWriter(outdir=outdir, n_rows=total_kept, feature_names=feature_names)

    offset = 0
    for chunk in iter_csv_chunks(
        input_path=input_path,
        chunksize=args.chunksize,
        encoding=args.encoding,
        delimiter=args.delimiter,
    ):
        ctx = pipeline.process_chunk(chunk)
        if ctx.X is None or ctx.y is None:
            continue

        X_arr, y_arr = fit.transform_chunk(ctx.X, ctx.y, feature_names=feature_names)
        writer.write(offset=offset, X_arr=X_arr, y_arr=y_arr)
        offset += X_arr.shape[0]

    writer.close()

    log.info("Done. Wrote rows=%s into %s", offset, outdir)
    log.info("Files: %s, %s", (outdir / "x_data.npy"), (outdir / "y_data.npy"))
    log.info("Feature names saved to: %s", (outdir / "feature_names.txt"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
