from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd

log = logging.getLogger("io.readers")


def _try_read_chunks(
    input_path: Path,
    chunksize: int,
    encoding: Optional[str],
    delimiter: Optional[str],
    engine: str,
) -> Iterator[pd.DataFrame]:
    kwargs = dict(
        chunksize=chunksize,
        encoding=encoding,
        sep=delimiter,
        engine=engine,
        quotechar='"',
        on_bad_lines="skip",
    )
    if engine == "c":
        kwargs["low_memory"] = False

    yield from pd.read_csv(input_path, **kwargs)



def iter_csv_chunks(
    input_path: Path,
    chunksize: int = 50_000,
    encoding: Optional[str] = None,
    delimiter: Optional[str] = None,
) -> Iterator[pd.DataFrame]:
    encodings = [encoding] if encoding else ["utf-8", "utf-8-sig", "cp1251"]
    engines = ["c", "python"]

    last_err: Exception | None = None
    for enc in encodings:
        for eng in engines:
            try:
                log.info("Reading %s with encoding=%s engine=%s", input_path, enc, eng)
                it = _try_read_chunks(input_path, chunksize=chunksize, encoding=enc, delimiter=delimiter, engine=eng)
                first = next(it)  # smoke test
                yield first
                for chunk in it:
                    yield chunk
                return
            except StopIteration:
                return
            except Exception as e:
                last_err = e

    raise RuntimeError(f"Failed to read CSV {input_path}: {last_err}") from last_err
