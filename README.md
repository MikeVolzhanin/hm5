# HH CSV preprocessing -> x_data.npy, y_data.npy (Chain of Responsibility)

## Setup
```bash
python -m venv .venv
# Windows: .\.venv\Scripts\Activate.ps1
# Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

## Run
```bash
python app.py --input hh.csv --outdir data/processed --chunksize 50000 --drop-missing-target
```

## Outputs
In `data/processed/`:
- `x_data.npy` (float32, [n_rows, n_features])
- `y_data.npy` (float32, [n_rows])
- `feature_names.txt`

## Notes
- 2-pass processing to support very large CSV without loading into RAM.
- Categorical features are label-encoded consistently across chunks.
- Default target: `salary_rub` parsed from `ЗП`.
