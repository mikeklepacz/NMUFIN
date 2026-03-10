# NMU FIN

Local multi-currency finance analyzer for CSV-based transaction imports.

## Run

```bash
python3 -m pip install -e ".[dev]"
uvicorn nmu_fin.web:app --reload
```

Open <http://127.0.0.1:8000>.

## Sample data

The repository includes bank-export CSVs under `history_cvs_2026-03-09/`.
