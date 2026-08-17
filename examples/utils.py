from pathlib import Path

import pandas as pd


def write_parquet(pdf: pd.DataFrame, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    pdf.to_parquet(path, engine="pyarrow", index=False)
    print(f"  wrote {path} ({len(pdf)} rows)")
    return len(pdf)


def run_dates(n: int, start: str = "2026-01-01") -> list[str]:
    return pd.date_range(start, periods=n, freq="D").strftime("%Y-%m-%d").tolist()
