import csv
from pathlib import Path
from typing import Iterable


def ensure_csv(path: Path, headers: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(headers)


def append_csv(path: Path, row: Iterable[str | int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)
