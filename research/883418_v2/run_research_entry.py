from __future__ import annotations

"""Validated entry point for the 883418 V2 study.

The main research implementation is kept readable in ``run_research.py``.
This entry point applies narrowly scoped compatibility fixes before executing
it, so a long market-data download does not fail on a pandas index/column name
ambiguity after the download has completed.
"""

from pathlib import Path
import runpy

ROOT = Path(__file__).resolve().parent
MAIN = ROOT / "run_research.py"
STREAM = ROOT / "run_research_streaming.py"

text = MAIN.read_text(encoding="utf-8")
old = 'base.sort_values(["total_mv", "ts_code"]).iterrows()'
new = 'base.reset_index(drop=True).sort_values(["total_mv", "ts_code"]).iterrows()'
if old in text:
    MAIN.write_text(text.replace(old, new), encoding="utf-8")
elif new not in text:
    raise RuntimeError("Expected pandas compatibility patch target was not found")

runpy.run_path(str(STREAM), run_name="__main__")
