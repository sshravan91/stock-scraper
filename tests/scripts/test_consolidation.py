import argparse
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from stock_scraper.mftreturnsconsolidator import consolidate_mft_returns

DEFAULT_TRAILING = os.path.join(PROJECT_ROOT, "tests", "fixtures", "trailing-returns.xls")
DEFAULT_RISK = os.path.join(PROJECT_ROOT, "tests", "fixtures", "risk-ratios.xls")
DEFAULT_OUT = os.path.join(PROJECT_ROOT, "tests", "fixtures", "consolidated-mft-returns-test.xls")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run consolidation using local trailing-returns and risk-ratios files only."
    )
    parser.add_argument("--trailing", default=DEFAULT_TRAILING, help="Path to trailing returns XLS/XLSX")
    parser.add_argument("--risk", default=DEFAULT_RISK, help="Path to risk ratios XLS/XLSX")
    parser.add_argument("--out", default=DEFAULT_OUT, help="Path for consolidated XLS output")
    args = parser.parse_args()

    for label, path in (("trailing", args.trailing), ("risk", args.risk)):
        if not os.path.exists(path):
            raise FileNotFoundError(f"{label} file not found: {path}")
        if os.path.getsize(path) <= 0:
            raise RuntimeError(f"{label} file is empty: {path}")

    saved = consolidate_mft_returns(args.trailing, args.risk, args.out)
    print(f"Consolidation complete: {saved}")


if __name__ == "__main__":
    main()
