import argparse
import glob
import os
import subprocess
import sys


def latest_csv(pattern: str) -> str | None:
    matches = glob.glob(pattern)
    if not matches:
        return None
    return max(matches, key=os.path.getmtime)


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
    parser_script = os.path.join(project_root, "src", "stock_scraper", "advisor_parser_new.py")
    fixtures_dir = os.path.join(project_root, "tests", "fixtures")
    default_consolidated = os.path.join(fixtures_dir, "consolidated-mft-returns.xls")
    default_benchmark_returns = os.path.join(fixtures_dir, "benchmark-returns.xls")
    default_category_monitor = os.path.join(fixtures_dir, "category-monitor.xls")
    default_mftools_json = os.path.join(project_root, "resources", "funds_and_categories_with_mftools.json")

    parser = argparse.ArgumentParser(
        description="Test runner for advisor-parser-new using a provided consolidated XLS/XLSX."
    )
    parser.add_argument(
        "--consolidated",
        default=default_consolidated,
        help="Path to consolidated XLS/XLSX input",
    )
    parser.add_argument(
        "--mftools-json",
        default=default_mftools_json,
        help="Path to funds_and_categories_with_mftools.json",
    )
    parser.add_argument(
        "--benchmark-returns",
        default=default_benchmark_returns,
        help="Path to benchmark-returns XLS/XLSX input",
    )
    parser.add_argument(
        "--category-monitor",
        default=default_category_monitor,
        help="Path to category-monitor XLS/XLSX input",
    )
    args = parser.parse_args()

    if not os.path.exists(args.consolidated):
        raise FileNotFoundError(f"Consolidated file not found: {args.consolidated}")
    if os.path.getsize(args.consolidated) <= 0:
        raise RuntimeError(f"Consolidated file is empty: {args.consolidated}")
    if not os.path.exists(args.mftools_json):
        raise FileNotFoundError(f"Mapping json not found: {args.mftools_json}")
    if not os.path.exists(args.benchmark_returns):
        raise FileNotFoundError(f"Benchmark returns file not found: {args.benchmark_returns}")
    if os.path.getsize(args.benchmark_returns) <= 0:
        raise RuntimeError(f"Benchmark returns file is empty: {args.benchmark_returns}")
    if not os.path.exists(args.category_monitor):
        raise FileNotFoundError(f"Category monitor file not found: {args.category_monitor}")
    if os.path.getsize(args.category_monitor) <= 0:
        raise RuntimeError(f"Category monitor file is empty: {args.category_monitor}")

    pattern = os.path.join(project_root, "fund-stats_*.csv")
    before_latest = latest_csv(pattern)

    cmd = [
        sys.executable,
        parser_script,
        "--consolidated",
        args.consolidated,
        "--mftools-json",
        args.mftools_json,
        "--benchmark-returns",
        args.benchmark_returns,
        "--category-monitor",
        args.category_monitor,
    ]
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd=project_root)

    after_latest = latest_csv(pattern)
    if after_latest is None:
        raise RuntimeError("No output CSV found after run.")
    if before_latest and os.path.abspath(before_latest) == os.path.abspath(after_latest):
        print(f"Run completed. Latest output unchanged: {after_latest}")
    else:
        print(f"Run completed. Output file: {after_latest}")


if __name__ == "__main__":
    main()
