import os

from stock_scraper import login
from stock_scraper.advisor_parser_new import run as run_advisor_parser
from stock_scraper.mftdownloader import download_report
from stock_scraper.mftreturnsconsolidator import consolidate_mft_returns


def file_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return -1


def main():
    # 1) Perform login flow to retrieve cookies (visible browser for CAPTCHA)
    jsessionid, session_cookie = login.get_jsessionid(headless=False, slow_mo=400)
    if not jsessionid:
        print("❌ Login failed: could not acquire JSESSIONID.")
        raise SystemExit("Failed to acquire JSESSIONID (login may have failed). Aborting.")
    print("✅ Login successful: session cookies acquired.")

    # 2) Download the Risk Ratios XLS to ./risk-ratios.xls
    out_path = os.path.join(os.getcwd(), "risk-ratios.xls")
    saved = download_report("risk-ratios", jsessionid, session_cookie, out_path)
    print(f"✅ Risk ratios downloaded to: {saved}")

    # 3) Download benchmark returns XLS to ./benchmark-returns.xls
    benchmark_out_path = os.path.join(os.getcwd(), "benchmark-returns.xls")
    benchmark_saved = download_report("benchmark-returns", jsessionid, session_cookie, benchmark_out_path)
    print(f"✅ Benchmark returns downloaded to: {benchmark_saved}")

    # 4) Download category monitor XLS to ./category-monitor.xls
    category_out_path = os.path.join(os.getcwd(), "category-monitor.xls")
    category_saved = download_report("category-monitor", jsessionid, session_cookie, category_out_path)
    print(f"✅ Category monitor downloaded to: {category_saved}")

    # 5) Download trailing returns XLS to ./trailing-returns.xls
    trailing_out_path = os.path.join(os.getcwd(), "trailing-returns.xls")
    trailing_candidate = download_report("trailing-returns", jsessionid, session_cookie, trailing_out_path)
    print(f"✅ Trailing returns downloaded to: {trailing_candidate}")

    # 6) Consolidate trailing returns + risk ratios into one XLS
    consolidated_out_path = os.path.join(os.getcwd(), "consolidated-mft-returns.xls")
    consolidated_saved = consolidate_mft_returns(trailing_candidate, saved, consolidated_out_path)
    print(f"✅ Consolidated returns generated at: {consolidated_saved}")

    # 7) Invoke advisor parser with consolidated + benchmark + category monitor
    mftools_json = os.path.join(os.getcwd(), "resources", "funds_and_categories_with_mftools.json")
    run_advisor_parser(
        consolidated=consolidated_saved,
        benchmark_returns=benchmark_saved,
        category_monitor=category_saved,
        mftools_json=mftools_json,
    )
    print("✅ advisor-parser-new completed")


if __name__ == "__main__":
    main()
