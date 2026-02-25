import os
import subprocess
import sys

import login
from mftdownloader import download_report
from mftreturnsconsolidator import consolidate_mft_returns


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
    trailing_saved = download_report("trailing-returns", jsessionid, session_cookie, trailing_out_path)
    print(f"✅ Trailing returns downloaded to: {trailing_saved}")

    # Issue with trailing returns download
    # # 6) Consolidate trailing returns + risk ratios into one XLS
    # consolidated_out_path = os.path.join(os.getcwd(), "consolidated-mft-returns.xls")
    # consolidated_saved = consolidate_mft_returns(trailing_saved, saved, consolidated_out_path)
    # print(f"✅ Consolidated returns generated at: {consolidated_saved}")

    # 7) Invoke advisor-parser-new with the downloaded risk-ratios path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    apn_path = os.path.join(script_dir, "advisor-parser-new.py")
    try:
        subprocess.run([sys.executable, apn_path, "--risk-ratios", saved], check=True)
        print("✅ advisor-parser-new completed")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ advisor-parser-new failed with code {e.returncode}")


if __name__ == "__main__":
    main()
