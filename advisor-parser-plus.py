import os
import subprocess
import sys

import login
from riskratiodownloader import download_risk_ratios


def main():
    # 1) Perform login flow to retrieve cookies (visible browser for CAPTCHA)
    jsessionid, session_cookie = login.get_jsessionid(headless=False, slow_mo=400)
    if not jsessionid:
        raise SystemExit("Failed to acquire JSESSIONID (login may have failed). Aborting.")

    # 2) Download the Risk Ratios XLS to ./risk-ratios.xls
    out_path = os.path.join(os.getcwd(), "risk-ratios.xls")
    saved = download_risk_ratios(jsessionid, session_cookie, out_path)
    print(f"✅ Risk ratios downloaded to: {saved}")

    # 3) Invoke advisor-parser-new with the downloaded risk-ratios path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    apn_path = os.path.join(script_dir, "advisor-parser-new.py")
    try:
        subprocess.run([sys.executable, apn_path, "--risk-ratios", saved], check=True)
        print("✅ advisor-parser-new completed")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ advisor-parser-new failed with code {e.returncode}")


if __name__ == "__main__":
    main()
