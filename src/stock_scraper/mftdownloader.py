import argparse
import os
import re

import requests


REPORT_SPECS = {
    "risk-ratios": {
        "url": (
            "https://www.mutualfundtools.com/admin-common/download/reportbuilder/downloadRiskRatiosReport"
            "?name=5%20Largest%20Fund%20Houses%2C10%20Largest%20Fund%20Houses%2C15%20Largest%20Fund%20Houses%2C360%20ONE%20Mutual%20Fund%2CAbakkus%20Mutual%20Fund%2CAditya%20Birla%20Sun%20Life%20Mutual%20Fund%2CAngel%20One%20Mutual%20Fund%2CAxis%20Mutual%20Fund%2CBajaj%20Finserv%20Mutual%20Fund%2CBandhan%20Mutual%20Fund%2CBank%20of%20India%20Mutual%20Fund%2CBaroda%20BNP%20Paribas%20Mutual%20Fund%2CCanara%20Robeco%20Mutual%20Fund%2CCapitalmind%20Mutual%20Fund%2CChoice%20Mutual%20Fund%2CDSP%20Mutual%20Fund%2CEdelweiss%20Mutual%20Fund%2CFranklin%20Templeton%20Mutual%20Fund%2CGroww%20Mutual%20Fund%2CHDFC%20Mutual%20Fund%2CHelios%20Mutual%20Fund%2CHSBC%20Mutual%20Fund%2CICICI%20Prudential%20Mutual%20Fund%2CInvesco%20Mutual%20Fund%2CITI%20Mutual%20Fund%2CJio%20BlackRock%20Mutual%20Fund%2CJM%20Financial%20Mutual%20Fund%2CKotak%20Mahindra%20Mutual%20Fund%2CLIC%20Mutual%20Fund%2CMahindra%20Manulife%20Mutual%20Fund%2CMirae%20Asset%20Mutual%20Fund%2CMotilal%20Oswal%20Mutual%20Fund%2CNavi%20Mutual%20Fund%2CNippon%20India%20Mutual%20Fund%2CNJ%20Mutual%20Fund%2COld%20Bridge%20Mutual%20Fund%2CPGIM%20India%20Mutual%20Fund%2CPPFAS%20Mutual%20Fund%2CQuant%20Mutual%20Fund%2CQuantum%20Mutual%20Fund%2CSamco%20Mutual%20Fund%2CSBI%20Mutual%20Fund%2CShriram%20Mutual%20Fund%2CSundaram%20Mutual%20Fund%2CTata%20Mutual%20Fund%2CTaurus%20Mutual%20Fund%2CThe%20Wealth%20Company%20Mutual%20Fund%2CTrust%20Mutual%20Fund%2CUnifi%20Mutual%20Fund%2CUnion%20Mutual%20Fund%2CUTI%20Mutual%20Fund%2CWhiteOak%20Capital%20Mutual%20Fund%2CZerodha%20Mutual%20Fund"
            "&category=Equity:%20All,Equity:%20Contra,Equity:%20Dividend%20Yield,Equity:%20ELSS,Equity:%20Flexi%20Cap,Equity:%20Focused,Equity:%20Large%20and%20Mid%20Cap,Equity:%20Large%20Cap,Equity:%20Mid%20Cap,Equity:%20Multi%20Cap,Equity:%20Sectoral-Banking%20and%20Financial%20Services,Equity:%20Sectoral-FMCG,Equity:%20Sectoral-Infrastructure,Equity:%20Sectoral-Pharma%20and%20Healthcare,Equity:%20Sectoral-Technology,Equity:%20Small%20Cap,Equity:%20Thematic-Active-Momentum,Equity:%20Thematic-Business%20Cycle,Equity:%20Thematic-Consumption,Equity:%20Thematic-Energy,Equity:%20Thematic-ESG,Equity:%20Thematic-Innovation,Equity:%20Thematic-International,Equity:%20Thematic-Manufacturing,Equity:%20Thematic-MNC,Equity:%20Thematic-Multi-Sector,Equity:%20Thematic-Others,Equity:%20Thematic-PSU,Equity:%20Thematic-Quantitative,Equity:%20Thematic-Special-Opportunities,Equity:%20Thematic-Transportation,Equity:%20Value"
            "&fieldlist=Volatility,Sharpe%20Ratio,Beta,Alpha,Up%20Market%20Capture%20Ratio,Down%20Market%20Capture%20Ratio,Mean,Sortino%20Ratio,Quartile%20Rank,PY%20Quartile%20Rank,Maximum%20Drawdown,R-Squared,Information%20Ratio,Treynor%20Ratio,AUM,Expense%20Ratio,Riskometer,Fund%20Manager"
        ),
        "referer": "https://www.mutualfundtools.com/report-builder/risk-ratios",
        "default_out": "risk-ratios.xls",
        "label": "Risk ratios",
    },
    "benchmark-returns": {
        "url": "https://www.mutualfundtools.com/mutual-funds-research/downloadBenchmarkMonitorXL?option=Trailing",
        "referer": "https://www.mutualfundtools.com/mutual-funds-research/mutual-fund-benchmark-monitor",
        "default_out": "benchmark-returns.xls",
        "label": "Benchmark returns",
    },
    "category-monitor": {
        "url": (
            "https://www.mutualfundtools.com/mutual-funds-research/downloadMutualFundCategoryMonitorXL"
            "?option=Trailing&broad_category=All"
        ),
        "referer": "https://www.mutualfundtools.com/mutual-funds-research/mutual-fund-category-monitor",
        "default_out": "category-monitor.xls",
        "label": "Category monitor",
    },
    "trailing-returns": {
        "url": "https://www.mutualfundtools.com/admin-common/download/reportbuilder/downloadTrailingReturnsXl",
        "method": "POST",
        "headers": {
            "Accept": "*/*",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://www.mutualfundtools.com",
            "X-Requested-With": "XMLHttpRequest",
        },
        "data": (
            "amc=360+ONE+Mutual+Fund%2C360+ONE+Mutual+Fund+SIF%2CAbakkus+Mutual+Fund%2CAditya+Birla+Sun+Life+Mutual+Fund%2CAditya+Birla+Sun+Life+Mutual+Fund+SIF%2CAngel+One+Mutual+Fund%2CAxis+Mutual+Fund%2CBajaj+Finserv+Mutual+Fund%2CBandhan+Mutual+Fund%2CBandhan+Mutual+Fund+SIF%2CBank+of+India+Mutual+Fund%2CBaroda+BNP+Paribas+Mutual+Fund%2CCanara+Robeco+Mutual+Fund%2CCapitalmind+Mutual+Fund%2CChoice+Mutual+Fund%2CDSP+Mutual+Fund%2CEdelweiss+Mutual+Fund%2CEdelweiss+Mutual+Fund+SIF%2CFranklin+Templeton+Mutual+Fund%2CGroww+Mutual+Fund%2CHDFC+Mutual+Fund%2CHelios+Mutual+Fund%2CHSBC+Mutual+Fund%2CICICI+Prudential+Mutual+Fund%2CICICI+Prudential+Mutual+Fund+SIF%2CInvesco+Mutual+Fund%2CITI+Mutual+Fund%2CITI+Mutual+Fund+SIF%2CJio+BlackRock+Mutual+Fund%2CJM+Financial+Mutual+Fund%2CKotak+Mahindra+Mutual+Fund%2CLIC+Mutual+Fund%2CMahindra+Manulife+Mutual+Fund%2CMirae+Asset+Mutual+Fund%2CMotilal+Oswal+Mutual+Fund%2CNavi+Mutual+Fund%2CNippon+India+Mutual+Fund%2CNJ+Mutual+Fund%2COld+Bridge+Mutual+Fund%2CPGIM+India+Mutual+Fund%2CPPFAS+Mutual+Fund%2CQuant+Mutual+Fund%2CQuant+Mutual+Fund+SIF%2CQuantum+Mutual+Fund%2CSamco+Mutual+Fund%2CSBI+Mutual+Fund%2CSBI+Mutual+Fund+SIF%2CShriram+Mutual+Fund%2CSundaram+Mutual+Fund%2CTata+Mutual+Fund%2CTata+Mutual+Fund+SIF%2CTaurus+Mutual+Fund%2CThe+Wealth+Company+Mutual+Fund%2CTrust+Mutual+Fund%2CUnifi+Mutual+Fund%2CUnion+Mutual+Fund%2CUTI+Mutual+Fund%2CWhiteOak+Capital+Mutual+Fund%2CZerodha+Mutual+Fund"
            "&category=Equity%3A+All%2CDebt%3A+All%2CHybrid%3A+All%2CSolution+Oriented%3A+All%2COthers%3A+All%2CChildrens+Fund%2CDebt%3A+Banking+and+PSU%2CDebt%3A+Corporate+Bond%2CDebt%3A+Credit+Risk%2CDebt%3A+Dynamic+Bond%2CDebt%3A+Floater%2CDebt%3A+Gilt%2CDebt%3A+Gilt+Fund+with+10+year+constant+duration%2CDebt%3A+Liquid%2CDebt%3A+Long+Duration%2CDebt%3A+Low+Duration%2CDebt%3A+Medium+Duration%2CDebt%3A+Medium+to+Long+Duration%2CDebt%3A+Money+Market%2CDebt%3A+Overnight%2CDebt%3A+Short+Duration%2CDebt%3A+Ultra+Short+Duration%2CEquity%3A+Contra%2CEquity%3A+Dividend+Yield%2CEquity%3A+ELSS%2CEquity%3A+Flexi+Cap%2CEquity%3A+Focused%2CEquity%3A+Large+and+Mid+Cap%2CEquity%3A+Large+Cap%2CEquity%3A+Mid+Cap%2CEquity%3A+Multi+Cap%2CEquity%3A+Sectoral-Banking+and+Financial+Services%2CEquity%3A+Sectoral-FMCG%2CEquity%3A+Sectoral-Infrastructure%2CEquity%3A+Sectoral-Pharma+and+Healthcare%2CEquity%3A+Sectoral-Technology%2CEquity%3A+Small+Cap%2CEquity%3A+Thematic-Active-Momentum%2CEquity%3A+Thematic-Business-Cycle%2CEquity%3A+Thematic-Consumption%2CEquity%3A+Thematic-Energy%2CEquity%3A+Thematic-ESG%2CEquity%3A+Thematic-Innovation%2CEquity%3A+Thematic-International%2CEquity%3A+Thematic-Manufacturing%2CEquity%3A+Thematic-MNC%2CEquity%3A+Thematic-Multi-Sector%2CEquity%3A+Thematic-Others%2CEquity%3A+Thematic-PSU%2CEquity%3A+Thematic-Quantitative%2CEquity%3A+Thematic-Special-Opportunities%2CEquity%3A+Thematic-Transportation%2CEquity%3A+Value%2CETFs%2CFund+of+Funds-Domestic-Debt%2CFund+of+Funds-Domestic-Equity%2CFund+of+Funds-Domestic-Gold%2CFund+of+Funds-Domestic-Gold+and+Silver%2CFund+of+Funds-Domestic-Hybrid%2CFund+of+Funds-Domestic-Silver%2CFund+of+Funds-Income+Plus+Arbitrage%2CFund+of+Funds-Overseas%2CHybrid%3A+Aggressive%2CHybrid%3A+Arbitrage%2CHybrid%3A+Balanced%2CHybrid%3A+Conservative%2CHybrid%3A+Dynamic+Asset+Allocation%2CHybrid%3A+Equity+Savings%2CHybrid%3A+Multi+Asset+Allocation%2CIndex+Fund%2CRetirement+Fund"
            "&fieldlist=1+Year+Return%2C3+Year+Return%2C5+Year+Return%2C10+Year+Return%2C"
        ),
        "referer": (
            "https://www.mutualfundtools.com/report-builder/trailing-returns?name=5%20Largest%20Fund%20Houses%2C10%20Largest%20Fund%20Houses%2C15%20Largest%20Fund%20Houses%2C360%20ONE%20Mutual%20Fund%2CAbakkus%20Mutual%20Fund%2CAditya%20Birla%20Sun%20Life%20Mutual%20Fund%2CAngel%20One%20Mutual%20Fund%2CAxis%20Mutual%20Fund%2CBajaj%20Finserv%20Mutual%20Fund%2CBandhan%20Mutual%20Fund%2CBank%20of%20India%20Mutual%20Fund%2CBaroda%20BNP%20Paribas%20Mutual%20Fund%2CCanara%20Robeco%20Mutual%20Fund%2CCapitalmind%20Mutual%20Fund%2CChoice%20Mutual%20Fund%2CDSP%20Mutual%20Fund%2CEdelweiss%20Mutual%20Fund%2CFranklin%20Templeton%20Mutual%20Fund%2CGroww%20Mutual%20Fund%2CHDFC%20Mutual%20Fund%2CHelios%20Mutual%20Fund%2CHSBC%20Mutual%20Fund%2CICICI%20Prudential%20Mutual%20Fund%2CInvesco%20Mutual%20Fund%2CITI%20Mutual%20Fund%2CJio%20BlackRock%20Mutual%20Fund%2CJM%20Financial%20Mutual%20Fund%2CKotak%20Mahindra%20Mutual%20Fund%2CLIC%20Mutual%20Fund%2CMahindra%20Manulife%20Mutual%20Fund%2CMirae%20Asset%20Mutual%20Fund%2CMotilal%20Oswal%20Mutual%20Fund%2CNavi%20Mutual%20Fund%2CNippon%20India%20Mutual%20Fund%2CNJ%20Mutual%20Fund%2COld%20Bridge%20Mutual%20Fund%2CPGIM%20India%20Mutual%20Fund%2CPPFAS%20Mutual%20Fund%2CQuant%20Mutual%20Fund%2CQuantum%20Mutual%20Fund%2CSamco%20Mutual%20Fund%2CSBI%20Mutual%20Fund%2CShriram%20Mutual%20Fund%2CSundaram%20Mutual%20Fund%2CTata%20Mutual%20Fund%2CTaurus%20Mutual%20Fund%2CThe%20Wealth%20Company%20Mutual%20Fund%2CTrust%20Mutual%20Fund%2CUnifi%20Mutual%20Fund%2CUnion%20Mutual%20Fund%2CUTI%20Mutual%20Fund%2CWhiteOak%20Capital%20Mutual%20Fund%2CZerodha%20Mutual%20Fund&category=Equity:%20All,Debt:%20All,Hybrid:%20All,Solution%20Oriented:%20All,Others:%20All,Childrens%20Fund,Debt:%20Banking%20and%20PSU,Debt:%20Corporate%20Bond,Debt:%20Credit%20Risk,Debt:%20Dynamic%20Bond,Debt:%20Floater,Debt:%20Gilt,Debt:%20Gilt%20Fund%20with%2010%20year%20constant%20duration,Debt:%20Liquid,Debt:%20Long%20Duration,Debt:%20Low%20Duration,Debt:%20Medium%20Duration,Debt:%20Medium%20to%20Long%20Duration,Debt:%20Money%20Market,Debt:%20Overnight,Debt:%20Short%20Duration,Debt:%20Ultra%20Short%20Duration,Equity:%20Contra,Equity:%20Dividend%20Yield,Equity:%20ELSS,Equity:%20Flexi%20Cap,Equity:%20Focused,Equity:%20Large%20and%20Mid%20Cap,Equity:%20Large%20Cap,Equity:%20Mid%20Cap,Equity:%20Multi%20Cap,Equity:%20Sectoral-Banking%20and%20Financial%20Services,Equity:%20Sectoral-FMCG,Equity:%20Sectoral-Infrastructure,Equity:%20Sectoral-Pharma%20and%20Healthcare,Equity:%20Sectoral-Technology,Equity:%20Small%20Cap,Equity:%20Thematic-Active-Momentum,Equity:%20Thematic-Business-Cycle,Equity:%20Thematic-Consumption,Equity:%20Thematic-Energy,Equity:%20Thematic-ESG,Equity:%20Thematic-Innovation,Equity:%20Thematic-International,Equity:%20Thematic-Manufacturing,Equity:%20Thematic-MNC,Equity:%20Thematic-Multi-Sector,Equity:%20Thematic-Others,Equity:%20Thematic-PSU,Equity:%20Thematic-Quantitative,Equity:%20Thematic-Special-Opportunities,Equity:%20Thematic-Transportation,Equity:%20Value,ETFs,Fund%20of%20Funds-Domestic-Debt,Fund%20of%20Funds-Domestic-Equity,Fund%20of%20Funds-Domestic-Gold,Fund%20of%20Funds-Domestic-Gold%20and%20Silver,Fund%20of%20Funds-Domestic-Hybrid,Fund%20of%20Funds-Domestic-Silver,Fund%20of%20Funds-Income%20Plus%20Arbitrage,Fund%20of%20Funds-Overseas,Hybrid:%20Aggressive,Hybrid:%20Arbitrage,Hybrid:%20Balanced,Hybrid:%20Conservative,Hybrid:%20Dynamic%20Asset%20Allocation,Hybrid:%20Equity%20Savings,Hybrid:%20Multi%20Asset%20Allocation,Index%20Fund,Retirement%20Fund,SIF%20-%20Equity%20Long%20Short,SIF%20-%20Hybrid%20Long%20Short&fieldlist=1%20Year%20Return,3%20Year%20Return,5%20Year%20Return,10%20Year%20Return,AUM,Expense%20Ratio,Riskometer,Alpha,Beta,Sharp%20Ratio,Standard%20Deviation,YTM,Average%20Maturity,Sortino%20Ratio,Quartile%20Rank,PY%20Quartile%20Rank,R-Squared,Information%20Ratio,Up%20Market%20Capture%20Ratio,Down%20Market%20Capture%20Ratio"
        ),
        "default_out": "trailing-returns.xls",
        "label": "Trailing returns",
    },
}

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
)


def _debug_enabled() -> bool:
    return os.getenv("MFT_DEBUG_DOWNLOAD", "1").strip().lower() not in {"0", "false", "no"}


def _log(msg: str) -> None:
    if _debug_enabled():
        print(f"[mftdownloader] {msg}")


def _maybe_follow_excel_pointer(resp: requests.Response, timeout: int = 300, label: str = "") -> requests.Response:
    """
    Some MFT endpoints return a plain-text URL pointer to the generated XLS file.
    If detected, fetch and return the pointed XLS response.
    """
    content_type = (resp.headers.get("Content-Type") or "").lower()
    text = (resp.text or "").strip()
    preview = text[:200].replace("\n", "\\n")
    print(
        f"{label} first response: status={resp.status_code}, content_type='{content_type}', "
        f"len={len(resp.content)}, preview='{preview}'"
    )

    # Match direct URL pointer even if extra whitespace/newline surrounds it.
    pointer_match = re.fullmatch(r"https?://\S+/resources/excel/\S+\.xls", text, flags=re.IGNORECASE)
    is_url_pointer = resp.status_code == 200 and pointer_match is not None
    if not is_url_pointer:
        print(f"{label} pointer detection: not a pointer response; using original body as file content.")
        return resp
    pointer_url = pointer_match.group(0)
    print(f"{label} pointer detection: URL found -> {pointer_url}")
    followed = requests.get(pointer_url, timeout=timeout)
    followed_ct = (followed.headers.get("Content-Type") or "").lower()
    print(
        f"{label} followed response: status={followed.status_code}, content_type='{followed_ct}', "
        f"len={len(followed.content)}"
    )
    return followed


def download_report(
    report: str,
    jsessionid: str,
    session_cookie: str | None = None,
    output_path: str | None = None,
) -> str:
    if report not in REPORT_SPECS:
        raise ValueError(f"Unsupported report '{report}'. Valid: {', '.join(REPORT_SPECS)}")
    if not jsessionid or not isinstance(jsessionid, str):
        raise ValueError("Valid jsessionid is required")

    spec = REPORT_SPECS[report]
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": spec["referer"],
        "User-Agent": USER_AGENT,
    }
    headers.update(spec.get("headers", {}))
    cookies = {"JSESSIONID": jsessionid}
    if session_cookie:
        cookies["session_cookie"] = session_cookie

    method = str(spec.get("method", "GET")).upper()
    if method == "POST":
        resp = requests.post(
            spec["url"],
            headers=headers,
            cookies=cookies,
            data=spec.get("data"),
            timeout=300,
        )
    else:
        resp = requests.get(spec["url"], headers=headers, cookies=cookies, timeout=300)
    resp = _maybe_follow_excel_pointer(resp, timeout=300, label=spec.get("label", report))
    if resp.status_code != 200:
        raise RuntimeError(f"{spec['label']} download failed with status {resp.status_code}")

    if not output_path:
        output_path = os.path.join(os.getcwd(), spec["default_out"])

    with open(output_path, "wb") as f:
        f.write(resp.content)

    return os.path.abspath(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download MFT XLS reports using cookies.")
    parser.add_argument("--report", required=True, choices=sorted(REPORT_SPECS.keys()))
    parser.add_argument("--jsessionid", required=True, help="JSESSIONID cookie value")
    parser.add_argument("--session-cookie", default=None, help="Optional session_cookie value")
    parser.add_argument("--out", default=None, help="Output XLS path (default depends on report)")
    args = parser.parse_args()

    out = download_report(args.report, args.jsessionid, args.session_cookie, args.out)
    print(f"✅ Saved file: {out}")
