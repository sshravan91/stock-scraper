import os
import requests


RISK_RATIOS_URL = (
    "https://www.mutualfundtools.com/admin-common/download/reportbuilder/downloadRiskRatiosReport"
    "?name=5%20Largest%20Fund%20Houses%2C10%20Largest%20Fund%20Houses%2C15%20Largest%20Fund%20Houses%2C360%20ONE%20Mutual%20Fund%2CAbakkus%20Mutual%20Fund%2CAditya%20Birla%20Sun%20Life%20Mutual%20Fund%2CAngel%20One%20Mutual%20Fund%2CAxis%20Mutual%20Fund%2CBajaj%20Finserv%20Mutual%20Fund%2CBandhan%20Mutual%20Fund%2CBank%20of%20India%20Mutual%20Fund%2CBaroda%20BNP%20Paribas%20Mutual%20Fund%2CCanara%20Robeco%20Mutual%20Fund%2CCapitalmind%20Mutual%20Fund%2CChoice%20Mutual%20Fund%2CDSP%20Mutual%20Fund%2CEdelweiss%20Mutual%20Fund%2CFranklin%20Templeton%20Mutual%20Fund%2CGroww%20Mutual%20Fund%2CHDFC%20Mutual%20Fund%2CHelios%20Mutual%20Fund%2CHSBC%20Mutual%20Fund%2CICICI%20Prudential%20Mutual%20Fund%2CInvesco%20Mutual%20Fund%2CITI%20Mutual%20Fund%2CJio%20BlackRock%20Mutual%20Fund%2CJM%20Financial%20Mutual%20Fund%2CKotak%20Mahindra%20Mutual%20Fund%2CLIC%20Mutual%20Fund%2CMahindra%20Manulife%20Mutual%20Fund%2CMirae%20Asset%20Mutual%20Fund%2CMotilal%20Oswal%20Mutual%20Fund%2CNavi%20Mutual%20Fund%2CNippon%20India%20Mutual%20Fund%2CNJ%20Mutual%20Fund%2COld%20Bridge%20Mutual%20Fund%2CPGIM%20India%20Mutual%20Fund%2CPPFAS%20Mutual%20Fund%2CQuant%20Mutual%20Fund%2CQuantum%20Mutual%20Fund%2CSamco%20Mutual%20Fund%2CSBI%20Mutual%20Fund%2CShriram%20Mutual%20Fund%2CSundaram%20Mutual%20Fund%2CTata%20Mutual%20Fund%2CTaurus%20Mutual%20Fund%2CThe%20Wealth%20Company%20Mutual%20Fund%2CTrust%20Mutual%20Fund%2CUnifi%20Mutual%20Fund%2CUnion%20Mutual%20Fund%2CUTI%20Mutual%20Fund%2CWhiteOak%20Capital%20Mutual%20Fund%2CZerodha%20Mutual%20Fund"
    "&category=Equity:%20All,Equity:%20Contra,Equity:%20Dividend%20Yield,Equity:%20ELSS,Equity:%20Flexi%20Cap,Equity:%20Focused,Equity:%20Large%20and%20Mid%20Cap,Equity:%20Large%20Cap,Equity:%20Mid%20Cap,Equity:%20Multi%20Cap,Equity:%20Sectoral-Banking%20and%20Financial%20Services,Equity:%20Sectoral-FMCG,Equity:%20Sectoral-Infrastructure,Equity:%20Sectoral-Pharma%20and%20Healthcare,Equity:%20Sectoral-Technology,Equity:%20Small%20Cap,Equity:%20Thematic-Active-Momentum,Equity:%20Thematic-Business%20Cycle,Equity:%20Thematic-Consumption,Equity:%20Thematic-Energy,Equity:%20Thematic-ESG,Equity:%20Thematic-Innovation,Equity:%20Thematic-International,Equity:%20Thematic-Manufacturing,Equity:%20Thematic-MNC,Equity:%20Thematic-Multi-Sector,Equity:%20Thematic-Others,Equity:%20Thematic-PSU,Equity:%20Thematic-Quantitative,Equity:%20Thematic-Special-Opportunities,Equity:%20Thematic-Transportation,Equity:%20Value"
    "&fieldlist=Volatility,Sharpe%20Ratio,Beta,Alpha,Up%20Market%20Capture%20Ratio,Down%20Market%20Capture%20Ratio,Mean,Sortino%20Ratio,Quartile%20Rank,PY%20Quartile%20Rank,Maximum%20Drawdown,R-Squared,Information%20Ratio,Treynor%20Ratio,AUM,Expense%20Ratio,Riskometer,Fund%20Manager"
)

DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.mutualfundtools.com/report-builder/risk-ratios",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
}


def download_risk_ratios(jsessionid: str, session_cookie: str | None = None, output_path: str | None = None) -> str:
    """
    Downloads the Risk Ratios XLS using provided session cookies.

    Args:
        jsessionid: Required JSESSIONID cookie value.
        session_cookie: Optional session_cookie value from site.
        output_path: Optional explicit output path (defaults to ./risk-ratios.xls)

    Returns:
        The absolute path to the saved XLS file.

    Raises:
        RuntimeError on non-200 HTTP status.
    """
    if not jsessionid or not isinstance(jsessionid, str):
        raise ValueError("Valid jsessionid is required")

    cookies = {"JSESSIONID": jsessionid}
    if session_cookie:
        cookies["session_cookie"] = session_cookie

    resp = requests.get(RISK_RATIOS_URL, headers=DEFAULT_HEADERS, cookies=cookies, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"Download failed with status {resp.status_code}")

    if not output_path:
        output_path = os.path.join(os.getcwd(), "risk-ratios.xls")

    with open(output_path, "wb") as f:
        f.write(resp.content)

    return os.path.abspath(output_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download Risk Ratios XLS using MFT cookies.")
    parser.add_argument("--jsessionid", required=True, help="JSESSIONID cookie value")
    parser.add_argument("--session-cookie", default=None, help="Optional session_cookie value")
    parser.add_argument("--out", default="risk-ratios.xls", help="Output XLS path (default: risk-ratios.xls)")
    args = parser.parse_args()

    out = download_risk_ratios(args.jsessionid, args.session_cookie, args.out)
    print(f"✅ Saved file: {out}")
