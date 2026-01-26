from playwright.sync_api import sync_playwright


def get_jsessionid(headless: bool = False, slow_mo: int = 400) -> tuple[str | None, str | None]:
    """
    Launches a Chromium browser, navigates to the MutualFundTools login page,
    waits for manual CAPTCHA entry and login, then extracts and returns:
      - JSESSIONID
      - session_cookie (if present)
    The function also persists the Playwright storage state to 'mft_session.json'.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slow_mo)
        context = browser.new_context()
        page = context.new_page()

        page.goto("https://www.mutualfundtools.com/login", wait_until="networkidle")

        # Fill login fields (adjust as needed)
        page.fill("#txt_mobile", "MOBILE_PHONE")
        page.fill("#txt_password", "PWD")

        print("👉 Enter CAPTCHA manually and click LOGIN")

        # Wait until login completes (adjust URL pattern if the site changes)
        page.wait_for_url("**/advisorytools**", timeout=0)

        # Extract cookies
        cookies = context.cookies()
        jsessionid = next((c["value"] for c in cookies if c.get("name") == "JSESSIONID"), None)
        session_cookie = next((c["value"] for c in cookies if c.get("name") == "session_cookie"), None)

        # Persist session to reuse later if desired
        context.storage_state(path="mft_session.json")

        browser.close()
        return jsessionid, session_cookie


if __name__ == "__main__":
    js, sess = get_jsessionid(headless=False)
    print(f"JSESSIONID={js}")
    if sess:
        print(f"session_cookie={sess}")
