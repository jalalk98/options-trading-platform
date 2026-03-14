"""
refresh_token.py
----------------
Automates Kite Connect login to get a fresh access token every morning.

Reads credentials from ~/.kite_secrets (never from .env or git).
Updates KITE_ACCESS_TOKEN in .env after successful login.

Usage:
    python refresh_token.py

Scheduled via cron at 7:00am IST (1:30am UTC) on weekdays.
"""

import os
import re
import sys
import time
import logging
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(Path.home() / "token_refresh.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
SECRETS_FILE = Path.home() / ".kite_secrets"
ENV_FILE      = Path(__file__).parent / ".env"


def load_secrets() -> dict:
    """Read key=value pairs from ~/.kite_secrets."""
    secrets = {}
    with open(SECRETS_FILE) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            secrets[key.strip()] = value.strip()
    required = {"KITE_USER_ID", "KITE_PASSWORD", "KITE_TOTP_SECRET"}
    missing = required - secrets.keys()
    if missing:
        raise ValueError(f"Missing keys in ~/.kite_secrets: {missing}")
    return secrets


def load_env_credentials() -> dict:
    """Read KITE_API_KEY and KITE_API_SECRET from .env."""
    load_dotenv(ENV_FILE)
    api_key    = os.getenv("KITE_API_KEY")
    api_secret = os.getenv("KITE_API_SECRET")
    if not api_key or not api_secret:
        raise ValueError("KITE_API_KEY / KITE_API_SECRET missing from .env")
    return {"api_key": api_key, "api_secret": api_secret}


def update_env_token(new_token: str):
    """Replace KITE_ACCESS_TOKEN value in .env in-place."""
    text = ENV_FILE.read_text()
    new_text = re.sub(
        r"^(KITE_ACCESS_TOKEN\s*=\s*).*$",
        rf"\g<1>'{new_token}'",
        text,
        flags=re.MULTILINE,
    )
    if new_text == text:
        raise RuntimeError("KITE_ACCESS_TOKEN line not found in .env — cannot update")
    ENV_FILE.write_text(new_text)
    log.info("KITE_ACCESS_TOKEN updated in .env")


def get_request_token(api_key: str, user_id: str, password: str, totp_secret: str) -> str:
    """
    Use a headless Chromium browser to log into Kite and capture the
    request_token from the redirect URL.
    """
    import pyotp
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    login_url = (
        f"https://kite.zerodha.com/connect/login"
        f"?api_key={api_key}&v=3"
    )

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()

        captured_token = None

        def handle_request(req):
            nonlocal captured_token
            url = req.url
            if "request_token=" in url:
                qs = parse_qs(urlparse(url).query)
                tokens = qs.get("request_token", [])
                if tokens:
                    captured_token = tokens[0]

        page.on("request", handle_request)

        log.info("Opening Kite login page …")
        page.goto(login_url, wait_until="networkidle", timeout=30_000)

        # ── Step 1: user ID + password ────────────────────────────────────
        page.fill('input[type="text"]',     user_id)
        page.fill('input[type="password"]', password)
        page.click('button[type="submit"]')
        log.info("Submitted user ID + password")

        # ── Step 2: TOTP ──────────────────────────────────────────────────
        # Wait for TOTP input to appear
        try:
            page.wait_for_selector('input[type="number"]', timeout=15_000)
        except PWTimeout:
            page.screenshot(path=str(Path.home() / "kite_login_debug.png"))
            raise RuntimeError("TOTP input did not appear — check credentials or screenshot ~/kite_login_debug.png")

        totp_code = pyotp.TOTP(totp_secret).now()
        log.info(f"Generated TOTP: {totp_code}")
        page.fill('input[type="number"]', totp_code)
        page.click('button[type="submit"]')
        log.info("Submitted TOTP")

        # ── Wait for redirect with request_token ──────────────────────────
        deadline = time.time() + 20
        while not captured_token and time.time() < deadline:
            time.sleep(0.2)

        browser.close()

    if not captured_token:
        raise RuntimeError("request_token not captured — login may have failed")

    log.info(f"Captured request_token: {captured_token[:8]}…")
    return captured_token


def exchange_for_access_token(api_key: str, api_secret: str, request_token: str) -> str:
    """Exchange request_token for an access_token via the Kite API."""
    import hashlib
    import requests

    checksum = hashlib.sha256(f"{api_key}{request_token}{api_secret}".encode()).hexdigest()
    resp = requests.post(
        "https://api.kite.trade/session/token",
        data={
            "api_key":       api_key,
            "request_token": request_token,
            "checksum":      checksum,
        },
        headers={"X-Kite-Version": "3"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    access_token = data["data"]["access_token"]
    log.info(f"Access token received: {access_token[:8]}…")
    return access_token


def main():
    log.info("=== Kite token refresh started ===")
    try:
        secrets = load_secrets()
        env     = load_env_credentials()

        request_token = get_request_token(
            api_key     = env["api_key"],
            user_id     = secrets["KITE_USER_ID"],
            password    = secrets["KITE_PASSWORD"],
            totp_secret = secrets["KITE_TOTP_SECRET"],
        )

        access_token = exchange_for_access_token(
            api_key       = env["api_key"],
            api_secret    = env["api_secret"],
            request_token = request_token,
        )

        update_env_token(access_token)
        log.info("=== Token refresh SUCCESSFUL ===")

    except Exception as e:
        log.error(f"=== Token refresh FAILED: {e} ===")
        sys.exit(1)


if __name__ == "__main__":
    main()
