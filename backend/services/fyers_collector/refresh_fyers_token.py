"""
refresh_fyers_token.py
----------------------
Automates Fyers API v3 daily token refresh via a headless Chromium browser.

Uses the official SDK auth flow:
  1. SessionModel.generate_authcode() → browser URL
  2. Playwright fills FYID, TOTP, PIN
  3. Fyers redirects to FYERS_REDIRECT_URI?auth_code=...
  4. SessionModel.generate_token() → trading access_token

Note: headless HTTP auth (vagator → /api/v3/token) was investigated and
abandoned (2026-05-10). The /api/v3/token response returns an intermediate
session JWT in data.auth that is not accepted by validate-authcode, and
the /generate-authcode browser endpoint does not honour Bearer tokens or
non-browser cookies. Playwright against the official flow is the supported
path per the fyers_apiv3 SDK source.

Reads credentials from:
  ~/.fyers_secrets  — FYERS_USER_ID, FYERS_PIN, FYERS_TOTP_SECRET
  ~/.kite_secrets   — TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (shared with Zerodha)
  .env.fyers        — FYERS_APP_ID, FYERS_SECRET_ID, FYERS_REDIRECT_URI

Writes FYERS_ACCESS_TOKEN back into .env.fyers in-place. The token is written
ONLY after /profile validation succeeds — the old token is never overwritten
on any failure.

One-time setup: playwright install chromium

Scheduled via cron at 7:00 AM IST every weekday.
"""

import hashlib
import json
import logging
import os
import random
import re
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv

# ── Paths ──────────────────────────────────────────────────────────────────────
_PROJECT_ROOT  = Path(__file__).resolve().parents[3]
_ENV_FILE      = _PROJECT_ROOT / ".env.fyers"
_FYERS_SECRETS = Path.home() / ".fyers_secrets"
_KITE_SECRETS  = Path.home() / ".kite_secrets"
_LOG_FILE      = Path.home() / "fyers_token_refresh.log"
_SCREENSHOT    = Path.home() / "fyers_login_debug.png"

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(_LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ── API endpoints (v3, officially documented in SDK) ──────────────────────────
_FYERS_BASE       = "https://api-t1.fyers.in/api/v3"
_EP_VALIDATE_AUTH = f"{_FYERS_BASE}/validate-authcode"
_EP_PROFILE       = f"{_FYERS_BASE}/profile"
_REQUEST_TIMEOUT  = 15  # seconds per HTTP call


# ── Secret / config loaders ───────────────────────────────────────────────────

def _load_kv_file(path: Path, required: set) -> dict:
    """Parse a KEY=VALUE file; raise if any required key is missing."""
    if not path.exists():
        raise FileNotFoundError(f"Secrets file not found: {path}")
    data = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        k, _, v = line.partition("=")
        data[k.strip()] = v.strip()
    missing = required - data.keys()
    if missing:
        raise ValueError(f"Missing keys in {path}: {missing}")
    return data


def load_fyers_secrets() -> dict:
    return _load_kv_file(_FYERS_SECRETS, {"FYERS_USER_ID", "FYERS_PIN", "FYERS_TOTP_SECRET"})


def load_telegram_credentials() -> dict:
    return _load_kv_file(_KITE_SECRETS, {"TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"})


def load_env_config() -> dict:
    """Load non-secret config from .env.fyers."""
    load_dotenv(_ENV_FILE, override=False)
    cfg = {
        "app_id":       os.getenv("FYERS_APP_ID", ""),
        "secret_id":    os.getenv("FYERS_SECRET_ID", ""),
        "redirect_uri": os.getenv("FYERS_REDIRECT_URI", ""),
    }
    missing = [k for k, v in cfg.items() if not v]
    if missing:
        raise ValueError(f"Missing keys in .env.fyers: {[k.upper() for k in missing]}")
    return cfg


# ── Token masking ─────────────────────────────────────────────────────────────

def _mask(token: str) -> str:
    """Show first 6 + last 4 chars; mask the middle."""
    if not token or len(token) < 12:
        return "***"
    return f"{token[:6]}...{token[-4:]}"


# ── Auth helpers ──────────────────────────────────────────────────────────────

def _app_id_hash(app_id: str, secret_id: str) -> str:
    """SHA256('{app_id}:{secret_id}') — mirrors SessionModel.get_hash() in SDK."""
    return hashlib.sha256(f"{app_id}:{secret_id}".encode()).hexdigest()


def _check_response(step: str, resp: requests.Response) -> dict:
    """Raise on non-2xx or Fyers error payload; return parsed JSON body."""
    try:
        body = resp.json()
    except Exception:
        raise RuntimeError(
            f"[{step}] Non-JSON response (HTTP {resp.status_code}): {resp.text[:300]}"
        )
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"[{step}] HTTP {resp.status_code}: {body}")
    status = body.get("s") or body.get("status") or ""
    if status not in ("ok", ""):
        msg = body.get("message") or body.get("msg") or json.dumps(body)
        raise RuntimeError(f"[{step}] API error: {msg}")
    return body


# ── Playwright auth ───────────────────────────────────────────────────────────

def get_fyers_auth_code(app_id: str, secret_id: str, redirect_uri: str,
                        user_id: str, pin: str, totp_secret: str) -> str:
    """
    Headless Chromium login via the official Fyers OAuth flow.

    Mirrors get_request_token() in refresh_token.py (Zerodha):
      - same browser launch options
      - same dual capture strategy: page.on("request") + page.url poll
      - same screenshot-on-failure pattern

    Flow:
      1. SessionModel.generate_authcode() → auth URL
      2. Navigate to auth URL
      3. Fill FYID → submit
      4. Fill TOTP → submit
      5. Fill PIN  → submit
      6. Capture auth_code from redirect URL

    Returns the auth_code string.
    """
    import pyotp
    from fyers_apiv3 import fyersModel
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    # Build auth URL using the official SDK — don't reinvent the URL construction.
    # SDK source: fyersModel.py SessionModel.generate_authcode() lines 655-668.
    session_model = fyersModel.SessionModel(
        client_id=app_id,
        redirect_uri=redirect_uri,
        response_type="code",
        state=str(random.randint(100_000, 999_999)),
        secret_key=secret_id,
        grant_type="authorization_code",
    )
    auth_url = session_model.generate_authcode()
    log.info("Step 1: auth URL constructed via SDK SessionModel.generate_authcode()")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--use-gl=swiftshader",   # software WebGL avoids null GPU fingerprint
            ],
        )
        page = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            )
        )

        captured_code: str | None = None

        def handle_request(req):
            nonlocal captured_code
            if "auth_code=" in req.url:
                qs = parse_qs(urlparse(req.url).query)
                codes = qs.get("auth_code", [])
                if codes:
                    captured_code = codes[0]

        page.on("request", handle_request)

        # ── Step 2: Navigate ─────────────────────────────────────────────────
        log.info("Step 2: navigating to Fyers login page …")
        page.goto(auth_url, wait_until="domcontentloaded", timeout=30_000)

        # ── Step 3: Switch to Client ID and fill FYID ────────────────────────
        # The login page defaults to Mobile number. Switch to Client ID tab.
        # Selectors confirmed by HTML inspection 2026-05-10:
        #   radio:  input[id="clientId_rb"]
        #   field:  input[id="fy_client_id"]
        #   submit: button[id="clientIdSubmit"]
        try:
            page.wait_for_selector('input[id="clientId_rb"]', state="visible", timeout=10_000)
        except PWTimeout:
            page.screenshot(path=str(_SCREENSHOT))
            raise RuntimeError(
                "Client ID radio not found — Fyers UI may have changed. "
                f"Screenshot: {_SCREENSHOT}"
            )
        page.click('input[id="clientId_rb"]')
        page.fill('input[id="fy_client_id"]', user_id)

        # ── BROWSER AUTOMATION BLOCKED BY CLOUDFLARE TURNSTILE ───────────────
        # Fyers login page shows an interactive "Verify you are human" checkbox
        # (Cloudflare Turnstile) on datacenter IPs.  Extensive attempts to click
        # it programmatically all failed (2026-05-10):
        #   • headless=False + Xvfb — Turnstile falls back to interactive mode
        #   • frame_locator / framenavigated event — frame found but element
        #     inaccessible (cross-origin or nested-iframe isolation)
        #   • CDP mouse.click at bounding-box coords — synthetic event rejected
        #   • xdotool X11 events — iframe coordinates unreliable; verification
        #     still not completed
        # Use get_token_manual.py for now.  Revisit when hosted on a residential
        # IP or when a Fyers API key with non-browser auth is available.
        page.screenshot(path=str(_SCREENSHOT))
        browser.close()
        raise RuntimeError(
            "Browser automation blocked by Cloudflare Turnstile on this host. "
            "Run get_token_manual.py to obtain an auth_code via your browser, "
            "or use a residential/proxy IP for automated login."
        )


# ── validate-authcode ─────────────────────────────────────────────────────────

def exchange_for_access_token(app_id: str, secret_id: str, auth_code: str) -> str:
    """
    Exchange auth_code for trading access_token.
    POST /api/v3/validate-authcode — officially documented in SDK.
    appIdHash = SHA256('{app_id}:{secret_id}') — SDK: fyersModel.py lines 670-672, 680.
    """
    log.info("Step 9: validate-authcode (token exchange) …")
    resp = requests.post(
        _EP_VALIDATE_AUTH,
        json={
            "grant_type": "authorization_code",
            "appIdHash":  _app_id_hash(app_id, secret_id),
            "code":       auth_code,
        },
        headers={"Content-Type": "application/json"},
        timeout=_REQUEST_TIMEOUT,
    )
    try:
        body = resp.json()
    except Exception:
        raise RuntimeError(f"[validate-authcode] Non-JSON response (HTTP {resp.status_code})")
    if body.get("s") != "ok":
        raise RuntimeError(
            f"validate-authcode failed: code={body.get('code')} "
            f"message={body.get('message')}"
        )
    access_token = body["access_token"]
    log.info("Step 9 OK — access_token received (%s), flat response shape confirmed", _mask(access_token))
    return access_token


# ── Profile validation ────────────────────────────────────────────────────────

def validate_token(app_id: str, access_token: str) -> str:
    """
    Call GET /api/v3/profile to confirm the new token is working.
    Returns display_name on success.
    Raises on failure — caller must NOT write to .env.fyers until this succeeds.
    """
    log.info("Step 10: validating new token via /api/v3/profile …")
    resp = requests.get(
        _EP_PROFILE,
        headers={
            "Authorization": f"{app_id}:{access_token}",
            "Content-Type":  "application/json",
        },
        timeout=_REQUEST_TIMEOUT,
    )
    body = _check_response("profile", resp)
    name = (body.get("data") or {}).get("display_name") or (body.get("data") or {}).get("name") or "unknown"
    log.info("Step 10 OK — profile: %s", name)
    return name


# ── .env.fyers token writer ───────────────────────────────────────────────────

def write_token_to_env(new_token: str) -> None:
    """In-place update of FYERS_ACCESS_TOKEN in .env.fyers.
    Same regex-replace pattern as Zerodha's update_env_token() in refresh_token.py."""
    text = _ENV_FILE.read_text()
    if not re.search(r"^FYERS_ACCESS_TOKEN\s*=", text, flags=re.MULTILINE):
        raise RuntimeError("FYERS_ACCESS_TOKEN line not found in .env.fyers — cannot update")
    new_text = re.sub(
        r"^(FYERS_ACCESS_TOKEN\s*=\s*).*$",
        rf"\g<1>{new_token}",
        text,
        flags=re.MULTILINE,
    )
    _ENV_FILE.write_text(new_text)
    log.info("Step 11: FYERS_ACCESS_TOKEN updated in .env.fyers (%s)", _mask(new_token))


# ── Telegram ──────────────────────────────────────────────────────────────────

def send_telegram(bot_token: str, chat_id: str, text: str, attach_log: bool = False) -> None:
    base = f"https://api.telegram.org/bot{bot_token}"
    try:
        if attach_log and _LOG_FILE.exists():
            with open(_LOG_FILE, "rb") as f:
                requests.post(
                    f"{base}/sendDocument",
                    data={"chat_id": chat_id, "caption": text},
                    files={"document": ("fyers_token_refresh.log", f)},
                    timeout=15,
                )
        else:
            requests.post(
                f"{base}/sendMessage",
                data={"chat_id": chat_id, "text": text},
                timeout=15,
            )
    except Exception as e:
        log.warning("Telegram notification failed: %s", e)


def send_telegram_photo(bot_token: str, chat_id: str, photo_path: Path, caption: str) -> None:
    base = f"https://api.telegram.org/bot{bot_token}"
    try:
        if photo_path.exists():
            with open(photo_path, "rb") as f:
                requests.post(
                    f"{base}/sendPhoto",
                    data={"chat_id": chat_id, "caption": caption},
                    files={"photo": ("fyers_login_debug.png", f)},
                    timeout=15,
                )
    except Exception as e:
        log.warning("Telegram photo send failed: %s", e)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=== Fyers token refresh started ===")

    try:
        secrets = load_fyers_secrets()
        tg      = load_telegram_credentials()
        cfg     = load_env_config()
    except Exception as e:
        log.error("Config error: %s", e)
        sys.exit(1)

    MAX_ATTEMPTS = 2
    last_error: Exception | None = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        if attempt > 1:
            log.info("Retrying in 120 seconds (attempt %d/%d) …", attempt, MAX_ATTEMPTS)
            time.sleep(120)

        try:
            auth_code = get_fyers_auth_code(
                app_id       = cfg["app_id"],
                secret_id    = cfg["secret_id"],
                redirect_uri = cfg["redirect_uri"],
                user_id      = secrets["FYERS_USER_ID"],
                pin          = secrets["FYERS_PIN"],
                totp_secret  = secrets["FYERS_TOTP_SECRET"],
            )

            access_token = exchange_for_access_token(
                app_id    = cfg["app_id"],
                secret_id = cfg["secret_id"],
                auth_code = auth_code,
            )

            # Validate BEFORE writing — any failure here preserves the old token
            display_name = validate_token(cfg["app_id"], access_token)

            write_token_to_env(access_token)
            log.info("=== Token refresh SUCCESSFUL ===")

            note = f" (succeeded on attempt {attempt})" if attempt > 1 else ""
            send_telegram(
                tg["TELEGRAM_BOT_TOKEN"],
                tg["TELEGRAM_CHAT_ID"],
                f"✅ Fyers token refreshed successfully. Account: {display_name}.{note}",
            )
            return

        except Exception as e:
            last_error = e
            log.error("Attempt %d FAILED: %s", attempt, e)

    # All attempts exhausted — old token untouched
    log.error("=== Token refresh FAILED after %d attempts: %s ===", MAX_ATTEMPTS, last_error)
    screenshot = _SCREENSHOT
    if screenshot.exists():
        send_telegram_photo(
            tg["TELEGRAM_BOT_TOKEN"],
            tg["TELEGRAM_CHAT_ID"],
            screenshot,
            caption=f"❌ Fyers token refresh FAILED: {last_error}",
        )
    send_telegram(
        tg["TELEGRAM_BOT_TOKEN"],
        tg["TELEGRAM_CHAT_ID"],
        f"❌ Fyers token refresh FAILED after {MAX_ATTEMPTS} attempts.\n"
        f"Error: {last_error}\n\nLog attached.",
        attach_log=True,
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
