"""
get_token_manual.py
-------------------
Manual Fyers token refresh for use when automated browser login is blocked
by Cloudflare Turnstile (e.g. on an EC2 datacenter IP).

Usage:
    cd /home/ubuntu/projects/options-trading-platform
    PYTHONPATH=. python -m backend.services.fyers_collector.get_token_manual

Steps:
  1. Script prints the Fyers OAuth URL — open it in your LOCAL browser.
  2. Log in (FYID → TOTP → PIN → Turnstile checkbox if shown).
  3. Browser redirects to https://127.0.0.1/?auth_code=...&state=...
     Copy the auth_code value from the URL bar.
  4. Paste it here when prompted.
  5. Script calls /api/v3/validate-authcode, validates via /profile,
     and writes FYERS_ACCESS_TOKEN to .env.fyers.
  6. Sends a Telegram confirmation.

Reads credentials from:
  ~/.fyers_secrets  — FYERS_USER_ID (unused here), FYERS_PIN (unused)
  ~/.kite_secrets   — TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  .env.fyers        — FYERS_APP_ID, FYERS_SECRET_ID, FYERS_REDIRECT_URI
"""

import hashlib
import json
import logging
import os
import random
import re
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel

# ── Paths ──────────────────────────────────────────────────────────────────────
_PROJECT_ROOT  = Path(__file__).resolve().parents[3]
_ENV_FILE      = _PROJECT_ROOT / ".env.fyers"
_KITE_SECRETS  = Path.home() / ".kite_secrets"
_LOG_FILE      = Path.home() / "fyers_token_refresh.log"

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

# ── API endpoints ──────────────────────────────────────────────────────────────
_FYERS_BASE       = "https://api-t1.fyers.in/api/v3"
_EP_VALIDATE_AUTH = f"{_FYERS_BASE}/validate-authcode"
_EP_PROFILE       = f"{_FYERS_BASE}/profile"
_REQUEST_TIMEOUT  = 15


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_kv_file(path: Path, required: set) -> dict:
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


def _mask(token: str) -> str:
    if not token or len(token) < 12:
        return "***"
    return f"{token[:6]}...{token[-4:]}"


def _app_id_hash(app_id: str, secret_id: str) -> str:
    return hashlib.sha256(f"{app_id}:{secret_id}".encode()).hexdigest()


def _check_response(step: str, resp: requests.Response) -> dict:
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


def load_env_config() -> dict:
    load_dotenv(_ENV_FILE, override=False)
    cfg = {
        "app_id":       os.getenv("FYERS_APP_ID", ""),
        "secret_id":    os.getenv("FYERS_SECRET_ID", ""),
        "redirect_uri": os.getenv("FYERS_REDIRECT_URI", ""),
    }
    missing = [k for k, v in cfg.items() if not v]
    if missing:
        raise ValueError(f"Missing in .env.fyers: {[k.upper() for k in missing]}")
    return cfg


def load_telegram_credentials() -> dict:
    return _load_kv_file(_KITE_SECRETS, {"TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"})


def build_auth_url(app_id: str, secret_id: str, redirect_uri: str) -> str:
    session = fyersModel.SessionModel(
        client_id=app_id,
        redirect_uri=redirect_uri,
        response_type="code",
        state=str(random.randint(100_000, 999_999)),
        secret_key=secret_id,
        grant_type="authorization_code",
    )
    return session.generate_authcode()


def exchange_for_access_token(app_id: str, secret_id: str, auth_code: str) -> str:
    log.info("Calling /api/v3/validate-authcode …")
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
    log.info("access_token received: %s (flat response shape)", _mask(access_token))
    return access_token


def validate_token(app_id: str, access_token: str) -> str:
    log.info("Validating token via /api/v3/profile …")
    resp = requests.get(
        _EP_PROFILE,
        headers={
            "Authorization": f"{app_id}:{access_token}",
            "Content-Type":  "application/json",
        },
        timeout=_REQUEST_TIMEOUT,
    )
    body = _check_response("profile", resp)
    name = (
        (body.get("data") or {}).get("display_name")
        or (body.get("data") or {}).get("name")
        or "unknown"
    )
    log.info("Profile OK — %s", name)
    return name


def write_token_to_env(new_token: str) -> None:
    text = _ENV_FILE.read_text()
    if not re.search(r"^FYERS_ACCESS_TOKEN=", text, flags=re.MULTILINE):
        raise RuntimeError("FYERS_ACCESS_TOKEN line not found in .env.fyers")
    new_text = re.sub(
        r"^(FYERS_ACCESS_TOKEN=)[^\n]*",
        rf"\g<1>{new_token}",
        text,
        flags=re.MULTILINE,
    )
    _ENV_FILE.write_text(new_text)
    log.info("FYERS_ACCESS_TOKEN written to .env.fyers (%s)", _mask(new_token))


def send_telegram(bot_token: str, chat_id: str, text: str) -> None:
    try:
        requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            data={"chat_id": chat_id, "text": text},
            timeout=15,
        )
    except Exception as e:
        log.warning("Telegram notification failed: %s", e)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=== Fyers manual token refresh ===")

    try:
        cfg = load_env_config()
        tg  = load_telegram_credentials()
    except Exception as e:
        log.error("Config error: %s", e)
        sys.exit(1)

    # ── Step 1: Print the auth URL ─────────────────────────────────────────────
    auth_url = build_auth_url(cfg["app_id"], cfg["secret_id"], cfg["redirect_uri"])
    print()
    print("=" * 72)
    print("  Open this URL in your LOCAL browser and log in:")
    print()
    print(f"  {auth_url}")
    print()
    print("  After login, you will be redirected to a URL like:")
    print("  https://127.0.0.1/?auth_code=XXXX&state=YYYY")
    print()
    print("  Copy the auth_code value (between auth_code= and &) from the URL bar.")
    print("=" * 72)
    print()

    # ── Step 2: Accept auth_code from paste or full URL ───────────────────────
    raw = input("Paste auth_code (or full redirect URL): ").strip()
    if not raw:
        log.error("No input provided — aborting.")
        sys.exit(1)

    # Accept either the bare auth_code or the full redirect URL
    if "auth_code=" in raw:
        qs = parse_qs(urlparse(raw).query)
        codes = qs.get("auth_code", [])
        auth_code = codes[0] if codes else ""
    else:
        auth_code = raw

    if not auth_code:
        log.error("Could not extract auth_code from input: %s", raw[:120])
        sys.exit(1)

    log.info("auth_code accepted: %s", _mask(auth_code))

    # ── Steps 3-5: Exchange → validate → write ─────────────────────────────────
    try:
        access_token = exchange_for_access_token(
            cfg["app_id"], cfg["secret_id"], auth_code
        )

        # Validate BEFORE writing — preserves old token on any failure
        display_name = validate_token(cfg["app_id"], access_token)

        write_token_to_env(access_token)
        log.info("=== Token refresh SUCCESSFUL ===")

        send_telegram(
            tg["TELEGRAM_BOT_TOKEN"],
            tg["TELEGRAM_CHAT_ID"],
            f"✅ Fyers token refreshed manually. Account: {display_name}.",
        )

    except Exception as e:
        log.error("Token exchange/validation FAILED: %s", e)
        send_telegram(
            tg["TELEGRAM_BOT_TOKEN"],
            tg["TELEGRAM_CHAT_ID"],
            f"❌ Fyers manual token refresh FAILED: {e}",
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
