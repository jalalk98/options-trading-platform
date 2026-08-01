import requests
import pyotp
from urllib.parse import parse_qs, urlparse
from fyers_apiv3 import fyersModel

APP_ID = "EOQ0OYG1G3-100"
SECRET_KEY = "XXZDEAPRQ3"
REDIRECT_URI = "https://127.0.0.1"

FYERS_ID = "YJ03466"
PIN = "0711"
TOTP_SECRET = "33PB5BO2ZLKFYBBR5SHI7H7WRRWECZPD"

# Step 1 - Generate OTP
totp = pyotp.TOTP(TOTP_SECRET).now()

# Step 2 - Send login OTP
url_send_otp = "https://api-t2.fyers.in/vagator/v2/send_login_otp"

payload = {
    "fy_id": FYERS_ID,
    "app_id": "2"
}

r1 = requests.post(url_send_otp, json=payload).json()

request_key = r1["request_key"]

# Step 3 - Verify OTP
url_verify_otp = "https://api-t2.fyers.in/vagator/v2/verify_otp"

payload = {
    "request_key": request_key,
    "otp": totp
}

r2 = requests.post(url_verify_otp, json=payload).json()

request_key = r2["request_key"]

# Step 4 - Verify PIN
url_verify_pin = "https://api-t2.fyers.in/vagator/v2/verify_pin_v2"

payload = {
    "request_key": request_key,
    "identity_type": "pin",
    "identifier": PIN
}

session_req = requests.Session()

r3 = session_req.post(url_verify_pin, json=payload).json()

print(r3)

# Step 5 - Generate auth code
headers = {
    "Authorization": f"Bearer {access_token_login}"
}

payload = {
    "fyers_id": FYERS_ID,
    "app_id": APP_ID[:-4],
    "redirect_uri": REDIRECT_URI,
    "appType": "100",
    "code_challenge": "",
    "state": "None",
    "scope": "",
    "nonce": "",
    "response_type": "code",
    "create_cookie": True
}

r4 = session_req.post(
    "https://api.fyers.in/api/v2/token",
    headers=headers,
    json=payload
).json()

url = r4["Url"]

auth_code = parse_qs(urlparse(url).query)["auth_code"][0]

# Step 6 - Exchange for final access token

session = fyersModel.SessionModel(
    client_id=APP_ID,
    secret_key=SECRET_KEY,
    redirect_uri=REDIRECT_URI,
    response_type="code",
    grant_type="authorization_code"
)

session.set_token(auth_code)

response = session.generate_token()

final_access_token = response["access_token"]

print(final_access_token)