import base64
import json
import time
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
import requests
import os
from dotenv import load_dotenv
from rich import print

load_dotenv(override=True)

API_KEY = os.getenv("KALSHI_API_KEY")
PRIVATE_KEY = os.getenv("KALSHI_PRIVATE_KEY")
BASE_URL = os.getenv("KALSHI_API_BASE")

if API_KEY is None: raise ValueError("KALSHI_API_KEY is not set!")
if PRIVATE_KEY is None: raise ValueError("KALSHI_PRIVATE_KEY is not set!")
if BASE_URL is None: raise ValueError("KALSHI_API_BASE is not set!")

PRIVATE_KEY_SERIALIZED = serialization.load_pem_private_key(
    PRIVATE_KEY.encode('utf-8'), password=None
)

def sign_pss_text(private_key, text: str) -> str:
    """Sign message using RSA-PSS"""
    message = text.encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


def create_headers(
    private_key: str, kalshi_api_key: str, method: str, path: str
) -> dict:
    """Create authentication headers"""
    timestamp = str(int(time.time() * 1000))
    msg_string = timestamp + method + path.split("?")[0]
    signature = sign_pss_text(private_key, msg_string)

    return {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": kalshi_api_key,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }

def get_tickers() -> list[str]:
    endpoint = "/trade-api/v2/markets"
    method = "GET"

    params = {"limit": 1000, "status": "open", "series_ticker": "KXNCAAFGAME"}

    headers = create_headers(
        PRIVATE_KEY_SERIALIZED, API_KEY, method, endpoint
    )

    response = requests.get(BASE_URL + endpoint, params=params, headers=headers)
    markets = response.json()["markets"]

    tickers = [market["ticker"] for market in markets]

    return tickers

def get_markets(tickers: list[str] | None = None, series_ticker: str | None = None) -> list[str]:
    endpoint = "/trade-api/v2/markets"
    method = "GET"

    params = {"limit": 1000, "status": "open"}

    if tickers is not None:
        params["tickers"] = ",".join(tickers)
    if series_ticker is not None:
        params["series_ticker"] = series_ticker

    headers = create_headers(
        PRIVATE_KEY_SERIALIZED, API_KEY, method, endpoint
    )

    response = requests.get(BASE_URL + endpoint, params=params, headers=headers)

    if response.ok:
        return response.json()["markets"]
    
    else:
        raise RuntimeError(response.text)


def get_portfolio_balance() -> float:
    endpoint = "/trade-api/v2/portfolio/balance"
    method = "GET"

    headers = create_headers(
        PRIVATE_KEY_SERIALIZED, API_KEY, method, endpoint
    )

    response = requests.get(BASE_URL + endpoint, headers=headers)
    portfolio_balance = response.json()['balance'] / 100

    return portfolio_balance

def create_order(action: str, count: int, side: str, ticker: str, yes_price: int, client_order_id: str) -> float:
    endpoint = "/trade-api/v2/portfolio/orders"
    method = "POST"

    payload = {
        "action": action,
        "count": count,
        "side": side,
        "ticker": ticker,
        "yes_price": yes_price,
        "client_order_id": client_order_id
    }

    headers = create_headers(
        PRIVATE_KEY_SERIALIZED, API_KEY, method, endpoint
    )

    response = requests.post(BASE_URL + endpoint, json=payload, headers=headers)

    if response.ok:
        return response.json()

    else:
        raise RuntimeError(response.text)