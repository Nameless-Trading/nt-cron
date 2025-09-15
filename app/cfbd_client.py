import requests
from dotenv import load_dotenv
import os

load_dotenv(override=True)

base_url = "https://api.collegefootballdata.com"
api_key = os.getenv("CFBD_API_KEY")

def get_game_schedule() -> list[dict]:
    endpoint = "/games/media"
    year = 2025

    params = {"year": year}
    headers = {"accept": "application/json", "Authorization": f"Bearer {api_key}"}

    response = requests.get(base_url + endpoint, params, headers=headers)

    return response.json()