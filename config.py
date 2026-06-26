import os
from dotenv import load_dotenv

load_dotenv()

HATCHET_CLIENT_TOKEN = os.getenv("HATCHET_CLIENT_TOKEN")

if not HATCHET_CLIENT_TOKEN:
    raise ValueError("Missing HATCHET_CLIENT_TOKEN. Check your .env file.")
