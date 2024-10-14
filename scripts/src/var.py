from dotenv import load_dotenv, find_dotenv
import os

# load env file
DOTENV_FILE = find_dotenv(".env")
load_dotenv(DOTENV_FILE)

# store env variables
OPENAQ_API_KEY = os.environ.get("OPENAQ_API_KEY")

