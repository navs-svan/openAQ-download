import requests
from var import OPENAQ_API_KEY


class openAQ:

    def __init__(self) -> None:
        self.API_KEY = OPENAQ_API_KEY

    def _auth_header(self) -> dict:
        return {"X-API-Key": self.API_KEY}

    def get_location(self, loc_id: int):
        endpoint = f"https://api.openaq.org/v3/locations/{loc_id}"
        r = requests.get(endpoint, headers=self._auth_header())

        if r.status_code == 200:
            return r.json()


if __name__ == "__main__":
    print("Hello World")

    app = openAQ()
    print(app._auth_header())

    print(app.get_location(8118))
