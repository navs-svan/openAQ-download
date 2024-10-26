import requests
from var import OPENAQ_API_KEY
import json
import sys
import time
from typing import List

# get countries id
# get location by country
# get sensors by location
# get measurements by sensor id


class openAQ:

    def __init__(self) -> None:
        self.API_KEY = OPENAQ_API_KEY

    def _auth_header(self) -> dict:
        return {"X-API-Key": self.API_KEY}

    def _send_request(self, endpoint, **params):
        endpoint = "https://api.openaq.org" + endpoint
        retries = 10
        for _ in range(retries):
            r = requests.get(endpoint, **params, headers=self._auth_header())
            if r.status_code == 200:
                # print(r.headers["x-ratelimit-used"])
                return r.json()

            elif r.status_code == 429:
                # limit rate exceeded
                ...
            elif r.status_code == 422:
                # prints out only one error can be improved
                resp = r.json()

                if isinstance(resp, str):
                    # eval might be dangerous but we're dealing with an API response
                    # so we can expect what the input will be
                    error = eval(resp)[0]
                elif isinstance(resp, dict):
                    error = resp["detail"][0]

                msg = (
                    f"Error type: {error['type']}\nLocation: {error['loc']}\n",
                    f"Message: {error['msg']}\nInput: {error['input']}",
                )
                print("".join(msg))
                sys.exit(1)
            elif r.status_code <= 500:
                print("Internal Server Error\nRetrying")
                time.sleep(5)

        print("Max retries exceeded")
        sys.exit(1)

    def get_measurements(self): ...

    def get_locations(
        self,
        loc_id: int = None,
        coordinates: str = None,
        radius: int = None,
        providers_id: List[int] = None,
        parameters_id: List[int] = None,
        limit: int = 100,
        page: int = 1,
        owner_contacts_id: List[int] = None,
        manufacturers_id: List[int] = None,
        order_by: str = None,
        sort_order: str = "asc",
        licenses_id: List[int] = None,
        monitor: bool = None,
        mobile: bool = None,
        instruments_id: List[int] = None,
        iso: str = None,
        countries_id: List[int] = None,
        bbox: str = None,
    ):
        endpoint = f"/v3/locations"
        params = None

        if loc_id:
            endpoint = endpoint + f"/{loc_id}"
        else:
            params = {
                "coordinates": coordinates,
                "radius": radius,
                "providers_id": providers_id,
                "parameters_id": parameters_id,
                "limit": limit,
                "page": page,
                "owner_contacts_id": owner_contacts_id,
                "manufacturers_id": manufacturers_id,
                "order_by": order_by,
                "sort_order": sort_order,
                "licenses_id": licenses_id,
                "monitor": monitor,
                "mobile": mobile,
                "instruments_id": instruments_id,
                "iso": iso,
                "countries_id": countries_id,
                "bbox": bbox,
            }

        return self._send_request(endpoint, params=params)

    def get_countries(
        self,
        country_id: int = None,
        order_by: str = None,
        sort_order: str = "asc",
        providers_id: int = None,
        parameters_id: List[int] = None,
        limit: int = 100,
        page: int = 1,
    ):
        endpoint = "/v3/countries"
        params = None

        if country_id:
            endpoint = endpoint + f"/{country_id}"
        else:
            params = {
                "order_by": order_by,
                "sort_order": sort_order,
                "providers_id": providers_id,
                "parameters_id": parameters_id,
                "limit": limit,
                "page": page,
            }

        return self._send_request(endpoint, params=params)

    def get_parameters(
        self,
        parameter_id: int = None,
        order_by: str = None,
        sort_order: str = "asc",
        parameter_type: str = None,
        coordinates: str = None,
        radius: int = None,
        bbox: str = None,
        iso: str = None,
        countries_id: List[int] = None,
        limit: int = 100,
        page: int = 1,
    ):

        endpoint = "/v3/parameters"
        if parameter_id:
            endpoint = endpoint + f"/{parameter_id}"
        else:
            params = {
                "order_by": order_by,
                "sort_order": sort_order,
                "parameter_type": parameter_type,
                "coordinates": coordinates,
                "radius": radius,
                "bbox": bbox,
                "iso": iso,
                "countries_id": countries_id,
                "limit": limit,
                "page": page,
            }

        return self._send_request(endpoint, params=params)


if __name__ == "__main__":

    app = openAQ()

    jsn = app.get_locations(countries_id=[1], limit=100, page=-1)
    with open("test.json", "w") as f:
        json.dump(jsn, f, indent=4)
