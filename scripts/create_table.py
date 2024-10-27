import os
import sys
from dotenv import find_dotenv, load_dotenv

from sqlalchemy import URL, create_engine
from sqlalchemy.orm import sessionmaker
from sql.schema import Base, Countries, Locations, Parameters, Sensors
from sqlalchemy.dialects.postgresql import insert

sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from src.openAQ_api import openAQ


def handle_date(datetime):
    if datetime:
        return datetime["utc"]
    else:
        return None


def get_locations(country_ids: list, app: openAQ):
    page = 1

    while True:
        locations = app.get_locations(countries_id=country_ids, limit=1000, page=page)
        try:
            for location in locations["results"]:
                yield {
                    "location_id": location["id"],
                    "location_name": location["name"],
                    "latitude": location["coordinates"]["latitude"],
                    "longitude": location["coordinates"]["longitude"],
                    "first_date": handle_date(location["datetimeFirst"]),
                    "last_date": handle_date(location["datetimeLast"]),
                    "country_id": location["country"]["id"],
                }
            if locations["meta"]["limit"] <= locations["meta"]["found"]:
                page += 1
            else:
                break
        except TypeError:
            page += 1
            continue


def get_sensors(country_ids: list, app: openAQ):
    page = 1

    while True:
        locations = app.get_locations(countries_id=country_ids, limit=1000, page=page)
        try:
            for location in locations["results"]:
                for sensor in location["sensors"]:
                    yield {
                        "sensor_id": sensor["id"],
                        "parameter_id": sensor["parameter"]["id"],
                    }
            if locations["meta"]["limit"] <= locations["meta"]["found"]:
                page += 1
            else:
                break
        except TypeError:
            page += 1
            continue


if __name__ == "__main__":

    # ENVIRONMENT VARIABLES
    env_file = find_dotenv(".env")
    load_dotenv(env_file)

    DBHOST = os.environ.get("DBHOST")
    DBPORT = os.environ.get("DBPORT")
    DBUSER = os.environ.get("DBUSER")
    DBNAME = os.environ.get("DBNAME")
    DBPASS = os.environ.get("DBPASS")

    COUNTRIES = os.environ.get("COUNTRIES")

    # CREATE ENGINE
    engine_url = URL.create(
        drivername="postgresql+psycopg2",
        username=DBUSER,
        password=DBPASS,
        host=DBHOST,
        port=DBPORT,
        database=DBNAME,
    )
    engine = create_engine(url=engine_url)

    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # CREATE APP
    app = openAQ()

    # POPULATE PARAMS TABLE
    param_list = []
    params = app.get_parameters()
    for param in params["results"]:
        param_list.append(
            {
                "parameter_id": param["id"],
                "parameter_name": param["name"],
                "units": param["units"],
                "display_name": param["displayName"],
                "description": param["description"],
            }
        )
    session.execute(insert(Parameters).values(param_list).on_conflict_do_nothing())
    session.commit()

    # POPULATE COUNTRIES TABLE
    relevant_countries = COUNTRIES.split(", ")
    print(relevant_countries)
    country_list = []
    countries = app.get_countries(limit=200)

    for country in countries["results"]:
        if country["code"] in relevant_countries:
            country_list.append(
                {
                    "country_id": country["id"],
                    "country_name": country["name"],
                    "first_date": country["datetimeFirst"],
                    "last_date": country["datetimeFirst"],
                }
            )
    session.execute(insert(Countries).values(country_list).on_conflict_do_nothing())
    session.commit()

    # POPULATE LOCATIONS TABLE
    country_ids = [country["country_id"] for country in country_list]
    locations_list = [
        detail for detail in get_locations(country_ids=country_ids, app=app)
    ]
    session.execute(insert(Locations).values(locations_list).on_conflict_do_nothing())
    session.commit()

    # POPULATE SENSORS TABLE
    sensor_list = [
        sensor_detail for sensor_detail in get_sensors(country_ids=country_ids, app=app)
    ]
    session.execute(insert(Sensors).values(sensor_list).on_conflict_do_nothing())
    session.commit()