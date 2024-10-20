import subprocess
import os
import shutil
import re
import sqlalchemy
import pandas as pd

from pathlib import Path
from dotenv import find_dotenv, load_dotenv

from sqlalchemy import URL, create_engine, select
from sqlalchemy.orm import sessionmaker
from sql.schema import Base, Countries, Locations, Parameters, Sensors
from sqlalchemy.dialects.postgresql import insert


def dl_from_aws(loc_id, year, loc_name, root_path):
    # Create local data directory
    loc_name_clean = re.sub('[\\/:*?"<>|]', "", loc_name).strip(" ")
    data_path = root_path / "data" / loc_name_clean / str(year)
    Path(data_path).mkdir(parents=True, exist_ok=True)
    # Download files

    push = subprocess.call(
        [
            "aws",
            "s3",
            "cp",
            "--no-sign-request",
            "--recursive",
            "--quiet",
            f"s3://openaq-data-archive/records/csv.gz/locationid={loc_id}/year={year}/",
            data_path,
        ]
    )


def postgres_ignore_duplicate(table, conn, keys, data_iter):

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_nothing()
    conn.execute(upsert_statement)


if __name__ == "__main__":
    # ENVIRONMENT VARIABLES
    env_file = find_dotenv(".env")
    load_dotenv(env_file)

    DBHOST = os.environ.get("DBHOST")
    DBPORT = os.environ.get("DBPORT")
    DBUSER = os.environ.get("DBUSER")
    DBNAME = os.environ.get("DBNAME")
    DBPASS = os.environ.get("DBPASS")

    # CREATE SESSION
    engine_url = URL.create(
        drivername="postgresql+psycopg2",
        username=DBUSER,
        password=DBPASS,
        host=DBHOST,
        port=DBPORT,
        database=DBNAME,
    )
    engine = create_engine(url=engine_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    # RETRIEVE LOCATION IDs FROM DATABASE
    print(f"RETRIEVING LOCATION IDs")

    location_stmt = select(
        Locations.location_id,
        Locations.first_date,
        Locations.last_date,
        Locations.location_name,
    ).limit(30)

    # DOWNLOAD FROM AWS S3 BUCKET
    ROOT_PATH = Path(__file__).parent.parent.resolve()
    results = session.execute(location_stmt)
    for chunk in results.chunks(10):
        print("DOWNLOADING FROM AWS S3 BUCKET")
        for row in chunk:
            if row[1] and row[2]:
                print(row[3])
                for year in range(row[1].year, row[2].year + 1):
                    print(year)
                    dl_from_aws(row[0], year, row[3], ROOT_PATH)

        # READ CSV FILES
        print("READING CSV FILES")

        usecols = ["location_id", "sensors_id", "datetime", "value"]
        params = {
            "compression": "gzip",
            "usecols": usecols,
        }

        dfs = []
        data_folder = (ROOT_PATH / "data").glob("**/*.gz")
        for file in data_folder:
            df = pd.read_csv(file, **params)
            dfs.append(df)

        frame = pd.concat(dfs, axis=0, ignore_index=True)

        # INSERT INTO POSTGRES
        # print("INSERTING INTO POSTGRES")

        while True:
            try:
                frame.to_sql(
                    name="measurements",
                    con=engine,
                    if_exists="append",
                    index=False,
                    method=postgres_ignore_duplicate,
                )
                break
            except sqlalchemy.exc.IntegrityError as e:
                # LOG THIS ERROR
                print(e.orig.diag.message_detail)

                error_sensor_id = re.search(
                    pattern=r"=\((\d*)", string=e.orig.diag.message_detail
                ).group(1)
                location = frame[frame["sensors_id"] == int(error_sensor_id)].iat[0, 0]
                year = frame[frame["sensors_id"] == int(error_sensor_id)].iat[0, 2]

                print(location, year)

                print("DROPPING ROWS")
                frame = frame[frame["sensors_id"] != int(error_sensor_id)]
                continue

        # DELETE LOCAL FILES DIRECTORY
        print("DELETING LOCAL DATA DIRECTORY")
        try:
            shutil.rmtree(ROOT_PATH / "data")
        except OSError as e:
            print("Error: %s : %s" % (ROOT_PATH / "data", e.strerror))
