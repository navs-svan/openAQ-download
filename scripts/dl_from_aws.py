import subprocess
import os
import shutil
import re
import sqlalchemy
import pandas as pd

from pathlib import Path
from dotenv import find_dotenv, load_dotenv

from sqlalchemy import URL, create_engine, select, func
from sqlalchemy.orm import sessionmaker
from sql.schema import Base, Locations, Measurements, Sensors, Parameters
from sqlalchemy.dialects.postgresql import insert

pd.options.mode.chained_assignment = None  # default='warn'

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

    CHUNKSIZE=1

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

    # RETRIEVE UNIQUE PARAMETERS WITH SENSORS
    # TO CREATE convert_dict
    unique_cte = (
        select(
            Sensors.sensor_id,
            Parameters.parameter_id,
            Parameters.parameter_name,
            func.row_number().over(partition_by=Parameters.parameter_id).label("row"),
        )
        .join_from(Sensors, Parameters)
        .cte()
    )
    convert_stmt = select(unique_cte).where(unique_cte.c.row == 1)
    params_results = session.execute(convert_stmt)
    convert_dict = {row.parameter_name:row.sensor_id for row in params_results}

    # RETRIEVE LOCATION IDs FROM DATABASE
    print(f"RETRIEVING LOCATION IDs")

    location_stmt = select(
        Locations.location_id,
        Locations.first_date,
        Locations.last_date,
        Locations.location_name,
    ).where(Locations.first_date != None)

    subq = (
        select(Measurements.location_id)
        .where(Measurements.location_id == Locations.location_id)
        .exists()
    )

    # DOWNLOAD FROM AWS S3 BUCKET
    ROOT_PATH = Path(__file__).parent.parent.resolve()
    results = session.execute(location_stmt.where(~subq))  #CHANGE LATER
    for chunk in results.chunks(CHUNKSIZE):
        print("DOWNLOADING FROM AWS S3 BUCKET")
        for row in chunk:
            if row[1] and row[2]:
                print(row[3])
                for year in range(row[1].year, row[2].year + 1):
                    print(year)
                    dl_from_aws(row[0], year, row[3], ROOT_PATH)

        # READ CSV FILES
        print("READING CSV FILES")

        usecols = ["location_id", "sensors_id", "datetime", "value", "parameter"]
        params = {
            "compression": "gzip",
            "usecols": usecols,
        }

        dfs = []
        data_folder = (ROOT_PATH / "data").glob("**/*.gz")
        for file in data_folder:
            df = pd.read_csv(file, **params)
            dfs.append(df)

        try: 
            frame = pd.concat(dfs, axis=0, ignore_index=True)
        except ValueError as err:
            # happens when there is no data for the location
            # but there is a recorded date in the api database
            print(err)
            continue


        # INSERT INTO POSTGRES
        print("INSERTING INTO POSTGRES")
        while True:
            try:
                print(frame.shape)

                frame_upload = frame.drop(columns=["parameter"])
                frame_upload.to_sql(
                    name="measurements",
                    con=engine,
                    if_exists="append",
                    index=False,
                    method=postgres_ignore_duplicate,
                )
                break
            except sqlalchemy.exc.IntegrityError as e:
                # LOG THIS ERROR
                # INSTEAD OF DROPPING, MAYBE LOOK FOR SAME PARAMETER AND SENSOR
                # THEN REPLACE IT
                print(e.orig.diag.message_detail)

                error_sensor_id = re.search(
                    pattern=r"=\((\d*)", string=e.orig.diag.message_detail
                ).group(1)
                
                condition = frame["sensors_id"] != int(error_sensor_id)

                print("UPDATING PROBLEMATIC ROWS")

                dropped = frame[~condition]
                dropped["sensors_id"] = dropped.apply(lambda row: convert_dict[row["parameter"]], axis=1)
                retained = frame[condition]

                frame = pd.concat([dropped, retained], axis=0, ignore_index=True)
                print(frame.shape)
                continue

        # DELETE LOCAL FILES DIRECTORY
        print("DELETING LOCAL DATA DIRECTORY")
        try:
            shutil.rmtree(ROOT_PATH / "data")
        except OSError as e:
            print("Error: %s : %s" % (ROOT_PATH / "data", e.strerror))
