import itertools
import subprocess
import os
import shutil
import re
import sqlalchemy
import pathlib
import pandas as pd

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

from sqlalchemy import URL, create_engine, select, func
from sqlalchemy.orm import sessionmaker
from sql.schema import Base, Locations, Measurements, Sensors, Parameters
from sqlalchemy.dialects.postgresql import insert

pd.options.mode.chained_assignment = None  # default='warn'


def dl_from_aws(loc_id, year, loc_name):
    # Create local data directory
    data_path = ROOT_PATH / "data" / loc_name / str(year)
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


def read_csv(csv_path: pathlib.WindowsPath):
    dfs = []
    data_folder = (csv_path).glob("**/*.gz")
    for file in data_folder:
        df = pd.read_csv(
            file,
            usecols=["location_id", "sensors_id", "datetime", "value", "parameter"],
            compression="gzip",
        )
        dfs.append(df)

    try:
        frame = pd.concat(dfs, axis=0, ignore_index=True)
        return frame
    except ValueError:
        # happens when there is no data for the location
        # but there is a recorded date in the api database
        raise ValueError


def postgres_ignore_duplicate(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    insert_statement = insert(table.table).values(data).on_conflict_do_nothing()
    conn.execute(insert_statement)


def insert_postgres(frame: pd.DataFrame, engine: sqlalchemy.engine.base.Engine):
    while True:
        try:
            frame_upload = frame.drop(columns=["parameter"])
            frame_upload.to_sql(
                name="measurements",
                con=engine,
                if_exists="append",
                index=False,
                method=postgres_ignore_duplicate,
            )
            return
        except sqlalchemy.exc.IntegrityError as e:
            print(e.orig.diag.message_detail)
            print("Updating rows")

            error_sensor_id = re.search(
                pattern=r"=\((\d*)", string=e.orig.diag.message_detail
            ).group(1)

            condition = frame["sensors_id"] != int(error_sensor_id)
            dropped = frame[~condition]
            dropped["sensors_id"] = dropped.apply(
                lambda row: convert_dict[row["parameter"]], axis=1
            )

            retained = frame[condition]
            frame = pd.concat([dropped, retained], axis=0, ignore_index=True)
            continue


def main(row: sqlalchemy.engine.row.Row, engine: sqlalchemy.engine.base.Engine):
    if row.first_date and row.last_date:
        loc_name_clean = re.sub('[\\/:*?"<>|]', "", row.location_name).strip(" ")

        for year in range(row.first_date.year, row.last_date.year + 1):
            dl_from_aws(row.location_id, year, loc_name_clean)

    # READ CSV FILES
    csv_folder = ROOT_PATH / "data" / row.location_name

    try:
        frame = read_csv(csv_folder)
        # INSERT INTO POSTGRES
        insert_postgres(frame=frame, engine=engine)
        print(
            f"Inserted {frame.shape[0]} rows from Location: {row.location_name} ({row.location_id})"
        )
    except ValueError as err:
        print(err)

    # DELETE LOCAL FILES DIRECTORY
    try:
        shutil.rmtree(ROOT_PATH / "data" / loc_name_clean)
    except OSError as e:
        print("Error: %s : %s" % (ROOT_PATH / "data" / loc_name_clean, e.strerror))


if __name__ == "__main__":
    # DEFINE VARIABLES
    env_file = find_dotenv(".env")
    load_dotenv(env_file)

    DBHOST = os.environ.get("DBHOST")
    DBPORT = os.environ.get("DBPORT")
    DBUSER = os.environ.get("DBUSER")
    DBNAME = os.environ.get("DBNAME")
    DBPASS = os.environ.get("DBPASS")

    ROOT_PATH = Path(__file__).parent.parent.resolve()

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

    # RETRIEVE UNIQUE PARAMETERS WITH SENSORS TO CREATE convert_dict
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
    convert_dict = {row.parameter_name: row.sensor_id for row in params_results}

    # RETRIEVE LOCATION IDs FROM DATABASE
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

    results = session.execute(location_stmt.where(~subq))

    # MAIN FUNCTION
    iter_engine = itertools.repeat(engine)
    with ThreadPoolExecutor() as executor:
        future = executor.map(main, results, iter_engine)
