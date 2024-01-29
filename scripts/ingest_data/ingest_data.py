#!/usr/bin/env python
# coding: utf-8

# Imports
import os
import argparse
import gzip

import pandas as pd
from sqlalchemy import create_engine
from time import time


def main(params):
    host = params.host
    user = params.user
    password = params.password
    database = params.database
    port = params.port
    table_name = params.table
    url = params.url

    output_file = "output.csv"

    print("Test,\n\n", user, password, host, port, database)

    # Download the csv file
    os.system(f"wget {url} -O {output_file}")

    # Create a connection to the database
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

    df_iter = pd.read_csv(output_file, iterator=True, chunksize=100000, compression="gzip")
    df = next(df_iter)

    # Convert the data type of some of the columns to a timestamp
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    # Create the initial schema
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    # Append the first chunck to postgres
    df.to_sql(name=table_name, con=engine, if_exists="append")

    # Append the rest of the chunks to postgres
    while True:
        t_start = time()
        try:
            df = next(df_iter)
        except StopIteration:
            break

        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

        df.to_sql(name=table_name, con=engine, if_exists="append")
        t_end = time()

        print(f"Chunk loaded in {t_end - t_start} seconds")


if __name__ == "__main__":
    # Create an argument parser
    parser = argparse.ArgumentParser(
        prog="Ingest Data",
        description="Ingest CSV Data to Postgres",
    )

    # List all the arguments needed
    parser.add_argument(
        "--host",
        type=str,
        help="The host of the postgres database",
        default="localhost",
    )
    parser.add_argument(
        "--user",
        type=str,
        help="The user of the postgres database",
    )
    parser.add_argument(
        "--password",
        type=str,
        help="The password of the postgres database",
    )
    parser.add_argument(
        "--database",
        type=str,
        help="The database of the postgres database",
    )
    parser.add_argument(
        "--port",
        type=int,
        help="The port of the postgres database",
        default=5432,
    )
    parser.add_argument(
        "--table",
        type=str,
        help="The table name of the postgres database",
    )
    parser.add_argument(
        "--url",
        type=str,
        help="The url of the csv file",
    )

    args = parser.parse_args()

    main(args)
