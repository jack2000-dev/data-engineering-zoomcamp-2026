import marimo

__generated_with = "0.23.6"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Explore the Data
    """)
    return


@app.cell
def _():
    import pandas as pd

    return (pd,)


@app.cell
def _(pd):
    # Read a sample of the data
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    df = pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz', nrows=100)
    return df, prefix


@app.cell
def _(df):
    df.head()
    return


@app.cell
def _(df):
    df.dtypes
    return


@app.cell
def _(df):
    df.shape
    return


@app.cell
def _(pd, prefix):
    # Specify data types
    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64"
    }

    parse_dates = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ]

    _df = pd.read_csv(
        prefix + 'yellow_tripdata_2021-01.csv.gz',
        nrows=100,
        dtype=dtype,
        parse_dates=parse_dates
    )
    return dtype, parse_dates


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Ingesting Data into Postgres
    """)
    return


@app.cell
def _():
    # `uv add sqlalchemy` then create database connection

    from sqlalchemy import create_engine
    engine = create_engine('postgresql+psycopg://root:root@localhost:5432/ny_taxi')
    return (engine,)


@app.cell
def _(df, engine):
    df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
    return


@app.cell
def _(df, engine, pd):
    # Get DDL Schema
    print (pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Ingesting Data in Chunks (batch ingestion)
    """)
    return


@app.cell
def _(dtype, parse_dates, pd, prefix):
    # df_iter = data frame iterator = a cycle steps logic to ingest data in chunks
    df_iter = pd.read_csv ( 
        prefix + 'yellow_tripdata_2021-01.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=100000
    )
    return (df_iter,)


@app.cell
def _():
    # Import lib for progress bar
    # uv add tqdm

    from tqdm.auto import tqdm

    return (tqdm,)


@app.cell
def _(df_iter, engine, tqdm):
    # Put iterator into loop
    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Export notebook into python script
    marimo export script
    """)
    return


if __name__ == "__main__":
    app.run()
