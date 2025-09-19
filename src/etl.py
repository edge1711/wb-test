import pandas as pd
import numpy as np
import duckdb

from src.logger_setup import get_logger


logger = get_logger(__name__)


def drop_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    df_na = df.loc[df
        .isna()
        .any(axis=1)
    ]
    df_without_na = df.dropna()
    logger.info(f'Dropped rows with NaN: \n {df_na.to_string(index=False)}')

    return df_without_na


def timestamp_to_datetime(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(timestamp=pd.to_datetime(df['timestamp']))

def timestamp_to_weekday(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.assign(
            weekday=lambda d: d['timestamp'].dt.dayofweek,
            week_start_date=lambda d: (
                    d['timestamp'] - pd.to_timedelta(d['weekday'], unit='d')
            ).dt.date,
        )
        .drop(columns=['weekday', 'timestamp'])
    )

def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        quantity_net=lambda x: x['quantity'].where(x['side'] == 'sell', -x['quantity']),
        pnl=lambda x: x['quantity_net'] * x['price']
    )


def aggregate_data(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .drop(columns=['side', 'price'])
        .groupby(by=['week_start_date', 'client_type', 'user_id', 'symbol'], as_index=False, group_keys=False)
        .agg(total_volume=('quantity', 'sum'),
             total_pnl=('pnl', 'sum'),
             total_trades=('quantity', 'count'))
    )

def extract(path: str) -> pd.DataFrame:
    dtypes = {
        'user_id': str,
        'client_type': str,
        'symbol': str,
        'side': str,
        'quantity': np.double,
        'price': np.double
    }
    df = pd.read_csv(
        filepath_or_buffer=path,
        dtype=dtypes,
        parse_dates=True
    )
    logger.info(f'Extracted {len(df)} rows.')

    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    return (df
        .pipe(drop_missing_data)
        .pipe(timestamp_to_datetime)
        .pipe(timestamp_to_weekday)
        .pipe(calculate_metrics)
        .pipe(aggregate_data)
    )

def load(df: pd.DataFrame, db_name: str, table_name: str) -> None:
    duckdb.sql(f"ATTACH '{db_name}.db';")
    duckdb.sql(f"DROP TABLE IF EXISTS {db_name}.{table_name};")
    duckdb.sql(f'CREATE TABLE {db_name}.{table_name} AS SELECT * FROM df')
    duckdb.sql(f"DETACH {db_name};;")
    logger.info(f'Loaded data to {table_name} table in {db_name} database.')


def etl(source_file: str, db_name: str, table_name: str) -> None:
    logger.info(f'Start ETL process from file {source_file}')

    df_raw = extract(path=source_file)
    df_transformed = transform(df=df_raw)
    load(df=df_transformed, db_name=db_name, table_name=table_name)

    logger.info(f'ETL process has been successfully finished.')
