import duckdb
import pandas as pd

from src.logger_setup import get_logger


logger = get_logger(__name__)


def get_top_bronze(db_name: str, table_name: str, metric: str) -> pd.DataFrame:
    query_top_3_bronze = f'''
    SELECT
        user_id,
        SUM({metric}) AS {metric}
    FROM {db_name}.{table_name}
    WHERE client_type = 'bronze'
    GROUP BY user_id
    ORDER BY sum({metric}) DESC
    LIMIT 3
    ;
    '''
    df = duckdb.sql(f"{query_top_3_bronze}").df()
    logger.info(f'Finished query - \n {query_top_3_bronze}')

    return df



def report_top_bronze_users(db_name: str, table_name: str) -> None:
    volume_metric = 'total_volume'
    pnl_metric = 'total_pnl'
    file_name = 'top_clients.csv'
    folder_name = 'output'

    logger.info("Start top bronze clients reporting.")
    duckdb.sql(f"ATTACH '{db_name}.db';")
    df_volume = get_top_bronze(db_name=db_name, table_name=table_name, metric=volume_metric)
    df_pnl = get_top_bronze(db_name=db_name, table_name=table_name, metric=pnl_metric)
    duckdb.sql(f"DETACH {db_name};")

    df_volume.to_csv(path_or_buf=f'{folder_name}/{volume_metric}_{file_name}', index=False)
    df_pnl.to_csv(path_or_buf=f'{folder_name}/{pnl_metric}_{file_name}', index=False)

    logger.info("Successfully saved top 3 users into CSV files.")
