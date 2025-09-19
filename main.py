import argparse

from src.etl import etl
from src.report import report_top_bronze_users


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run ETL and reporting jobs")
    parser.add_argument("--source_file", help="Path to the source CSV file")
    parser.add_argument("--db_name", help="Database name")
    parser.add_argument("--table_name", help="Table name")

    args = parser.parse_args()

    etl(source_file=args.source_file, db_name=args.db_name, table_name=args.table_name)
    report_top_bronze_users(db_name=args.db_name, table_name=args.table_name)
