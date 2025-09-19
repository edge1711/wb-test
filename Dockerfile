FROM python:3.13-slim

WORKDIR /app
COPY . .
RUN pip install -r requirements.txt

ENTRYPOINT ["python", "main.py"]
CMD ["--source_file", "trades.csv", "--db_name", "agg_result", "--table_name", "agg_trades_weekly"]