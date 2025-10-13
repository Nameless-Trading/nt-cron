import datetime as dt
import nt_cron.kalshi_client as kalshi_client
import os
from dotenv import load_dotenv
from rich import print
import polars as pl
import uuid

def urp_strategy_job():
    today = dt.date.today()
    if today.isoweekday() in [4, 5, 6] or True:

        portfolio_value = kalshi_client.get_portfolio_balance()
        print(portfolio_value)

        markets = kalshi_client.get_markets(series_ticker="KXNFLGAME")

        trades = (
            pl.DataFrame(markets)
            .select(
                "ticker",
                "expected_expiration_time",
                "yes_ask",
            )
            .with_columns(
                pl.col("expected_expiration_time")
                .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ")
                .dt.convert_time_zone("utc")
            )
            .with_columns(
                pl.col('expected_expiration_time').dt.convert_time_zone('America/Denver'),
                pl.col('expected_expiration_time').dt.convert_time_zone('America/Denver').dt.date().alias('game_day')
            )
            .filter(
                pl.col('yes_ask').is_between(1, 99),
                # pl.col('yes_ask').is_between(90, 99),
                pl.col('game_day').eq(today)
            )
            .with_columns(
                pl.lit(1).truediv(pl.len()).alias('weight')
            )
            .with_columns(
                pl.col('weight').mul(portfolio_value).alias('dollars')
            )
            .with_columns(
                pl.col('dollars').floordiv(pl.col('yes_ask').truediv(100)).cast(pl.Int32).alias('contracts')
            )
            .sort('expected_expiration_time')
            .to_dicts()
        )

        print(trades)

        for trade in trades:
            order = kalshi_client.create_order(
                action='buy',
                count=trade['contracts'],
                side="yes",
                ticker=trade['ticker'],
                yes_price=trade['yes_ask'],
                client_order_id=str(uuid.uuid4())
            )

            print(order)

if __name__ == '__main__':
    urp_strategy_job()

