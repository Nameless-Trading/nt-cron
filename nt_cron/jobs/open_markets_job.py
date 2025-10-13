import nt_cron.kalshi_client as kalshi_client
from nt_cron.database import write_dataframe
from dotenv import load_dotenv
import polars as pl
import datetime as dt

load_dotenv(override=True)


def open_markets_job() -> None:
    markets = kalshi_client.get_markets()

    markets_df = (
        pl.DataFrame(markets)
        .select(
            "ticker",
            "event_ticker",
            "title",
            "yes_sub_title",
            "expected_expiration_time",
        )
        .with_columns(
            pl.col("expected_expiration_time")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ")
            .dt.convert_time_zone("utc")
        )
        .with_columns(
            pl.col("expected_expiration_time")
            .sub(dt.timedelta(hours=3))
            .alias("estimated_start_time")
        )
        .sort("estimated_start_time")
    )

    table_name = "open_markets"
    write_dataframe(markets_df, table_name)
