from nt_cron.cfbd_client import get_game_schedule
from nt_cron.database import write_dataframe
import datetime as dt
import polars as pl


def game_schedule_job() -> None:
    today = dt.date.today()
    records = get_game_schedule()

    df = (
        pl.DataFrame(records)
        .select("season", "week", "startTime", "isStartTimeTBD", "homeTeam", "awayTeam")
        .rename(
            {
                "startTime": "start_time",
                "isStartTimeTBD": "is_start_time_tbd",
                "homeTeam": "home_team",
                "awayTeam": "away_team",
            }
        )
        .with_columns(
            pl.col("start_time")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.fZ")
            .dt.convert_time_zone("utc")
        )
        .with_columns(pl.lit(today).alias("last_update_date"))
        .unique()
        .sort("start_time")
    )

    table_name = "schedule"
    write_dataframe(df, table_name)
