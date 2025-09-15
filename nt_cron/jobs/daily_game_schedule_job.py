from nt_cron.cfbd_client import get_game_schedule
from nt_cron.database import stage_dataframe
import datetime as dt
import polars as pl

def daily_game_schedule_job() -> None:
    today = dt.date.today()
    records = get_game_schedule()

    df =  (
        pl.DataFrame(records)
        .select(
            "season", "week", "startTime", "isStartTimeTBD", "homeTeam", "awayTeam"
        )
        .rename({
            'startTime': 'start_time',
            'isStartTimeTBD': 'is_start_time_tbd',
            'homeTeam': 'home_team',
            'awayTeam': 'away_team'
        })
        .with_columns(
            pl.col('start_time').str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.fZ").dt.convert_time_zone('utc')
        )
        .with_columns(
            pl.col('start_time').dt.convert_time_zone('America/Denver'),
            pl.lit(today).alias('last_update_date')
        )
        .filter(pl.col('start_time').dt.date().eq(today))
        .unique()
        .sort('start_time')
    )

    table_name = f"schedule"
    stage_dataframe(df, table_name)
