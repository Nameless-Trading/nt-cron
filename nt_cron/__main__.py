from nt_cron.cfbd_client import get_game_schedule
import datetime as dt
from rich import print
import polars as pl

def game_schedule_cron() -> None:
    today = dt.date.today() - dt.timedelta(days=1)
    records = get_game_schedule()

    print(records)

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
            pl.col('start_time').dt.convert_time_zone('America/Denver')
        )
        .filter(pl.col('start_time').dt.date().eq(today))
        .unique()
        .sort('start_time')
        .to_dicts()
    )

    print(df)

    return df

if __name__ == '__main__':
    print(
        game_schedule_cron()
    )