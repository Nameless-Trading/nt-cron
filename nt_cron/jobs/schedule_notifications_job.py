import datetime as dt
from nt_cron.database import read_dataframe
from nt_cron.slack import schedule_message, Channel
import polars as pl
from dataclasses import dataclass
from zoneinfo import ZoneInfo


@dataclass
class Game:
    event_ticker: str
    start_time: dt.datetime
    title: str


def get_games(date_: dt.date) -> list[Game]:
    games = (
        read_dataframe("open_markets")
        .with_columns(
            pl.col("estimated_start_time").dt.convert_time_zone("America/Denver"),
        )
        .select('event_ticker', 'title', 'estimated_start_time').unique()
        .with_columns(pl.col('title').str.replace(" Winner\\?", ""))
        .filter(pl.col("estimated_start_time").dt.date().eq(date_))
        .sort("estimated_start_time")
        .to_dicts()
    )

    return [
        Game(
            event_ticker=game['event_ticker'],
            start_time=game["estimated_start_time"],
            title=game['title']
        )
        for game in games
    ]


def schedule_notifications_job() -> None:
    denver_tz = ZoneInfo('America/Denver')
    today = dt.datetime.now(denver_tz).date()
    games = get_games(today)

    for game in games:
        message = (
            f"Alert: {game.title} starts in 30 minutes!"
        )
        notification_time = game.start_time - dt.timedelta(minutes=30)
        schedule_message(
            channel=Channel.General, text=message, schedule_time=notification_time
        )
