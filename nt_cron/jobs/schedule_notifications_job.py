import datetime as dt
from nt_cron.database import read_dataframe
from nt_cron.slack import schedule_message, Channel
import polars as pl
from dataclasses import dataclass


@dataclass
class Game:
    start_time: dt.datetime
    is_start_time_tbd: bool
    home_team: str
    away_team: str


def get_games(date_: dt.date) -> list[Game]:
    games = (
        read_dataframe("schedule")
        .with_columns(
            pl.col("start_time").dt.convert_time_zone("America/Denver"),
        )
        .filter(pl.col("start_time").dt.date().eq(date_))
        .sort("start_time")
        .to_dicts()
    )

    return [
        Game(
            start_time=game["start_time"],
            is_start_time_tbd=game["is_start_time_tbd"],
            home_team=game["home_team"],
            away_team=game["away_team"],
        )
        for game in games
    ]


def schedule_notifications_job() -> None:
    today = dt.date.today()
    games = get_games(today)

    for game in games:
        if not game.is_start_time_tbd:
            message = (
                f"Alert: {game.home_team} vs. {game.away_team} starts in 30 minutes!"
            )
            notification_time = game.start_time - dt.timedelta(minutes=30)
            schedule_message(
                channel=Channel.Testing, text=message, schedule_time=notification_time
            )
