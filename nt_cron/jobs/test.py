import datetime as dt
from zoneinfo import ZoneInfo
from nt_cron.slack import schedule_message, Channel, list_scheduled_messages
from rich import print

def test_run():
    today = dt.date.today()
    time = dt.time(15, 42, tzinfo=ZoneInfo('America/Denver'))
    notification_time = dt.datetime.combine(today, time)
    print(f"notification_time = {notification_time}")

    schedule_message(
        channel=Channel.Testing,
        text=f'Test: notification_time = {notification_time}',
        schedule_time=notification_time
    )