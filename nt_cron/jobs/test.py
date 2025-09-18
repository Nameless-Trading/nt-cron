import datetime as dt
from zoneinfo import ZoneInfo
from nt_cron.slack import schedule_message, Channel, list_scheduled_messages, clear_scheduled_messages

def test_run():
    today = dt.date.today()
    time = dt.time(16, 0, tzinfo=ZoneInfo('America/Denver'))
    notification_time = dt.datetime.combine(today, time).astimezone(ZoneInfo('UTC'))
    print(f"notification_time = {notification_time}")

    schedule_message(
        channel=Channel.Testing,
        text=f'Test: notification_time = {notification_time}',
        schedule_time=notification_time
    )

# if __name__ == '__main__':
#     test_run()
#     print(
#         list_scheduled_messages(Channel.Testing)
#     )

    # 1758232740

    # 1758232740