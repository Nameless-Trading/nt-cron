import os
from slack_sdk import WebClient
from dotenv import load_dotenv
import datetime
from enum import Enum
from zoneinfo import ZoneInfo

load_dotenv(override=True)

token = os.getenv("SLACK_BOT_TOKEN")

client = WebClient(token=token)


class Channel(Enum):
    Testing = "C09F2T6SU2H"
    General = "C09FJV0AM33"


def send_message(channel: Channel, text: str) -> None:
    client.chat_postMessage(channel=channel.value, text=text)


def schedule_message(
    channel: Channel, text: str, schedule_time: datetime.datetime
) -> None:
    schedule_timestamp = str(int(schedule_time.astimezone(ZoneInfo('UTC')).timestamp()))

    client.chat_scheduleMessage(
        channel=channel.value, text=text, post_at=schedule_timestamp
    )


def list_scheduled_messages(channel: Channel):
    return client.chat_scheduledMessages_list(channel=channel.value)


def clear_scheduled_messages(channel: Channel):
    messages = list_scheduled_messages(channel)
    message_ids = [message["id"] for message in messages["scheduled_messages"]]
    for message_id in message_ids:
        client.chat_deleteScheduledMessage(
            channel=channel.value, scheduled_message_id=message_id
        )
