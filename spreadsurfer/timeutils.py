import math
from datetime import datetime, timezone


def now():
    return datetime.now(timezone.utc)


def timedelta_ms(start, end):
    return round(abs((end - start).total_seconds()) * 1000)


def now_isoformat():
    return datetime.utcnow().replace(microsecond=0).isoformat()


def timestamp_now_ms():
    return math.floor(datetime.now().timestamp() * 1000)
