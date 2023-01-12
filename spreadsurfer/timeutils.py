from datetime import datetime, timezone


def now():
    return datetime.now(timezone.utc)


def timedelta_ms(start, end):
    return round(abs((end - start).total_seconds()) * 1000)
