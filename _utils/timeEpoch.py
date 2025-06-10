from datetime import datetime, timedelta, timezone
import re

def convert_relative_time_to_epoch(relative_time: str) -> int:
  
    now = datetime.now(timezone.utc)

    time_map = {
        'second': 'seconds',
        'seconds': 'seconds',
        'minute': 'minutes',
        'minutes': 'minutes',
        'hour': 'hours',
        'hours': 'hours',
        'day': 'days',
        'days': 'days',
        'week': 'weeks',
        'weeks': 'weeks',
        'month': 'days',   
        'months': 'days',
        'year': 'days',    
        'years': 'days'
    }

    match = re.match(r"(\d+)\s+(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)\s+ago", relative_time.strip())
    if not match:
        return int(now.timestamp())

    value, unit = int(match.group(1)), match.group(2)

    if unit in ['month', 'months']:
        delta = timedelta(days=value * 30)
    elif unit in ['year', 'years']:
        delta = timedelta(days=value * 365)
    else:
        delta = timedelta(**{time_map[unit]: value})

    target_time = now - delta
    return int(target_time.timestamp())

  