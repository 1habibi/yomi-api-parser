from datetime import datetime, timezone, date
from typing import Optional
from config import YEAR_MIN, YEAR_MAX
from utils.logging_config import logger


def parse_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None

    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"

        dt = datetime.fromisoformat(s)

        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

        return dt

    except ValueError as e:
        logger.warning(f"Invalid datetime format '{s}': {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing datetime '{s}': {e}", exc_info=True)
        return None


def parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None

    try:
        if len(s) == 4 and s.isdigit():
            year = int(s)
            if YEAR_MIN <= year <= YEAR_MAX:
                return date(year, 1, 1)
            else:
                logger.warning(f"Year out of valid range [{YEAR_MIN}-{YEAR_MAX}]: {s}")
                return None
        return datetime.fromisoformat(s).date()

    except ValueError as e:
        logger.warning(f"Invalid date format '{s}': {e}")

        try:
            from dateutil.parser import parse
            dt = parse(s, fuzzy=True)
            if dt.year < YEAR_MIN or dt.year > YEAR_MAX:
                logger.warning(f"Year out of valid range in date: {s}")
                return None
            return dt.date()
        except (ImportError, ValueError) as fallback_error:
            logger.error(f"Failed to parse date '{s}': {fallback_error}")
            return None
    except Exception as e:
        logger.error(f"Unexpected error parsing date '{s}': {e}", exc_info=True)
        return None
