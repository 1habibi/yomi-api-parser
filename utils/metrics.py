from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from utils.logging_config import logger


@dataclass
class SyncMetrics:
    added_count: int = 0
    updated_count: int = 0
    unchanged_count: int = 0
    errors_count: int = 0
    start_time: Optional[datetime] = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None

    @property
    def total_count(self) -> int:
        return self.added_count + self.updated_count + self.unchanged_count

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    def mark_added(self) -> None:
        self.added_count += 1

    def mark_updated(self) -> None:
        self.updated_count += 1

    def mark_unchanged(self) -> None:
        self.unchanged_count += 1

    def mark_error(self) -> None:
        self.errors_count += 1

    def finish(self) -> None:
        self.end_time = datetime.now()

    def log_summary(self) -> None:
        self.finish()
        duration = self.duration_seconds

        logger.info("=" * 60)
        logger.info("Sync Summary:")
        logger.info(f"  Total records processed: {self.total_count}")
        logger.info(f"  Added: {self.added_count}")
        logger.info(f"  Updated: {self.updated_count}")
        logger.info(f"  Unchanged: {self.unchanged_count}")
        logger.info(f"  Errors: {self.errors_count}")
        if duration:
            logger.info(f"  Duration: {duration:.2f} seconds")
        logger.info("=" * 60)
