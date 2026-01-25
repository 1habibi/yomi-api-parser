import logging
from logging.handlers import RotatingFileHandler
from config import LOG_FILE


def setup_logger(name: str = __name__) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        handler = RotatingFileHandler(
            LOG_FILE,
            maxBytes=5*1024*1024,  # 5MB
            backupCount=5,
            encoding="utf-8"
        )
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

logger = setup_logger("api-parser")
