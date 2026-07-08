import logging
from logging.handlers import RotatingFileHandler
import os
import sys

# Create logs directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Per-service fixed filename — set LOG_SERVICE_NAME in each systemd unit.
# Fixed name lets RotatingFileHandler's backupCount actually prune old rotations.
# backupCount=3 at 30MB each = 90MB max per service (covers one full trading day).
_service_name = os.environ.get("LOG_SERVICE_NAME", "trade_project")
log_file = os.path.join(log_dir, f"{_service_name}.log")

# Configure logging
log_formatter = logging.Formatter(
    '%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Get the root logger
logger = logging.getLogger()

# Prevent duplicate handlers
if not logger.hasHandlers():
    # File handler for logging
    file_handler = RotatingFileHandler(
        log_file, maxBytes=30 * 1024 * 1024, backupCount=3  # 30MB per file, 3 backups = 90MB max
    )
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.INFO)

    # Console handler for logging
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.INFO)

    # Add handlers to the root logger
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# Redirect unhandled exceptions to logger
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = handle_exception

# Redirect sys.stderr to logger
class LoggingStream:
    def write(self, message):
        if message.strip():  # Ignore empty writes
            logger.error(message.strip())

    def flush(self):
        pass

sys.stderr = LoggingStream()

# To use this logger in other modules, import it using:
# from logging_config import logger
