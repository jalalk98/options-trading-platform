import logging
from logging.handlers import RotatingFileHandler
import os
import sys
from datetime import datetime

# Create logs directory if it doesn't exist
log_dir = r"D:\User\Desktop\Trade_Project\trade\Zerodha\custom_package\logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Generate a unique log filename based on the current timestamp
log_filename = datetime.now().strftime("trade_project_%d-%m-%Y___%H-%M-%S.log")
log_file = os.path.join(log_dir, log_filename)

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
        log_file, maxBytes=30 * 1024 * 1024, backupCount=15  # 30MB per file, 15 backups
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
