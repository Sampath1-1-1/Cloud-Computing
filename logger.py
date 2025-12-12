# logger.py
# Simple colored logger with timestamps

from datetime import datetime
import sys

class Logger:
    @staticmethod
    def _timestamp():
        return datetime.now().strftime("%H:%M:%S")

    @staticmethod
    def info(message: str):
        print(f"\033[92m[INFO]  {Logger._timestamp()} - {message}\033[0m")
        sys.stdout.flush()

    @staticmethod
    def warn(message: str):
        print(f"\033[93m[WARN]  {Logger._timestamp()} - {message}\033[0m")
        sys.stdout.flush()

    @staticmethod
    def error(message: str):
        print(f"\033[91m[ERROR] {Logger._timestamp()} - {message}\033[0m")
        sys.stderr.flush()

# Global logger instance (easy to import and use)
logger = Logger()