
import os
import logging


def current_dir():
    return os.path.dirname(os.path.abspath(__file__))


CURRENT_DIR = current_dir()

# ----------------------------- LOGGING -----------------------------
LOG_LEVEL = logging.DEBUG
LOG_FORMAT = ''
