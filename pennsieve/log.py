from __future__ import absolute_import, division, print_function

import logging
import os

logging.basicConfig()


def get_logger(name="pennsieve-python"):
    """
    Returns a logger configured to be used throughout the
    pennsieve-python library

    Args:
      name (string): Name of the logger, default "pennsieve-python"

    Returns:
      Logger
    """

    logger = logging.getLogger(name)
    logger.setLevel(os.environ.get("PENNSIEVE_LOG_LEVEL", "INFO"))

    return logger
