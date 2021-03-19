import logging
import os


def test_default_log_level():
    from pennsieve import log

    # default log level should be INFO
    base_log = log.get_logger("base_log")
    assert base_log.getEffectiveLevel() == logging.INFO


def test_log_level_based_on_env():
    from pennsieve import log

    # setting env var should change logging level
    os.environ["PENNSIEVE_LOG_LEVEL"] = "WARN"
    warn_log = log.get_logger("warn_log")
    assert warn_log.getEffectiveLevel() == logging.WARN
