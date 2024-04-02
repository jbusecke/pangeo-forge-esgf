"""Not sure if this is working but I want to somehow test that logging is active and working as expected."""

from pangeo_forge_esgf import setup_logging


def test_setup_logging_smoketest():
    setup_logging(level="DEBUG")
