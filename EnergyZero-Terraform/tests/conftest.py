"""
conftest.py — shared pytest fixtures for EnergyZero test suite.
"""
import pytest


@pytest.fixture(scope="session", autouse=True)
def suppress_spark_logging():
    """Reduce Spark log noise during test runs."""
    import logging
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)
