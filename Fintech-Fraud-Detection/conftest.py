"""conftest.py — shared pytest config for Fintech-Fraud-Detection."""
import pytest
import logging


@pytest.fixture(scope="session", autouse=True)
def suppress_spark_logs():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)
