import os
import shutil
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


os.environ["TZ"] = "UTC"
os.environ["JUPYTER_PLATFORM_DIRS"] = "1"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


if not hasattr(F, "try_to_date"):
    from pyspark.sql.functions import try_to_timestamp as _try_ts

    def _try_to_date(col, fmt=None):
        ts = _try_ts(col, F.lit(fmt)) if fmt else _try_ts(col)
        return ts.cast("date")

    F.try_to_date = _try_to_date

    import pyspark.sql.functions as _fmod
    _fmod.try_to_date = _try_to_date


@pytest.fixture(scope="session")
def spark():
    if shutil.which("java") is None:
        pytest.skip("Java is required to run Spark integration tests")

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
