import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for sub in ("src/lambdas/ingest", "src/lambdas/data_quality", "src/glue", "scripts"):
    p = str(ROOT / sub)
    if p not in sys.path:
        sys.path.insert(0, p)


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark.sql")
    session = (
        pyspark.SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
