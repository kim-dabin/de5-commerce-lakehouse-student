#!/usr/bin/env python3
"""R8 mentor demo: blow up the Spark driver heap on purpose.

Builds memory-heavy rows and pulls them all to the driver with collect(). With a small
--driver-memory this exhausts the driver heap (java.lang.OutOfMemoryError). It models
"a job that collect()s too much / skew overwhelms the driver", contained to this single
spark-submit JVM (capped by -Xmx via --driver-memory), so it does not endanger the rest
of the shared stack.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    spark = SparkSession.builder.appName("ops-r8-spark-driver-oom").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    n_rows = int(os.environ.get("OOM_ROWS", "5000000"))
    width = int(os.environ.get("OOM_WIDTH", "500"))

    print(f"collecting {n_rows} rows x ~{width} bytes to the driver (expect OOM)...")
    df = spark.range(0, n_rows).select(
        F.col("id"),
        F.expr(f"repeat('x', {width})").alias("payload"),
    )
    rows = df.collect()  # pulls the whole dataset into the driver heap
    print(f"unexpectedly collected {len(rows)} rows (increase OOM_ROWS or lower --driver-memory)")
    spark.stop()


if __name__ == "__main__":
    main()
