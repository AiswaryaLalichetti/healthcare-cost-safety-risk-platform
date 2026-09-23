"""
Quick test: confirms PySpark can actually start a Spark session and run a
basic operation, before we build real processing logic on top of it.
"""

import sys
import os

# Windows-specific fix: explicitly tell Spark which Python to use, and force
# the driver to bind to the loopback address. Without this, the JVM and the
# Python worker process it spawns often fail to find each other on Windows.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

def test_spark():
    spark = (
        SparkSession.builder
        .appName("SparkSanityCheck")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

    data = [("CMS", "structured"), ("openFDA", "semi-structured"), ("PDF manuals", "unstructured")]
    df = spark.createDataFrame(data, ["source", "data_type"])

    print("Spark session started successfully. Sample DataFrame:")
    df.show()

    spark.stop()
    print("Spark session stopped cleanly.")

if __name__ == "__main__":
    test_spark()
