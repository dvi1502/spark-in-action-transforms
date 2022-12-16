"""
    Use of from_unixtime().
    @author Dmitry Ivakin from RUSSIA
"""

import time
import random
import logging
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType, StructField,
                               StringType, IntegerType)

def time_serias_generator(size=1000):
    now = int(time.time())
    for i in range(0, size):
        yield (i, now)
        now = now + (random.randint(1,3) + 1)

schema = StructType([
    StructField('event', IntegerType(), False),
    StructField('ts', StringType(), False)
])

def main(spark):
    log = logging.getLogger("noname")

    df = spark.createDataFrame(time_serias_generator(10), schema)
    df.show(5)

    # Turning the timestamps to dates
    print("""df.withColumn("date", F.from_unixtime(F.col("ts")))""")
    df = df.withColumn("date", F.from_unixtime(F.col("ts")))
    df.show()

    # Collecting the result and printing out
    timeRows = (row for row in df.collect())

    for row in timeRows:
         log.warning(f"{row[0]} : {row[1]} ({row[2]})")


if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession\
        .builder\
        .appName("from_unixtime()") \
        .master("local[*]")\
        .getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()

