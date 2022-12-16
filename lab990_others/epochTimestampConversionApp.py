"""
 Use of from_unixtime() and unix_timestamp().

 @author Dmitry Ivakin from RUSSIA
"""
import time
import random
import logging
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType, StructField,
                               StringType, IntegerType)

schema = StructType([
    StructField('event', IntegerType(), False),
    StructField('original_ts', StringType(), False)
])

def time_serias_generator(size=1000):
    now = int(time.time())
    for i in range(0, size):
        yield (i, now)
        now = now + (random.randint(1,3) + 1)

def main(spark):

    log = logging.getLogger(__name__)

    df = spark.createDataFrame(time_serias_generator(), schema)
    # df.show()
    # df.printSchema()

    # Turning the timestamps to Timestamp datatype
    # timestamp, format='yyyy-MM-dd HH:mm:ss')
    log.warning(""" df.withColumn('date', F.from_unixtime(df.original_ts).cast('timestamp')) """)
    df = df.withColumn('date', F.from_unixtime(df.original_ts).cast('timestamp'))
    df.show(truncate=False)
    df.printSchema()

    # Turning back the timestamps to epoch
    log.warning(""" df.withColumn('epoch', F.unix_timestamp(df.date)) """)
    df = df.withColumn('epoch', F.unix_timestamp(df.date))
    df.show(truncate=False)
    df.printSchema()

    # Collecting the result and printing out
    for row in (row for i, row in enumerate(df.collect()) if i < 20):
        log.warning("{0} : {1} ({2})".format(row[0], row[1], row[2]))

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession.builder.appName("from_unixtime()") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()
