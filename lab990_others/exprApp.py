"""
 Use of expr().

 @author rambabu.posa
"""
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType, StructField,
                               StringType, IntegerType)

schema = StructType([
    StructField('title', StringType(), False),
    StructField('start', IntegerType(), False),
    StructField('end', IntegerType(), False)
])

rows = [
    ("bla0", 10, 300),
    ("bla1", 10, 78),
    ("bla3", 11, 64),
    ("bla2", 10, 20)
]

def main(spark):

    df = spark.createDataFrame(rows, schema)
    df.show()

    df = df \
        .withColumn("time_spent", F.expr("end - start")) \
        .withColumn("time_avg", F.expr("(end + start) / 2")) \
        .drop("start") \
        .drop("end") \
        .orderBy("title")

    df.show()

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession\
        .builder\
        .appName("expr()") \
        .master("local[*]")\
        .getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()
