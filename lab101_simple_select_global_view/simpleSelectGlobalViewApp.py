"""
Simple SQL select on ingested data, using a global view
@author  DVI
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType,StructField,
                               StringType,DoubleType)
import os
import arguments as utils


schema = StructType([
    StructField('geo',StringType(), True),
    StructField('yr1980', DoubleType(), False)
])

query1 = """
    SELECT * FROM global_temp.geodata 
    WHERE yr1980 < 1 
    ORDER BY 2 
    LIMIT 5
"""

query2 = """
    SELECT * FROM global_temp.geodata
    WHERE yr1980 >= 1 
    ORDER BY 2 
    LIMIT 5
"""


if __name__ == "__main__":

    args = utils.args_reader()

    spark = SparkSession\
        .builder\
        .appName("Simple SELECT using SQL") \
        .master("local[*]") \
        .getOrCreate()


    df = spark.read.format("csv") \
        .option("header", True) \
        .schema(schema) \
        .load(args.datapath)

    df.createOrReplaceGlobalTempView("geodata")
    df.printSchema()

    smallCountriesDf = spark.sql(query1)
    smallCountriesDf.show(10, False)

    spark2 = spark.newSession()
    slightlyBiggerCountriesDf = spark2.sql(query2)
    slightlyBiggerCountriesDf.show(10, False)

    spark.stop()
    spark2.stop()

