"""
Simple SQL select on ingested data
@author rambabu.posa - не уверен
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType,StructField,
                               StringType,DoubleType)
import arguments as utils


schema = StructType([
    StructField('geo',StringType(), True),
    StructField('yr1980', DoubleType(), False)
  ])


def read_csv_file(spark,filename):

    # Reads a CSV file with header, called books.csv, stores it in a dataframe
    df = spark.read.csv(
        header=True,
        inferSchema=True,
        schema=schema,
        path=filename
    )

    df.createOrReplaceTempView('geodata')
    df.printSchema()

    query = """
      SELECT * FROM geodata
      WHERE yr1980 < 1
      ORDER BY 2
      LIMIT 15
    """

    smallCountries = spark.sql(query)
    smallCountries.show(10, False)



if __name__ == "__main__":

    args = utils.args_reader()

    spark = SparkSession\
        .builder\
        .appName("Simple SELECT using SQL") \
        .master("local[*]") \
        .getOrCreate()

    read_csv_file(
        spark,
        args.datapath
    )

    spark.stop()


