"""
  Using JSONpath-like in SQL queries.

 @author rambabu.posa
"""
import os
from pyspark.sql import (SparkSession, functions as F)
import arguments as utils


def main(spark,filename):

    # Reads a JSON, stores it in a dataframe
    df = spark.read.format("json") \
        .option("multiline", True) \
        .load(filename)

   # Explode the array
    df = df.withColumn("items", F.explode(F.col("store.book")))

    # Creates a view so I can use SQL
    df.createOrReplaceTempView("books")
    sqlQuery = "SELECT items.author,* FROM books WHERE items.category = 'reference'"

    authorsOfReferenceBookDf = spark.sql(sqlQuery)
    authorsOfReferenceBookDf.show(truncate=False)

if __name__ == "__main__":

    args = utils.args_reader()

    # Creates a session on a local master
    spark = SparkSession.builder.appName("Query on a JSON doc") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark,args.datapath)
    spark.stop()
