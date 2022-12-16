"""
  Simple SQL select on ingested data after preparing
    the data with the dataframe API.
  @author dvi
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType,StructField,
                               StringType,DoubleType)
import os
import arguments as utils

schema = StructType([
    StructField('geo',StringType(), True),
    StructField('yr1980', DoubleType(), False),
    StructField('yr1981', DoubleType(), False),
    StructField('yr1982', DoubleType(), False),
    StructField('yr1983', DoubleType(), False),
    StructField('yr1984', DoubleType(), False),
    StructField('yr1985', DoubleType(), False),
    StructField('yr1986', DoubleType(), False),
    StructField('yr1987', DoubleType(), False),
    StructField('yr1988', DoubleType(), False),
    StructField('yr1989', DoubleType(), False),
    StructField('yr1990', DoubleType(), False),
    StructField('yr1991', DoubleType(), False),
    StructField('yr1992', DoubleType(), False),
    StructField('yr1993', DoubleType(), False),
    StructField('yr1994', DoubleType(), False),
    StructField('yr1995', DoubleType(), False),
    StructField('yr1996', DoubleType(), False),
    StructField('yr1997', DoubleType(), False),
    StructField('yr1998', DoubleType(), False),
    StructField('yr1999', DoubleType(), False),
    StructField('yr2000', DoubleType(), False),
    StructField('yr2001', DoubleType(), False),
    StructField('yr2002', DoubleType(), False),
    StructField('yr2003', DoubleType(), False),
    StructField('yr2004', DoubleType(), False),
    StructField('yr2005', DoubleType(), False),
    StructField('yr2006', DoubleType(), False),
    StructField('yr2007', DoubleType(), False),
    StructField('yr2008', DoubleType(), False),
    StructField('yr2009', DoubleType(), False),
    StructField('yr2010', DoubleType(), False)
])
query1 = """
  SELECT * FROM geodata
  WHERE geo IS NOT NULL AND evolution<=0
  ORDER BY evolution
  LIMIT 25
"""
query2 = """
  SELECT * FROM geodata
  WHERE geo IS NOT NULL AND evolution>999999
  ORDER BY evolution DESC
  LIMIT 25
"""


if __name__ == "__main__":

    args = utils.args_reader()

    spark = SparkSession\
        .builder\
        .appName("Simple SQL") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.format("csv") \
        .option("header", True) \
        .schema(schema) \
        .load(args.datapath)

    df.printSchema()

    for i in range(1981,2010):
        df = df.drop(F.col("yr{}".format(i)))

    # Creates a new column with the evolution of the population between
    # 1980
    # and 2010
    df = df.withColumn("evolution", F.expr("round((yr2010 - yr1980) * 1000000)"))
    df.createOrReplaceTempView("geodata")


    negativeEvolutionDf = spark.sql(query1)

    # Shows at most 15 rows from the dataframe
    negativeEvolutionDf.show(15, False)


    moreThanAMillionDf = spark.sql(query2)
    moreThanAMillionDf.show(15, False)

    # Good to stop SparkSession at the end of the application
    spark.stop()


