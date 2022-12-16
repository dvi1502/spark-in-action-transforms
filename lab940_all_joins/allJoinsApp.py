"""
 All joins in a single app, inspired by
 https://stackoverflow.com/questions/45990633/what-are-the-various-join-types-in-spark.
 Used in Spark in Action 2e, http://jgp.net/sia

 @author rambabu.posa
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField,
                               IntegerType, StringType)
import arguments as utils

join_types = [
    "inner",
    "outer",
    "full",
    "full_outer",
    "left",
    "left_outer",
    "right",
    "right_outer",
    "left_semi",
    "left_anti",
    "cross"
]


def create_left_df(spark):

    schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('value', StringType(), True)
    ])

    rows = [
             (1, "Value 1 (left)"),
             (2, "Value 2 (left)"),
             (3, "Value 3 (left)"),
             (4, "Value 4 (left)")
           ]
    return spark.createDataFrame(rows, schema)

def create_right_df(spark):

    schema = StructType([
            StructField('id', IntegerType(), True),
            StructField('value', StringType(), True)
        ])

    rows = [
        (3, "Value 3 (right)"),
        (4, "Value 4 (right)"),
        (4, "Value 4_1 (right)"),
        (5, "Value 5 (right)"),
        (6, "Value 6 (right)")
    ]
    return spark.createDataFrame(rows, schema)

def main(spark):
    left_df = create_left_df(spark)
    left_df.show()

    right_df = create_right_df(spark)
    right_df.show()

    for join_type in join_types:
        print( "{0} {1}".format(join_type.upper(),"JOIN"))
        df = left_df.join(right_df, left_df["id"] == right_df["id"], join_type)
        df.orderBy(left_df["id"]).show()

    print("CROSS JOIN (without a column)")
    df = left_df.crossJoin(right_df)
    df.orderBy(left_df["id"]).show()

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession\
        .builder\
        .appName("All joins!") \
        .master("local[*]")\
        .getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()
