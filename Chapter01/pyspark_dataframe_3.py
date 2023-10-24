import unittest
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import *


class MyTestCase(unittest.TestCase):

    def test_something(self):
        self.assertEqual(True, False)

    def test_dataframe(self):
        conf = SparkConf()
        conf.set("spark.app.name", "PySpark DataFrame #3")
        conf.set("spark.master", "local[*]")

        spark = (SparkSession.builder
                 .config(conf=conf)
                 .getOrCreate())

        schema = StructType([StructField("text", StringType(), True)])
        transfer_cost_df = spark.read.schema(schema).text("transfer_cost.txt")
        transfer_cost_df.show(truncate=False)


        # transfer_cost 안에 있는 데이터 중 임의의 데이터
        # On 2021-01-04 the cost per ton from 85002 to 85010 is 19.80 at Haul Today
        regex_str = r'On (\S+) the cost per ton from (\d+) to (\d+) is (\S+) at (.*)'

        df_with_new_columns = transfer_cost_df \
            .withColumn('week', regexp_extract('text', regex_str, 1)) \
            .withColumn('departure_zipcode', regexp_extract(column('text'), regex_str, 2)) \
            .withColumn('arrival_zipcode', regexp_extract(transfer_cost_df.text, regex_str, 3)) \
            .withColumn('cost', regexp_extract(col('text'), regex_str, 4)) \
            .withColumn('vendor', regexp_extract(col('text'), regex_str, 5))

        df_with_new_columns.printSchema()

        final_df = df_with_new_columns.drop("text")

        final_df.write.csv("extracted.csv")

        final_df.write.format("json").save("extracted.json")

if __name__ == '__main__':
    unittest.main()
