import unittest


class MyTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)  # add assertion here

    def test_pandas(self):
        import pandas as pd
        pd_df = pd.read_csv(
            "1800.csv",
            names=["stationID", "date", "measure_type", "temperature"],
            usecols=[0, 1, 2, 3]
        )

        pd_df.head()

        # Filter out all but TMIN entries
        pd_minTemps = pd_df[pd_df['measure_type'] == "TMIN"]

        pd_minTemps.head()

        # Select only stationID and temperature
        pd_stationTemps = pd_minTemps[["stationID", "temperature"]]

        # Aggregate to find minimum temperature for every station
        pd_minTempsByStation = pd_stationTemps.groupby(["stationID"]).min("temperature")
        pd_minTempsByStation.head()

    def test_spark(self):
        from pyspark.sql import SparkSession
        from pyspark import SparkConf
        conf = SparkConf()
        conf.set("spark.app.name", "PySpark DataFrame #1")
        conf.set("spark.master", "local[*]")

        spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

        df = spark.read.format("csv").load("1800.csv")

        df.printSchema()

        df = spark.read.format("csv") \
            .load("1800.csv") \
            .toDF("stationID", "date", "measure_type", "temperature", "_c4", "_c5", "_c6", "_c7")

        df.printSchema()

        df = spark.read.format("csv") \
            .option("inferSchema", "true") \
            .load("1800.csv") \
            .toDF("stationID", "date", "measure_type", "temperature", "_c4", "_c5", "_c6", "_c7")

        df.printSchema()

    def test_spark_sql(self):
        from pyspark.sql import SparkSession
        from pyspark import SparkConf
        from pyspark.sql.types import StringType, IntegerType, FloatType
        from pyspark.sql.types import StructType, StructField

        conf = SparkConf()
        conf.set("spark.app.name", "PySpark DataFrame #1")
        conf.set("spark.master", "local[*]")

        spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

        schema = StructType([
            StructField("stationID", StringType(), True),
            StructField("date", IntegerType(), True),
            StructField("measure_type", StringType(), True),
            StructField("temperature", FloatType(), True)])

        # df = spark.read.schema(schema).format("csv").load("1800.csv")
        df = spark.read.schema(schema).csv("1800.csv")

        df.printSchema()

        # Filter out all but TMIN entries
        minTemps = df.filter(df.measure_type == "TMIN")

        minTemps.count()

        # Column expression으로 필터링 적용
        minTemps = df.where(df.measure_type == "TMIN")

        minTemps.count()

        # SQL expression으로 필터링 적용
        minTemps = df.where("measure_type = 'TMIN'")

        minTemps.count()

        # Aggregate to find minimum temperature for every station
        minTempsByStation = minTemps.groupBy("stationID").min("temperature")
        minTempsByStation.show()

        # Select only stationID and temperature
        stationTemps = minTemps[["stationID", "temperature"]]

        stationTemps.show(5)

        stationTemps = minTemps.select("stationID", "temperature")

        stationTemps.show(5)

        # Collect, format, and print the results
        results = minTempsByStation.collect()

        for result in results:
            print(result[0] + "\t{:.2f}F".format(result[1]))

        df.createOrReplaceTempView("station1800")
        results = spark.sql("""SELECT stationID, MIN(temperature)
        FROM station1800
        WHERE measure_type = 'TMIN'
        GROUP BY 1
        """).collect()

        for r in results:
            print(r)


if __name__ == '__main__':
    unittest.main()
