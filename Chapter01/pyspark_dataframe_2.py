import unittest


class MyTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)

    def test_datafame(self):
        import pyspark.sql.functions as f
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as func
        from pyspark.sql.types import StructType, StructField, StringType, FloatType
        import os
        from pyspark.sql import SparkSession

        spark = (SparkSession.builder.master("local[*]")
                 .appName('Pyspark DataFrame #2')
                 .getOrCreate())

        # 데이터가 정상적으로 들어가있는지 확인
        # print(os.system("head -5 customer-orders.csv"))

        schema = StructType([
            StructField("cust_id", StringType(), True),
            StructField("item_id", StringType(), True),
            StructField("amount_spent", FloatType(), True)
        ])

        # 생성된 DataFrame의 schema 확인
        df = spark.read.schema(schema).csv("customer-orders.csv")
        # df.printSchema()

        # cut_id를 기준으로 Group으로 묶고 amount_spent를 합산한다.
        df_ca = df.groupBy("cust_id").sum("amount_spent")
        # df_ca.show()

        # sum(amount_spent)의 column 명을 sum으로 변경
        df_ca = df.groupBy("cust_id").sum("amount_spent").withColumnRenamed("sum(amount_spent)", "sum")

        # df_ca.show(10)

        # pyspark.spl.functions 라이브러리를 이용하여 함수들로 간편하게 column명 변경
        df_ca = df.groupBy("cust_id").agg(f.sum('amount_spent').alias('sum'))
        # df_ca.show(5)

        rst = df.groupBy("cust_id").agg(
            f.sum('amount_spent').alias('sum'),
            f.max('amount_spent').alias('max'),
            f.avg('amount_spent').alias('avg')
        ).collect()

        # for row in rst:
        #     print(row)

        df.createOrReplaceGlobalTempView("customer_orders")
        spark.sql("""SELECT cust_id, SUM(amount_spent) sum, MAX(amount_spent) max, AVG(amount_spent) avg
        FROM customer_orders
        GROUP BY 1""").head(5)

        spark.catalog.listTables()





if __name__ == '__main__':
    unittest.main()
