from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("SimpleApp") \
        .getOrCreate()

    data = [("aa",), ("abcdefghijk123",), ("&*#/><...[]",), ("334gad355  344 t44",), ("",)]
    df = spark.createDataFrame(data, ["my_string"])

    df.show()

    spark.stop()

if __name__ == "__main__":
    main()