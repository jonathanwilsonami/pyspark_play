from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def main():
    spark = SparkSession.builder \
        .appName("SimpleApp") \
        .getOrCreate()

    data = [("aa",), ("aa",), ("abcdefghijk123",), ("&*#/><...[]",), ("334gad355a  344 t44a",), ("",)]
    df = spark.createDataFrame(data, ["my_string"])

    # df.show()


    def shannon_entropy(df, column_name):
        my_expr = f"string_split({column_name}, '')"
        # Compute character frequencies as a map
        # freq_expr = f"AGGREGATE(SEQUENCE(0, LENGTH({column_name}) - 1), CAST(MAP() AS MAP<STRING, INT>), (acc, i) -> map_concat(acc, MAP({column_name}[i], 1)))"
        
        # # Compute Shannon entropy using the formula
        # # entropy_expr = f"-SUM(VALUE * LOG(2, VALUE / LENGTH({column_name}))) FROM EXPLODE({freq_expr})"
        # # entropy_expr = f"LOG(2, 55 / LENGTH({column_name}))"
        
        # df_transformed = df.withColumn("entropy", F.expr(freq_expr))
        df_transformed = df.withColumn("trans", F.expr(my_expr))
        
        return df_transformed

    df_shannon = shannon_entropy(df, "my_string")
    df_shannon.show(5000,False)

    spark.stop()

if __name__ == "__main__":
    main()