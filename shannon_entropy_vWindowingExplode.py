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

    def shannon_entropy(df, string_col):
        df_s = df
        df_s = df_s.withColumn("id", F.monotonically_increasing_id())
        df_s = df_s.withColumn("chars", F.split(F.col(string_col), ""))
        # For some reason I don't need to get rid of the extra comma at the end. Look into this on Zep
        df_s = df_s.withColumn("string_len", F.length(F.col(string_col)))

        char_pos_explode = df_s.select("id", string_col, "string_len", F.explode("chars").alias("char"))

        window_spec_id = Window.partitionBy("id")
        window_spec_char = Window.partitionBy("id", "char")

        df_agg = char_pos_explode.withColumn("char_count", F.count("char").over(window_spec_char))

        df_distinct = df_agg.dropDuplicates()

        df_probability = df_distinct.withColumn("probability", F.col("char_count") / F.col("string_len")).drop("char_count", "string_len", "char")

        df_entropy_term = df_probability.withColumn("entropy_term", -F.col("probability") * F.log2(F.col("probability")))

        df_final = df_entropy_term.withColumn("shannon_entropy", F.sum("entropy_term").over(window_spec_id)).drop("entropy_term", "probability")
        df_final = df_final.dropDuplicates().drop("id")

        return df_final

    # TODO
    # Create a function that uses transform instead 

    df_shannon = shannon_entropy(df, "my_string")
    df_shannon.show(5000,False)

    spark.stop()

if __name__ == "__main__":
    main()