from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def main():
    spark = SparkSession.builder \
        .appName("SimpleApp") \
        .getOrCreate()

    data = [("aa",), ("aa",), ("abcdefghijk123",), ("&*#/><...[]",), ("334gad355a  344 t44a",), ("",), ("a")]
    df = spark.createDataFrame(data, ["my_string"])

    def shannon_entropy(df, string_col):
        # Step 1: Get frequencies of each character in the string
        df_freq = df.withColumn('char', F.explode(F.expr("split(my_string, '')"))) \
                    .groupBy("my_string", "char") \
                    .agg(F.count("char").alias("char_count"))

        # Step 2: Calculate probability for each character
        total_length = F.length("my_string")
        df_prob = df_freq.withColumn("probability", F.col("char_count") / total_length)

        # # Step 3: Calculate the entropy
        # entropy_term = -F.col("probability") * F.log2(F.col("probability"))
        # df_entropy = df_prob.withColumn("entropy_term", entropy_term) \
        #                     .groupBy("my_string") \
        #                     .agg(F.sum("entropy_term").alias("shannon_entropy"))

        return df_prob

    df_shannon = shannon_entropy(df, "my_string")
    df_shannon.show(5000,False)

    spark.stop()

if __name__ == "__main__":
    main()