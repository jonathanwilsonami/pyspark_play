# C2
## Launch local cluster 
pyspark --master local[*]

## Get unparsed data
prev = spark.read.csv("block*.csv")

## Creating data schema

### Scheam Inferance - Is very slow for large data sets
parsed = spark.read.option("header", "true").option("nullValue", "?").option("inferSchema", "true").csv("block*.csv")
parsed.printSchema()

### Faster - Create own schema using pyspark.sql.types.StructType
from pyspark.sql.types import * 

schema = StructType([StructField("id_1", IntegerType(), False), StructField("id_2", StringType())])
spark.read.scehma(scehma.csv("..."))

### Define schema using a DDL (data definition lang)
schema = "id_1 INT, id_2 STRING..."

## Cache parsed data. If you don't every action you perform has to reparse the data over and over wasting time. Cache 
## saves the data to a cluster or your local machine so it does not have to do this over and over. 
parsed.cache()

## Get record counts 
from pyspark.sql.functions import col 
parsed.groupBy("is_match").count().orderBy(col("count").desc()).show()

## Creating a temp view for SQL queries on data. The downsides are that SQL can be difficult to express complex
##, multistage analyse in a dynamic readable and testable way. Opt for DataFrame APIs when possible. 
parsed.createOrReplaceTempView("linkage")
spark.sql("""
    SELECT is_match, COUNT(*) cnt
    FROM linkage
    """).show()

## Can also use HiveQL by enabling the Hive support on the spark session 

## Filtering 
misses = parsed.filter(col("is_match") == False)

## Convert Spark DF to Pandas DF !Caution! Only do this for relativly small data sets i.e. data that can fit in local memory 
summary_p = summary.toPandas()
### Now you can use pandas functions on this. 

### Convert back to Spark DF
summaryT = spark.create.createDataFrame(summary_p)
### But you will need to add the data types back into the DF as they default to String type
summaryT = summaryT.withColumn(c, summaryT[c].cast(DoubleType())) # Can put this in a loop to iterate over all cols where c is col

summary = parsed.describe()
summary.select("summary", ).show()


## Useful 
parsed.first()
parsed.count()
parsed.collect() # action that returns an Array of all Row objects. This array resides in local memory. 
summary_stats = parsed.describe()
summary_stats.show()
parsed.where(col("is_match") == True).count()