from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
from pyspark.sql.types import StructField,StructType,StringType,LongType,IntegerType
from pyspark.sql import Row




spark = SparkSession.builder.appName("structured_operations")\
    .master("local[*]")\
    .getOrCreate()

# spark.conf.set("spark.sql.shuffle.partition","5")
# spark.conf.set("spark.executor.heartbeatInterval","3600s")



df = spark.read.format("json").load(r"F:\SPARK_DEFINITIVE_PROJECTS\\data\Spark-The-Definitive-Guide\data\flight-data\json\2015-summary.json")


# df.printSchema()


# manual schema

myManualSchema = StructType([
    StructField("DEST_COUNTRY_NAME",StringType(),True),
    StructField("ORIGIN_COUNTRY_NAME",StringType(),True),
    StructField("count",IntegerType(),True)
])

df = spark.read.format("json").schema(myManualSchema).load(r"F:\SPARK_DEFINITIVE_PROJECTS\\data\Spark-The-Definitive-Guide\data\flight-data\json\2015-summary.json")


df.select(col("DEST_COUNTRY_NAME"))


df.columns

df.first()

#  Creating the Rows

myRow = Row("Hello",None,1,False)

# print(myRow[0])

df = spark.read.format("json").load(r"F:\SPARK_DEFINITIVE_PROJECTS\\data\Spark-The-Definitive-Guide\data\flight-data\json\2015-summary.json")

df.createOrReplaceTempView("df_table")


myManualSchema = StructType([
StructField("some", StringType(), True),
StructField("col", StringType(), True),
StructField("names", IntegerType(), False)
])

myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
# myDf.show()

df.select("DEST_COUNTRY_NAME")

df.select(col("DEST_COUNTRY_NAME"),"DEST_COUNTRY_NAME")


######### selectExpr ############

df.select(expr("DEST_COUNTRY_NAME AS destination"),"DEST_COUNTRY_NAME")

df.selectExpr("DEST_COUNTRY_NAME AS destination","upper(DEST_COUNTRY_NAME)")

df.selectExpr("avg(count)","count(DEST_COUNTRY_NAME)")

df.select(expr("*"),lit(1).alias("one"))

## add columns ##
df.withColumn("numberOne",lit(1))

df.withColumn("withCountry",expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))


## columns Rename ##

# print(df.withColumn("Destination",expr("DEST_COUNTRY_NAME")).columns)

# print(df.withColumnRenamed("DEST_COUNTRY_NAME","dest").columns)

#Note :  backticks  will be used in selectExpr function only

dfWithLongColName = df.withColumn("This long col name",expr("ORIGIN_COUNTRY_NAME"))

# one way to change col name
dfWithLongColName.withColumnRenamed("This long col name","new col")

# other way to change the col name

dfWithLongColName.selectExpr("`This long col name` as `new col`")

dfWithLongColName.createOrReplaceTempView("dfTableLong")

spark.sql("""
SELECT `This long col name`, `This long col name` as `new col` from dfTableLong
""")

# By default spark is insensitive ; however you can make spark case sensitive by setting the configuration:

# set spark.sql.caseSensitive true

##### REMOVING COLUMNS ######

df.drop("ORIGIN_COUNTRY_NAME").columns

## drop multiple columns
# print(df.drop("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME").columns)



# Casting

df.withColumn("count2",col("count").cast("long"))


df.filter(col("count") < 2)

df.where("count < 2")


# multiple filter conditions

df.where(col("count")<2).where(trim(col("DEST_COUNTRY_NAME")) != "Croatia")\
    .where(trim(col("ORIGIN_COUNTRY_NAME")) != "Croatia")

df.createOrReplaceTempView("df")

spark.sql("select * from df where count > 2 ")

# print(df.select("ORIGIN_COUNTRY_NAME").count())

# print(df.select("ORIGIN_COUNTRY_NAME").distinct().count())


seed = 5

withReplacement = False

fraction = 0.5  ## which is 50% of rows will be selected

df.sample(seed=seed,fraction=fraction,withReplacement=withReplacement).count()

#### Randomsplits ####
## it will split the dataframes based on the fractions u specified .

dataframes = df.randomSplit([0.25,0.75],seed)

dataframes[0].count()  #60

dataframes[1].count()  #196


## Concatenating and Appending Rows (Union)

schema = df.schema

newRows = [Row("new country","other country",50),
           Row("new country 2","other country 2",40)]

parallelizedRows = spark.sparkContext.parallelize(newRows)

# print(parallelizedRows.collect())


#
newDF = spark.createDataFrame(parallelizedRows,schema)



df.union(newDF).where("count = 1").where(col("ORIGIN_COUNTRY_NAME") != "United States")

### SORTING ###

df.sort("count")
df.orderBy("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME")
df.orderBy(expr("DEST_COUNTRY_NAME desc"), expr("ORIGIN_COUNTRY_NAME desc"))

import time
start = time.time()
df.sort("count")  ## sort is an alias for order by



spark.read.format("json").load(r"F:\SPARK_DEFINITIVE_PROJECTS\data\Spark-The-Definitive-Guide\data\flight-data\json\2015-summary.json").sortWithinPartitions("count")

## LIMIT ###

df.limit(5)


""" 
REPARTITION AND COALESCE
REPARTITION : it will do full shuffling that means all the partitioned data will move 
from one partition to another, you need to choose this function only when your future partition is greater
than current one, it means if u want to increase the partitions for the dataframes

COALESCE : it will not incur full shuffle and will try to combine partitions .
"""

## to get current partition number
df.rdd.getNumPartitions()  #1

df = df.repartition(5)

df.rdd.getNumPartitions() # 5

## if u going to filter by a certain columns u can use repartition based on that column , so that column
# will be in separate partition and u can easily filter without taking time

df = df.repartition(col("DEST_COUNTRY_NAME"))

df.where("DEST_COUNTRY_NAME != 'Germany'")

# you can also specify the numbr of partitions u would like too

df = df.repartition(5,col("DEST_COUNTRY_NAME"))

df.repartition(5,col("DEST_COUNTRY_NAME")).coalesce(2)


## collect() : collect gets all the data from the entire dataframe
## take(n) : select first N rows
## show() : prints out a number of rows nicely

collectDF = df.limit(10)
collectDF.take(5)
collectDF.show()
collectDF.show(5,False)
collectDF.collect()


# IMPORTANT
"""
There’s an additional way of collecting rows to the driver in order to iterate over the entire
dataset. The method toLocalIterator collects partitions to the driver as an iterator. This
method allows you to iterate over the entire dataset partition-by-partition in a serial manner
"""

collectDF.toLocalIterator()



