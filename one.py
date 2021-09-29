from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
from pyspark.sql.types import StructField,StructType,StringType,LongType,IntegerType
from pyspark.sql import Row

spark = SparkSession.builder.appName("structured_operations")\
    .master("local[*]")\
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partition","5")



df = spark.read.format("json").load(r"F:\SPARK_DEFINITIVE_PROJECTS\\data\Spark-The-Definitive-Guide\data\flight-data\json\2015-summary.json")


# df.printSchema()


# manual schema

myManualSchema = StructType([
    StructField("DEST_COUNTRY_NAME",StringType(),True),
    StructField("ORIGIN_COUNTRY_NAME",StringType(),True),
    StructField("count",LongType(),True)
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
StructField("names", LongType(), False)
])

myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()

# pg : 72

