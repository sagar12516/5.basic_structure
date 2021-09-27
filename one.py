from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
from pyspark.sql.types import StructField,StructType,StringType,LongType

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

# pg  : 70