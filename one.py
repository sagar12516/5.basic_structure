from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("structured_operations")\
    .master("local[*]")\
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partition","5")



df = spark.read.format("json").load(r"F:\SPARK_DEFINITIVE_PROJECTS\\data\Spark-The-Definitive-Guide\data\flight-data\json\2015-summary.json")


df.printSchema()
