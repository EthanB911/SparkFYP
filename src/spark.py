from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import matplotlib.pyplot as plt 


# sc = SparkContext(master="spark://localhost:7077", appName="test")
# spark = SparkSession \ 
#     .builder \
#     .master("spark://localhost:7077") \     
#     .appName("Word Count") \
#     .config("spark.some.config.option", "some-value") \
#     .getOrCreate()
def get_spark_session():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Word Count") \
        .config("spark.executor.memory", "1000m") \
        .config("spark.executor.instances", "4") \
        .config("spark.executor.cores", "1") \
        .getOrCreate()
    return spark
    # conf = SparkConf().setAppName('hello').setMaster('local[*]').setSparkHome('/opt/spark/')
    # sc = SparkContext(conf=conf)
    # return sc



#we can use the columns attribute just like with pandas
# spark = get_spark_session()
# df = spark.read.csv('/Users/ethan/Downloads/Exportcompletetransactionhistory.csv', header='true', inferSchema = True)
# columns = df.columns
# print('The column Names are:')
# for i in columns:
#     print(i)

# print("hello") 
