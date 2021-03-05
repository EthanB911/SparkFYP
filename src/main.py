import mysql.connector
from pandas import DataFrame
from trie import add, TrieNode
from spark import get_spark_session
from pyspark.sql.types import Row, StructType, StructField, StringType

connection = mysql.connector.connect(
  host="localhost",
  user="root",
  password="!Ethan911!",
  database="uniprot2020_Dec"
)

def get_data():
    sql_select_Query = "select * from GoGraph_proteincathfunfamily limit 10"
    # MySQLCursorDict creates a cursor that returns rows as dictionaries
    cursor = connection.cursor(dictionary=True)
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    print(type(records))
    return records

def printProteinData(data):
    print("Id Region Funfam                  Name")
    print("-----------------------------------------")
    for x in data:
        id = x["id"]
        reg = x["regions"]
        funfam = x["cathfunfamilyfull_id"]
        name = x["protein_name"]
        print(id, reg, funfam, name)




records = get_data()
printProteinData(records)
spark = get_spark_session()
print("rdd")
rdd = spark.sparkContext.parallelize(records)
print(type(rdd))
schema = StructType([ \
    StructField("id",StringType(),True), \
    StructField("regions",StringType(),True), \
    StructField("cathfunfamilyfull_id",StringType(),True), \
    StructField("protein_ref_id", StringType(), True), \
    StructField("protein_name", StringType(), True), \
    StructField("meets_inclusion_threshold", StringType(), True) \
  ])

# schema = StructType(['id', 'regions', 'cathfunfamilyfull_id', 'protein_ref_id', 'protein_name', 'meets_inclusion_threshold'])
df = spark.createDataFrame(rdd, schema=schema)

df.show()
# df = spark.createDataFrame(records)
# print(type(df))
