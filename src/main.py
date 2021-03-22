import mysql.connector
from pandas import DataFrame
from trie import add, TrieNode, find_matching_prefix
from spark import get_spark_session
from pyspark.sql.types import Row, StructType, StructField, StringType
from pyspark.sql import SQLContext
from Bio import SeqIO
from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *
import networkx as nx
import matplotlib.pyplot as plt

def PlotGraph(edges):
    gp = nx.from_pandas_edgelist(edges.toPandas(), 'src', 'dst')
    nx.draw(gp, with_labels=True)
    plt.show()


def fasta_to_trie():
    # file = SeqIO.to_dict(SeqIO.parse("/Users/ethan/Downloads/1.10.1870.10/full_alignments/1.10.1870.10-FF-000001.faa", "fasta"))
    file = SeqIO.parse("/Users/ethan/Downloads/1.10.1870.10/full_alignments/1.10.1870.10-FF-000001.faa", "fasta")

    #initialize trie
    root = TrieNode('1.10.1870.10-FF-000001')

    for seq_record in file:
        #print(seq_record.seq)
        add(root, seq_record.seq)

    print(find_matching_prefix(root, 'GF'))

#get faa sequences from files
#1st file is 1.10.170.10
def spark_graph():
    spark = get_spark_session()
    # spark.sparkContext.addPyFile('/Users/ethan/Downloads/graphframes-0.7.0-spark2.4-s_2.11.jar')
    sqlContext = SQLContext(spark.sparkContext)
    vertices = sqlContext.createDataFrame([
        ("a", "Alice", 34),
        ("b", "Bob", 36),
        ("c", "Charlie", 30),
        ("d", "David", 29),
        ("e", "Esther", 32),
        ("f", "Fanny", 36),
        ("g", "Gabby", 60)], ["id", "name", "age"])

    print(vertices)
    edges = sqlContext.createDataFrame([
        ("a", "b", "friend"),
        ("b", "c", "follow"),
        ("c", "b", "follow"),
        ("f", "c", "follow"),
        ("e", "f", "follow"),
        ("e", "d", "friend"),
        ("d", "a", "friend"),
        ("a", "e", "friend")
    ], ["src", "dst", "relationship"])
    print(edges.show())
    g = GraphFrame(vertices, edges)
    ## Take a look at the DataFrames
    g.vertices.show()
    g.edges.show()
    ## Check the number of edges of each vertex
    g.degrees.show()
    PlotGraph(edges)

spark_graph()


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

def print_protein_data(data):
    print("Id Region Funfam                  Name")
    print("-----------------------------------------")
    for x in data:
        id = x["id"]
        reg = x["regions"]
        funfam = x["cathfunfamilyfull_id"]
        name = x["protein_name"]
        print(id, reg, funfam, name)



def print_data():
    records = get_data()
    print_protein_data(records)
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

