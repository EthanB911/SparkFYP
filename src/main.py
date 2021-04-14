import mysql.connector
from pandas import DataFrame
from trie import add, TrieNode, find_matching_prefix
from spark import get_spark_session
from pyspark.sql.types import Row, StructType, StructField, StringType
from pyspark.sql import SQLContext
from Bio import SeqIO, AlignIO
from Bio.Align import AlignInfo
from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *
import networkx as nx
import matplotlib.pyplot as plt


# list to dictionary

def Convert(lst):
    res_dct = {lst[i]: lst[i + 1] for i in range(0, len(lst), 2)}
    return res_dct



def PlotGraph(edges, vertices):
    gp = nx.from_pandas_edgelist(edges.toPandas(), 'src', 'dst', create_using=nx.OrderedMultiDiGraph())
    new_vertices = Convert(vertices)
    print(new_vertices)
    G= nx.relabel_nodes(gp, new_vertices, copy=False)
    G.remove_edges_from(nx.selfloop_edges(G))
    nx.draw(G, with_labels=True)
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

    g = GraphFrame(vertices, edges)
    ## Take a look at the DataFrames
    g.vertices.show()
    g.edges.show()
    ## Check the number of edges of each vertex
    g.degrees.show()
    return edges, g

# edges, graph = spark_graph()
# PlotGraph(edges)



# file = SeqIO.parse("/Users/ethan/Downloads/1.10.1870.10/full_alignments/1.10.1870.10-FF-000001.faa", "fasta")

file = AlignIO.read("/Users/ethan/Downloads/1.10.1870.10/full_alignments/1.10.1870.10-FF-000002.faa", "fasta")
for line in file:
    print(line)
summary_align = AlignInfo.SummaryInfo(file)

print(summary_align.gap_consensus())
print(summary_align.dumb_consensus( threshold=0,ambiguous='-', require_multiple=1))




def convert_Trie_To_Table(trie, id):
    global generatedId
    generatedId = id
    verts.append((id, trie.char))
    ver.append(id)
    ver.append(trie.char)
    #root .for each
    for child in trie.children:
        edges.append((id, generatedId + 1, "word"))
        addrowRecord(child, generatedId + 1)
        generatedId += 1




def addrowRecord(node, id):
    global generatedId
    generatedId = id
    verts.append((id, node.char))
    ver.append(id)
    ver.append(node.char)
    if(len(node.children) != 0):
        for child in node.children:
            generatedId += 1
            edges.append((id, generatedId + 1, "word"))
            generatedId = addrowRecord(child, generatedId + 1)
    if(node.word_finished == True):
        generatedId += 1
        verts.append((generatedId + 1, '$'))
        ver.append(generatedId + 1)
        ver.append('$')
        edges.append((id, generatedId + 1, "word"))
    return generatedId

#Example -----
verts = []
ver = []
edges = []
# root = TrieNode("names")
# add(root, "ethan")
# add(root, "brady")
# add(root, "ethanol")
# add(root, "ethenial")
# spark = get_spark_session()
# sqlContext = SQLContext(spark.sparkContext)
#
# convert_Trie_To_Table(root, 0)
# vertices = sqlContext.createDataFrame(verts, ["id", "name"])
# vertices.show()
# edgesspark = sqlContext.createDataFrame(edges, ["src", "dst", "relationship"])
# edgesspark.show()
#
# print(verts)
# print(edges)
# PlotGraph(edgesspark, ver)


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

