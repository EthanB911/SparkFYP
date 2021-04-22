from src.spark import get_spark_session
from pyspark.sql.types import Row, StructType, StructField, StringType
from pyspark.sql import SQLContext
from graphframes import *
import time


def spark_graph():
    spark = get_spark_session()
    spark.sparkContext.setCheckpointDir('dir')
    # spark.sparkContext.addPyFile('/Users/ethan/Downloads/graphframes-0.7.0-spark2.4-s_2.11.jar')
    sqlContext = SQLContext(spark.sparkContext)
    vertices = sqlContext.createDataFrame([
        ("a", "World", 34),
        ("b", "Europe", 36),
        ("c", "Malta", 30),
        ("d", "Rabat", 29),
        ("e", "Home", 32),
        ("f", "Bedroom", 36),
        ("g", "Bed", 60)], ["id", "name", "age"])
    edges = sqlContext.createDataFrame([
        ("a", "b", "links"),
        ("b", "c", "links"),
        ("c", "d", "links"),
        ("d", "e", "links"),
        ("e", "f", "links"),
        ("f", "g", "links"),
    ], ["src", "dst", "relationship"])

    graph = GraphFrame(vertices, edges)
    ## Take a look at the DataFrames
    ## Check the number of edges of each vertex
    start_time = time.time()
    # paths = graph.bfs("name = 'Esther'", "age < 32")
    # paths.show()

    print("motifs")
    # Search for pairs of vertices with edges in both directions between them.
    motifs = graph.find("(a)-[e]->(d)")
    motifs.show()
    # whats this ?
    # result = graph.connectedComponents()
    # result.show()

    print("--- %s seconds ---" % (time.time() - start_time))
    return graph

spark_graph()

