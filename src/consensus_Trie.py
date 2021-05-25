from spark import get_spark_session
from pyspark.sql.types import Row, StructType, StructField, StringType
from trie import add, TrieNode, find_matching_prefix
from Matrices.get_replacement_dict import get_replacement_dict
import pandas as pd
from Tries.suffix_trie import SuffixTree
from Bio import SeqIO, AlignIO
import time
from pyspark.sql import SQLContext
import pyspark
import re
from Bio.Align import AlignInfo
import os
from graphframes import *

import os
print(os.__file__)

def fasta_to_trie():
    file = AlignIO.read("/Users/ethan/Downloads/1.10.1870.10/full_alignments/1.10.1870.10-FF-000002.faa", "fasta")

    summary_align = AlignInfo.SummaryInfo(file)
    substitutions = {'R': 'N', 'L': 'Q'}

    #initialize trie
    root = TrieNode('1.10.1870.10-FF-000002')
    consenus = str(summary_align.dumb_consensus(threshold=0, ambiguous='-', require_multiple=1))
    add(root, consenus)
    for key, value in substitutions.items():
        print(key, '->', value)
        subst = consenus.replace(key, value)
        print(subst)
        add(root, subst)


def allCharactersSame(s):
    n = len(s)
    for i in range(1, n):
        if s[i] != s[0]:
            return False

    return True

def all_fasta_alignments_to_trie():
    arr = os.listdir('/Users/ethan/Downloads/1.10.1870.10/full/')
    print(arr)
    trie = SuffixTree("1.10.1870.10")
    # substitutions = {'R': 'N', 'L': 'Q'}
    substitutions = get_replacement_dict()
    # for key, value in substitutions.items():
    #     print(key, '->', value)
    #     for val in value:
    #         print(key, '->', val)
    for path in arr:
        file = AlignIO.read("/Users/ethan/Downloads/1.10.1870.10/full/"+path, "fasta")
        print('path:' + path)

        summary_align = AlignInfo.SummaryInfo(file)

        consenus = str(summary_align.dumb_consensus(threshold=0, ambiguous='-', require_multiple=1))
        if(allCharactersSame(consenus) != True):
            trie.add(consenus, path)
            for key, value in substitutions.items():
                for val in value:

                    print(key, '->', val)
                    subst = consenus.replace(key, val)
                    print(subst)
                    trie.add(subst, path)

    return trie

# def trie_to_graphframe(root):
#     spark = get_spark_session()
#     sqlContext = SQLContext(spark.sparkContext)
#
#     convert_Trie_To_Table(root, 0)
#     vertices = sqlContext.createDataFrame(verts, ["id", "name", "superfamily"])
#     vertices.show()
#     edgesspark = sqlContext.createDataFrame(edges, ["src", "dst", "relationship"])
#     graph = GraphFrame(vertices, edgesspark)
#     return graph

def verts_edges_to_graphframe(v, e):
    start_time = time.time()
    spark = get_spark_session()
    sqlContext = SQLContext(spark.sparkContext)
    verts = sqlContext.createDataFrame(v, ["id", "name", "superfamily"])
    edgs = sqlContext.createDataFrame(e, ["src", "dst", "family"])
    print("--- %s seconds ---" % (time.time() - start_time))
    return GraphFrame(verts, edgs)

trie = SuffixTree("xabxac")
# trie.add("ethan", "bri")
#
# trie.add("banana", "a1")

trie = all_fasta_alignments_to_trie()
trie.visualize()
# #
# vertices, edges = trie.proper_to_graphframe(0)
# g = verts_edges_to_graphframe(vertices, edges)
# g.cache()
# motif = g.find("(x)-[e1]->(x1)") \
#         .filter("x1.name = '$'") \
#         .select('e1.src', 'e1.dst', 'e1.family')
# motif.show()

def save_graphframe(graph):
    graph.vertices.write.parquet("/Users/ethan/Downloads/1.10.1870.10/graphframe/vertices")
    graph.edges.write.parquet("/Users/ethan/Downloads/1.10.1870.10/graphframe/edges")
    print('saved')

def load_graph():
    start_time = time.time()
    spark = get_spark_session()
    sqlContext = SQLContext(spark.sparkContext)
    verts = sqlContext.read.parquet("/Users/ethan/Downloads/1.10.1870.10/graphframe/vertices")
    edgs = sqlContext.read.parquet("/Users/ethan/Downloads/1.10.1870.10/graphframe/edges")
    print("--- %s seconds ---" % (time.time() - start_time))
    return GraphFrame(verts, edgs)

# g= load_graph()
# g.cache()


# trie.add("xadina", "e1")
# trie.add("bbababab", "e1")
# trie.add("malcom", "e1")
# trie.add("michael", "e1")
# trie.add("aaiert", "e1")
# trie.add("depbajkd", "e1")
# trie.add("dembabkeumdsk", "e1")
# trie.add("peterporsche", "e1")
# trie.add("ferrarialfaromeo", "e1")
# # trie.add("mercedesprojecgtone", "e1")
# trie.add("bradley", "e1")
# trie.add("wizascooot", "e1")
# trie.add("sheesh", "e1")
# trie.add("jadebriffa", "e1")
# trie.add("jpseph", "e1")
# trie.add("bonello", "e1")
# # trie.add("forceininasauberamggts", "e1")
# trie.add("ethan", "e1")
#---------------------------_----------------------
# trie.add("banana", "e1")
# trie.add("banuna", "e1")
# trie.add("binana", "e2")
# vertices, edges = trie.proper_to_graphframe(0)
# g = verts_edges_to_graphframe(vertices, edges)
# g.cache()
# motif = g.find("(x)-[e1]->(x1)") \
#         .filter("x.name='root'") \
#         .filter("x1.name='b'") \
#         .select('e1.src', 'e1.dst', 'e1.family')
# motif.show()
# trie.add("banana")

# g.persist(storageLevel=pyspark.StorageLevel.MEMORY_ONLY)
# edges = g.edges.filter("src="+str(rt)).show()

def search_graph_from_root(graph, next):
    start_time = time.time()
    motif = graph.find("(x)-[e1]->(x1)") \
        .filter("x.name='root'")\
        .filter("x1.name='" + next+"'") \
        .select('e1.src', 'e1.dst', 'e1.family')

    print("--- %s seconds ---" % (time.time() - start_time))
    return motif



def search_graph_from_id(graph,current,  next):
    start_time = time.time()
    motif = graph.find("(x)-[e1]->(x1)") \
        .filter("e1.src=" + str(current)) \
        .filter("x1.name='" + next + "'") \
        .select('e1.src', 'e1.dst', 'e1.family')
    # .filter("x1.name='" + next+"' or x1.name = '$'") \

    print("--- %s seconds ---" % (time.time() - start_time))
    return motif

def alternative_search(graph):
    start_time = time.time()



    print("--- %s seconds ---" % (time.time() - start_time))
# search_graph_from_id(g, 0, "d")

# trie = all_fasta_alignments_to_trie()
# g = trie.to_graphframe(trie)
#search algorithm?
#
# list_of_superfamilies = []
# search_term = "GAYKALNGFVKLGLINRGAFPALRPDANPLTWKELLCDLVGIS-PSSKCDVLKEAVFKKL"
# current_search_term = "GYAKALNGFVKLGLINRGAFPALRPDANPLTWKELLCDLVGIS-PSSKCDVLKEAVFKKL"
# current_matching_term = ""
# matching_motif_patterns = []
# score_dictionary = {}
# print(len(current_matching_term))
#
# #steps
#
# #1. Arrive at superfamily using subgraph routine
# #probably a while loop(while word is not null or something
# s = time.time()
# current_node = "root"
# while len(current_search_term) > 0:
#     print("Current search term: " + current_search_term)
#     first = current_search_term[0]
#
#     if(len(current_matching_term) == 0):
#         result =  search_graph_from_root(g,first)
#         #do checking here
#         ss = time.time()
#         if(result.count() != 0):
#             print("After count")
#             print("--- %s seconds ---" % (time.time() - ss))
#             current_matching_term = first
#             current_search_term = current_search_term[1:]
#             current_node = result.select("dst").collect()[0]["dst"]
#
#     else:
#         #here we have a matching path in hand and are checking if this motif is larger
#         result = search_graph_from_id(g, current_node, first)
#         #do checking here
#         ss = time.time()
#         # if result found or # found
#         if(result.count() != 0):
#             print("After count")
#             print("--- %s seconds ---" % (time.time() - ss))
#             current_matching_term += first
#             current_search_term = current_search_term[1:]
#             current_node = result.select("dst").collect()[0]["dst"]
#         else:
#             print("After count")
#             print("--- %s seconds ---" % (time.time() - ss))
#             #motif stops here
#             # matching_motif_patterns.append((current_matching_term,result.select("family").collect()[0]["family"]))
#             matching_motif_patterns.append(current_matching_term)
#             current_matching_term =""
#             current_node= "root"
#
#     print("Current search term after check: " + current_search_term)
#
# #if current_mathing is not empty add it!
# if(len(current_matching_term) != 0 ):
#     matching_motif_patterns.append(current_matching_term)
#
#
# print("Mathed patterns: ")
# print(matching_motif_patterns)
# print("--- %s seconds ---" % (time.time() - s))

#2. Get first character and check if next node matches
#find all occurances where src is the root of the superfamily and dest matches word.
#for each word check if next word matches until word is finished or word does not match
#When checking for next node always check all children, therefore all nodes where src matches current node, to see all children

#Repeat for next character , use some sort of pointer to know which is the next word







