from spark import get_spark_session
from pyspark.sql.types import Row, StructType, StructField, StringType
from trie import add, TrieNode, find_matching_prefix
from Matrices.get_replacement_dict import get_replacement_dict
import pandas as pd
from Tries.suffix_trie import SuffixTree
from Bio import SeqIO, AlignIO
import time
from pyspark.sql import SQLContext
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
    arr = os.listdir('/Users/ethan/Downloads/1.10.1870.10/full_alignments/')
    print(arr)
    root = TrieNode('1.10.1870.10')
    # substitutions = {'R': 'N', 'L': 'Q'}
    substitutions = get_replacement_dict()
    # for key, value in substitutions.items():
    #     print(key, '->', value)
    #     for val in value:
    #         print(key, '->', val)
    for path in arr:
        file = AlignIO.read("/Users/ethan/Downloads/1.10.1870.10/full_alignments/"+path, "fasta")
        print('path:' + path)

        summary_align = AlignInfo.SummaryInfo(file)

        consenus = str(summary_align.dumb_consensus(threshold=0, ambiguous='-', require_multiple=1))
        if(allCharactersSame(consenus) != True):
            add(root, consenus)
            for key, value in substitutions.items():
                for val in value:

                    print(key, '->', val)
                    subst = consenus.replace(key, val)
                    print(subst)
                    add(root, subst)

    return root

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
    spark = get_spark_session()
    sqlContext = SQLContext(spark.sparkContext)
    verts = sqlContext.createDataFrame(v, ["id", "name", "superfamily"])
    edgs = sqlContext.createDataFrame(e, ["src", "dst"])
    return GraphFrame(verts, edgs)

trie = SuffixTree("xabxac")
# trie.add("xabxac")

trie.add("banana")
trie.add("xadina")
trie.add("bbababab")
trie.add("malcom")
trie.add("michael")
trie.add("aaiert")
trie.add("depbajkd")
trie.add("dembabkeumdsk")
trie.add("peterporsche")
trie.add("ferrarialfaromeo")
# trie.add("mercedesprojecgtone")
trie.add("bradley")
trie.add("wizascooot")
trie.add("sheesh")
trie.add("jadebriffa")
trie.add("jpseph")
trie.add("bonello")
# trie.add("forceininasauberamggts")
trie.add("ethan")
trie.add("ethen")

# trie.add("banana")

vertices, edges = trie.proper_to_graphframe(0)
g = verts_edges_to_graphframe(vertices, edges)
g.cache()


# paths = g.bfs("name = 'root'","name = '$'")
# paths.show()
# rt = g.vertices.filter("name='root'").select("id").collect()[0]["id"]
# print(rt)

# motif = g.find("(x)-[e1]->(x1)")\
#         .filter("x.name='root'")\
#         .filter("x1.name='n'")\
#         .select('e1.src', 'e1.dst')
# motif.show()
#
# start_time = time.time()
# print("Destination")
#
# print()
# print("--- %s seconds ---" % (time.time() - start_time))
# motif.foreach(lambda row:
#     print(row['src'])
#     )
# t1 = time.time()
# subg = g.vertices.filter("name='e'")
# print(subg)
#
# print("--- %s seconds ---" % (time.time() - t1))

# motif = g.find("(x)-[e1]->(x1)")\
#         .filter("x.name='root'")\
#         .filter("x1.name='e'")\
#         .select('e1.src', 'e1.dst')
#
#
# t2 = time.time()
# print(motif.count())
# print("--- %s seconds ---" % (time.time() - t2))


# edges = g.edges.filter("src="+str(rt)).show()

def search_graph_from_root(graph, next):
    start_time = time.time()
    motif = graph.find("(x)-[e1]->(x1)") \
        .filter("x.name='root'")\
        .filter("x1.name='" + next+"'") \
        .select('e1.src', 'e1.dst')

    print("--- %s seconds ---" % (time.time() - start_time))
    return motif



def search_graph_from_id(graph,current,  next):
    start_time = time.time()
    motif = graph.find("(x)-[e1]->(x1)") \
        .filter("e1.src=" + str(current))\
        .filter("x1.name='" + next+"'") \
        .select('e1.src', 'e1.dst')

    print("--- %s seconds ---" % (time.time() - start_time))
    return motif

def alternative_search(graph):
    start_time = time.time()



    print("--- %s seconds ---" % (time.time() - start_time))
# search_graph_from_id(g, 0, "d")

# trie = all_fasta_alignments_to_trie()
# g = trie_to_graphframe(trie)
#search algorithm?

list_of_superfamilies = []
search_term = "ethna"
current_search_term = "ethna"
current_matching_term = ""
matching_motif_patterns = []
score_dictionary = {}
print(len(current_matching_term))

#steps

#1. Arrive at superfamily using subgraph routine
#probably a while loop(while word is not null or something
s = time.time()
current_node = "root"
while len(current_search_term) > 0:
    print("Current search term: " + current_search_term)
    first = current_search_term[0]

    if(len(current_matching_term) == 0):
        result =  search_graph_from_root(g,first)
        #do checking here
        ss = time.time()
        if(result.count() != 0):
            print("--- %s seconds ---" % (time.time() - ss))
            current_matching_term = first
            current_search_term = current_search_term[1:]
            current_node = result.select("dst").collect()[0]["dst"]

    else:
        #here we have a matching path in hand and are checking if this motif is larger
        result = search_graph_from_id(g, current_node, first)
        #do checking here
        ss = time.time()
        if(result.count() != 0):
            print("--- %s seconds ---" % (time.time() - ss))
            current_matching_term += first
            current_search_term = current_search_term[1:]
            current_node = result.select("dst").collect()[0]["dst"]
        else:
            print("--- %s seconds ---" % (time.time() - ss))
            #motif stops here
            matching_motif_patterns.append(current_matching_term)
            current_matching_term =""
            current_node="root"

    print("Current search term after check: " + current_search_term)

#if current_mathing is not empty add it!
if(len(current_matching_term) != 0 ):
    matching_motif_patterns.append(current_matching_term)


print("Mathed patterns: ")
print(matching_motif_patterns)
print("--- %s seconds ---" % (time.time() - s))

#2. Get first character and check if next node matches
#find all occurances where src is the root of the superfamily and dest matches word.
#for each word check if next word matches until word is finished or word does not match
#When checking for next node always check all children, therefore all nodes where src matches current node, to see all children

#Repeat for next character , use some sort of pointer to know which is the next word







