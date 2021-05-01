from spark import get_spark_session
from pyspark.sql.types import Row, StructType, StructField, StringType
from trie import add, TrieNode, find_matching_prefix
from Matrices.get_replacement_dict import get_replacement_dict
from Tries.suffix_trie import SuffixTree
from Bio import SeqIO, AlignIO
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
trie.add("xabxac")

vertices, edges = trie.proper_to_graphframe(0)
g = verts_edges_to_graphframe(vertices, edges)

g.vertices.show()
g.edges.show()



# trie = all_fasta_alignments_to_trie()
# g = trie_to_graphframe(trie)
#search algorithm?

list_of_superfamilies = []
search_term = "ethan"
current_search_term = "than"
matching_motif_patterns = []
score_dictionary = {}


#steps

#1. Arrive at superfamily using subgraph routine
#probably a while loop(while word is not null or something
#2. Get first character and check if next node matches
#find all occurances where src is the root of the superfamily and dest matches word.
#for each word check if next word matches until word is finished or word does not match
#When checking for next node always check all children, therefore all nodes where src matches current node, to see all children

#Repeat for next character , use some sort of pointer to know which is the next word







