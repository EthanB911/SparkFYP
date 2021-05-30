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
    # substitutions = {'R': 'N', 'L': 'Q'}
    substitutions = get_replacement_dict()

    trie = SuffixTree("1.10.1870.10")
    fams  = os.listdir('/Users/ethan/Downloads/1.10.1870.10/rest_alignments/')
    for fun_Fam in fams:
        file = AlignIO.read("/Users/ethan/Downloads/1.10.1870.10/rest_alignments/" + fun_Fam , "fasta")
        print('path:' + fun_Fam)

        summary_align = AlignInfo.SummaryInfo(file)
        fun_Fam = fun_Fam.split('-')[2].split('.')[0]
        consenus = str(summary_align.dumb_consensus(threshold=0, ambiguous='-', require_multiple=1))
        if(allCharactersSame(consenus) != True):
            trie.add(consenus,'1.10.1870.10', fun_Fam)
            for key, value in substitutions.items():
                for val in value:

                    print(key, '->', val)
                    subst = consenus.replace(key, val)
                    print(subst)
                    trie.add(subst,'1.10.1870.10', fun_Fam)
    return trie


def allCharactersSame(s):
    n = len(s)
    for i in range(1, n):
        if s[i] != s[0]:
            return False

    return True

def all_fasta_alignments_to_trie():



    # substitutions = {'R': 'N', 'L': 'Q'}
    substitutions = get_replacement_dict()
    # for key, value in substitutions.items():
    #     print(key, '->', value)
    #     for val in value:
    #         print(key, '->', val)
    arr = os.listdir('/Users/ethan/Downloads/funfams/')
    print(arr)
    trieList = []
    for super_fam in arr:
        trie = SuffixTree("1.10.1870.10")
        fams  = os.listdir('/Users/ethan/Downloads/funfams/' + super_fam + '/full_alignments/')
        for fun_Fam in fams:
            file = AlignIO.read("/Users/ethan/Downloads/funfams/" + super_fam + "/full_alignments/" + fun_Fam , "fasta")
            print('path:' + fun_Fam)

            summary_align = AlignInfo.SummaryInfo(file)
            fun_Fam = fun_Fam.split('-')[2].split('.')[0]
            consenus = str(summary_align.dumb_consensus(threshold=0, ambiguous='-', require_multiple=1))
            if(allCharactersSame(consenus) != True):
                trie.add(consenus,'1.10.1870.10', fun_Fam)
                for key, value in substitutions.items():
                    for val in value:

                        print(key, '->', val)
                        subst = consenus.replace(key, val)
                        print(subst)
                        trie.add(subst,'1.10.1870.10', fun_Fam)
        trieList.append(trie)
    print(trieList)
    return trieList

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
    print('started')
    verts = sqlContext.createDataFrame(v, ["id", "name", "superfamily"])
    print('vertices done')
    edgs = sqlContext.createDataFrame(e, ["src", "dst", "family"])
    print("--- %s seconds ---" % (time.time() - start_time))
    return GraphFrame(verts, edgs)

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


#load all super families
# tries = all_fasta_alignments_to_trie()
#
# id = 0
# total_vertices = []
# total_edges = []
# for trie in tries:
#     vertices,edges, id = trie.proper_to_graphframe(id)
#     total_vertices += vertices
#     total_edges += edges
# # # #
# # vertices, edges = trie.proper_to_graphframe(0)
# g = verts_edges_to_graphframe(total_vertices, total_edges)
# save_graphframe(g)

#load 1 superfamily
trie = fasta_to_trie()
vertices, edges, id = trie.proper_to_graphframe(0)
g = verts_edges_to_graphframe(vertices, edges)
save_graphframe(g)


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


def query_creator(sources):
    query = "e1.src='" + str(sources[0]['dst']) + "'"
    for src in sources[1:]:
        query += " or e1.src='" + str(src['dst']) + "'"
    return query


def get_dollar_families(graph, src):
    print('search 1')
    start_time = time.time()
    x = graph.find("(x)-[e1]->(x1)") \
        .filter("x.id='" + str(src) + "'") \
        .filter("x1.name='$'") \
        .select('e1.family').collect()
    print("--- %s seconds ---" % (time.time() - start_time))
    return x


def get_dollar_families_from_children(graph, query_sources):
    print('search 2')
    start_time = time.time()
    x = graph.find("(x)-[e1]->(x1)") \
        .filter(query_sources) \
        .filter("x1.name='$'") \
        .select('e1.family').take(1)
    print("--- %s seconds ---" % (time.time() - start_time))
    return x


def search_graph_from_root(graph, next_term):
    print('search 3')
    start_time = time.time()
    x = graph.find("(x)-[e1]->(x1)") \
        .filter("x.name='root'") \
        .filter("x1.name='" + next_term + "'") \
        .select('e1.src', 'e1.dst', 'e1.family').collect()
    print("--- %s seconds ---" % (time.time() - start_time))
    return x


def search_from_multiple_src(graph, query_sources, next_term):
    print('search 4')
    print(query_sources)
    start_time = time.time()
    x = graph.find("(x)-[e1]->(x1)") \
        .filter(query_sources) \
        .filter("x1.name='" + next_term + "' or x1.name = '$'") \
        .select('e1.src', 'e1.dst', 'e1.family').collect()
    print("--- %s seconds ---" % (time.time() - start_time))
    return x


def search_src_to_term(graph, current_node, next_term):
    print('search 5')
    print(current_node)
    print(next_term)
    start_time = time.time()
    x = graph.find("(x)-[e1]->(x1)") \
        .filter("e1.src=" + str(current_node)) \
        .filter("x1.name='" + next_term + "' or x1.name = '$'") \
        .select('e1.src', 'e1.dst', 'e1.family').collect()
    print("--- %s seconds ---" % (time.time() - start_time))
    return x


def get_children_from_multiple_sources(graph, query_sources):
    print('search 6')
    print(query_sources)
    start_time = time.time()
    children = graph.find("(x)-[e1]->(x1)") \
        .filter(query_sources) \
        .filter("x1.name != '$'") \
        .select('e1.dst').collect()
    # still need to implement well..
    # definetly need to remove where dest = $
    print("--- %s seconds ---" % (time.time() - start_time))
    return children


def get_children_from_single_source(graph, source_node):
    print('search 7')
    print(source_node)
    start_time = time.time()
    children = graph.find("(x)-[e1]->(x1)") \
        .filter("e1.src=" + str(source_node)) \
        .filter("x1.name != '$'") \
        .select('e1.dst').collect()
    # still need to implement well..
    print("--- %s seconds ---" % (time.time() - start_time))
    return children


def get_next_only_from_source(graph, source_node, next_term):
    print('search 8')
    print(source_node)
    start_time = time.time()
    x = graph.find("(x)-[e1]->(x1)") \
        .filter("e1.src=" + str(source_node)) \
        .filter("x1.name='" + next_term + "'") \
        .select('e1.src', 'e1.dst', 'e1.family').collect()
    print("--- %s seconds ---" % (time.time() - start_time))
    return x



#
# #--- start algo here
# import time
#
# search_term = "PDANPLTWKELLCDLVGIS-PSSKCDVLKEAVFKKLEGDNTQLEAVEWLGLLG-DEQVPRAESLVDALSKHLAMKLS"
# current_search_term = search_term
# matching_term = ""
# prev_match = False  # previous match
# new_match = True  # set to true when we want to start searching from root
# current_node = 0  # pointer to current node
# children_nodes = []
# max_mismatches = 9  # if this value is exceeded search restarts from root
# current_mismatches = 0
# motifs_found = []
# current_matched = 0
# max_matches = 12
# current_families = []
# s = time.time()
# # start algorithm here.
# while len(current_search_term):
#     start_time = time.time()
#     first = current_search_term[0]
#
#     # check if we have overtook max_mismatches or completed a motif.
#     if (current_mismatches > max_mismatches or new_match):
#         # if there were a lot of matches ensure to save it before to disregard it
#         matching_term = ""
#         new_match = False
#         current_mismatches = 0
#         current_matched = 0
#         prev_match = False
#         # search from root if so to find new motif.
#         result = search_graph_from_root(g, first)
#         res_count = len(result)
#
#         if (res_count != 0):
#             # we should have next term here
#             # append it to matching_term
#             prev_match = True
#             matching_term += first
#             current_node = result[0]["dst"]
#             current_families = result[0]["family"]
#             #     prev_match = True
#             current_search_term = current_search_term[1:]
#             current_matched += 1
#         else:
#             # no match found
#             # we need to get all children of current node for next search part
#             prev_match = False
#             matching_term += '-'
#             current_mismatches += 1
#             current_search_term = current_search_term[1:]
#             # get children node of current node.
#             children_nodes = get_children_from_single_source(g, current_node)
#
#     elif (prev_match):
#         result = search_src_to_term(g, current_node, first)
#         res_count = len(result)
#
#         if (res_count != 0):
#             # match found
#             if (res_count == 2):
#                 # there is next term and also a $
#                 # get $ for its families.
#                 families = get_dollar_families(g, current_node)
#                 # keep going as if we found next only?
#                 matching_term += first
#                 motifs_found.append({'motif': matching_term, 'families': families[0]["family"]})
#
#                 next_term_node = get_next_only_from_source(g, current_node, first)
#                 current_node = next_term_node[0]["dst"]
#                 current_families = next_term_node[0]["family"]
#                 #       prev_match = True
#                 current_search_term = current_search_term[1:]
#                 current_matched += 1
#             else:
#                 # there is only 1 term, so get it and see what it is.
#                 dst = result[0]["dst"]
#                 if (dst == '$'):
#                     # motif ends here
#                     # get families here
#                     motifs_found.append({'motif': matching_term, 'families': result[0]["family"]})
#                     new_match = True
#
#
#                 else:
#                     # we should have next term here
#                     # append it to matching_term
#                     matching_term += first
#                     current_node = dst
#                     #         prev_match = True
#                     current_families = result[0]["family"]
#                     current_search_term = current_search_term[1:]
#                     current_matched += 1
#         else:
#             # no match found
#             # we need to get all children of current node for next search part
#             prev_match = False
#             matching_term += '-'
#             current_mismatches += 1
#             current_search_term = current_search_term[1:]
#             # get children node of current node.
#             children_nodes = get_children_from_single_source(g, current_node)
#     else:
#         # previous search didn't match
#         # here we should have a list of source nodes.
#         # search from all source nodes for next term
#         result = search_from_multiple_src(g, query_creator(children_nodes), first)
#         res_count = len(result)
#         if (res_count != 0):
#             # match found
#             prev_match = True
#             if (res_count >= 2):
#                 # there is next term, possibly more than 1 and possibly a $ sign
#                 # possible motif found, add it but keep going as if we didn't finish it.
#                 #families = get_dollar_families_from_children(g, query_creator(children_nodes))
#                 matching_term += first
#                 # keep going as if we found next only?
#                 #motifs_found.append({'motif': matching_term, 'families': families[0]["family"]})
#
#                 current_node = result[0]["dst"]
#                 #       prev_match = True
#                 current_search_term = current_search_term[1:]
#                 current_matched += 1
#             else:
#                 # there is only 1 term, so get it and see what it is.
#                 dst = result[0]["dst"]
#                 if (dst == '$'):
#                     # motif ends here
#                     # get families here
#                     motifs_found.append({'motif': matching_term, 'families': result[0]["family"]})
#                     new_match = True
#
#                 else:
#                     # we should have next term here
#                     # append it to matching_term
#                     matching_term += first
#                     current_node = dst
#                     prev_match = True
#                     current_families = result[0]["family"]
#                     current_search_term = current_search_term[1:]
#                     current_matched += 1
#         else:
#             # no match found
#             # we need to get all children of current node for next search part
#             prev_match = False
#             matching_term += '-'
#             current_mismatches += 1
#             current_search_term = current_search_term[1:]
#             # get children node of current node.
#             children_nodes = get_children_from_multiple_sources(g, query_creator(children_nodes))
#
#     # after search is computed always check if we have matched multiple terms to stop traversing current branch
#     # so compare current matched and get current_node families or just keep track of families being visited..
#     # if case holds set new_match to true
#     if (current_matched >= max_matches):
#         motifs_found.append({'motif': matching_term, 'families': current_families})
#         new_match = True
#     print(current_matched)
#     print(current_search_term)
#     print("--- %s seconds ---" % (time.time() - start_time))
# print(motifs_found)
# print("--- %s seconds ---" % (time.time() - s))
# # if out of while loop and current_match not empty see if we can add it ....