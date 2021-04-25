from spark import get_spark_session
from pyspark.sql.types import Row, StructType, StructField, StringType
from trie import add, TrieNode, find_matching_prefix
from Matrices.get_replacement_dict import get_replacement_dict
from Bio import SeqIO, AlignIO
from pyspark.sql import SQLContext
import re
from Bio.Align import AlignInfo
import os
from graphframes import *



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
verts = []
ver = []
edges = []
def trie_to_graphframe(root):
    spark = get_spark_session()
    sqlContext = SQLContext(spark.sparkContext)

    convert_Trie_To_Table(root, 0)
    vertices = sqlContext.createDataFrame(verts, ["id", "name", "superfamily"])
    vertices.show()
    edgesspark = sqlContext.createDataFrame(edges, ["src", "dst", "relationship"])
    graph = GraphFrame(vertices, edgesspark)
    return graph



def addrowRecord(node, id):
    global generatedId
    generatedId = id
    verts.append((id, node.char, "1.10.1870.10"))
    ver.append(id)
    ver.append(node.char)
    if (len(node.children) != 0):
        for child in node.children:
            generatedId += 1
            edges.append((id, generatedId + 1, "000002"))
            generatedId = addrowRecord(child, generatedId + 1)
    if (node.word_finished == True):
        generatedId += 1
        verts.append((generatedId + 1, '$', "1.10.1870.10"))
        ver.append(generatedId + 1)
        ver.append('$')
        edges.append((id, generatedId + 1, "1.10.1870.10"))
    return generatedId

def convert_Trie_To_Table(trie, id):
    global generatedId
    generatedId = id
    verts.append((id, trie.char, "1.10.1870.10"))
    ver.append(id)
    ver.append(trie.char)
    # root .for each
    for child in trie.children:
        edges.append((id, generatedId + 1, "000002"))
        addrowRecord(child, generatedId + 1)
        generatedId += 1


trie = all_fasta_alignments_to_trie()
g = trie_to_graphframe(trie)
#search algorithm?

list_of_superfamilies = []
search_term = ""
current_search_term = ""
matching_motif_patterns = []
score_dictionary = {}


#steps

#1. Arrive at superfamily

#2. Get first character and check if next node matches
#When checking for next node always check all children, therefore all nodes where src matches current node, to see all children

#3. If found set this node as current node and move to next character in string
#4. If not found i. move to next string in search_term, therefore now attempting to search for a new 'motif'
# ii) Shall I revesit new path in string on continue along current traversed path. This will completely eliminate the other paths in the sub trie of the super family.









