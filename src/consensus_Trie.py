from spark import get_spark_session
from pyspark.sql.types import Row, StructType, StructField, StringType
from trie import add, TrieNode, find_matching_prefix
from Matrices.get_replacement_dict import get_replacement_dict
from Bio import SeqIO, AlignIO
import re

from Bio.Align import AlignInfo
import os



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




all_fasta_alignments_to_trie()






