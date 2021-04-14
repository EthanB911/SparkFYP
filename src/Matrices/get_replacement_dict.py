# Usage: python blosum.py blosum62.txt
#        Then, enter input in "row col" format -- e..g, "s f".
import sys

class InvalidPairException(Exception):
  pass

class Matrix:
  def __init__(self, matrix_filename):
    self._load_matrix(matrix_filename)

  def _load_matrix(self, matrix_filename):
    with open(matrix_filename) as matrix_file:
      matrix = matrix_file.read()
    lines = matrix.strip().split('\n')

    header = lines.pop(0)
    columns = header.split()
    matrix = {}

    for row in lines:
      entries = row.split()
      row_name = entries.pop(0)
      matrix[row_name] = {}

      if len(entries) != len(columns):
        raise Exception('Improper entry number in row')
      for column_name in columns:
        matrix[row_name][column_name] = entries.pop(0)

    self._matrix = matrix

  def lookup_score(self, a, b):
    a = a.upper()
    b = b.upper()

    if a not in self._matrix or b not in self._matrix[a]:
      raise InvalidPairException('[%s, %s]' % (a, b))
    return self._matrix[a][b]


def get_replacement_dict():
    matrix = Matrix('/Users/ethan/Desktop/Spark/src/Matrices/blosum62.txt')
    print(matrix._matrix['A']['G'])
    replacement_dict = {}
    amino_acids = ['A','R', 'N', 'D', 'C', 'Q', 'E', 'G', 'H', 'I', 'L', 'K', 'M', 'F', 'P', 'S', 'T', 'W', 'Y', 'V', 'B', 'Z', 'X', '*']
    for vertical in amino_acids:
        arr = []
        for horizontal in amino_acids:
            # print('['+ vertical+']''['+ horizontal+']')
            if(vertical != horizontal):
                if(int(matrix._matrix[vertical][horizontal]) >= 0):
                    arr.append(horizontal)

        if(len(arr) != 0):
            replacement_dict[vertical]= arr


    print(replacement_dict)
    return replacement_dict

