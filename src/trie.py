from typing import Tuple
class TrieNode(object):

    def __init__(self, char: str):
        self.char = char
        self.children = []
        # Is it the last character of the word.`
        self.word_finished = False
        # How many times this character appeared in the addition process
        self.counter = 1




def add(root, word: str):

    node = root
    for char in word:
        is_found = False
        # If the root node has no children, then it must be empty
        for child in node.children:
            if child.char == char:
                # We found it, increase the counter by 1 to keep track that another
                # word has it as well
                child.counter += 1
                # And point the node to the child that contains this char
                node = child
                is_found = True
                break
        # We did not find it so add a new chlid
        if not is_found:
            new_node = TrieNode(char)
            node.children.append(new_node)
            # And then point node to the new child
            node = new_node
    # Mark the node as the ending of the sequence
    node.word_finished = True

def find_matching_prefix(root, prefix: str) -> Tuple[bool, int]:
    """
        Check for any matchin prefix 
    """
    node = root
    # If the root node has no children, then return False.
    # Because it means we are trying to search in an empty trie
    if not root.children:
        return False, 0
    for char in prefix:
        char_not_found = True
        # Search through all the children of the present `node`
        for child in node.children:
            if child.char == char:
                # We found the char existing in the child.
                char_not_found = False
                # Assign node as the child containing the char and break
                node = child
                break
        # Return False anyway when we did not find a char.
        if char_not_found:
            return False, 0
    #found and give count
    return True, node.counter


if __name__ == "__main__":
    root = TrieNode('*')
    add(root, 'ethan')
    add(root, 'ethanol')
    add(root, 'ethoa')
    add(root, 'etean')
    add(root, 'eaten')

    print(find_matching_prefix(root, 'eat'))
    print(find_matching_prefix(root, 'ethan'))
    print(find_matching_prefix(root, 'et'))
    print(find_matching_prefix(root, 'ten'))