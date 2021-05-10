class Node:
    def __init__(self, sub="", children=None, family=""):
        self.sub = sub
        self.ch = children or []
        self.family = family


class SuffixTree:
    def __init__(self, str):
        self.nodes = [Node()]
        self.name = str


    def add(self,str, family):
        str+='$'
        for i in range(len(str)):
            self.addSuffix(str[i:], family)

    def addSuffix(self, suf, family):
        n = 0
        i = 0
        while i < len(suf):
            b = suf[i]
            x2 = 0
            while True:
                children = self.nodes[n].ch
                if x2 == len(children):
                    # no matching child, remainder of suf becomes new node
                    n2 = len(self.nodes)
                    self.nodes.append(Node(suf[i:], [], family))
                    if ('$' in suf[i:]):
                        print(suf[i:])
                    self.nodes[n].ch.append(n2)
                    return
                n2 = children[x2]
                if self.nodes[n2].sub[0] == b:
                    break
                x2 = x2 + 1

            # find prefix of remaining suffix in common with child
            sub2 = self.nodes[n2].sub
            j = 0
            while j < len(sub2):
                if suf[i + j] != sub2[j]:
                    # split n2
                    n3 = n2
                    # new node for the part in common
                    n2 = len(self.nodes)
                    self.nodes.append(Node(sub2[:j], [n3]))
                    self.nodes[n3].sub = sub2[j:]  # old node loses the part in common
                    if('$' in sub2[j:]):
                        print(sub2[j:])
                    self.nodes[n].ch[x2] = n2
                    break  # continue down the tree
                j = j + 1
            i = i + j  # advance past part in common
            n = n2  # continue down the tree

    def visualize(self):
        self.count = 0
        if len(self.nodes) == 0:
            print("<empty>")
            return
        # for w in range(len(self.nodes)):
        #     print(self.nodes[w].sub)
        def f(n, pre):
            children = self.nodes[n].ch
            if len(children) == 0:
                print("-- ", self.nodes[n].sub + " " +  self.nodes[n].family)
                self.count += 1
                return
            print("+-", self.nodes[n].sub)

            for c in children[:-1]:
                print(pre, "+- ", end='')
                f(c, pre + " | ")

            print(pre, "+- ", end='')
            f(children[-1], pre + "  ")

        f(0, "")
        print(self.count)

    def to_graphframe(self, id):
        #this method saves a word as a node rather than a letter per node...
        verts = []
        edges = []
        global generatedId
        generatedId = id
        self.count = 0
        if len(self.nodes) == 0:
            print("<empty>")
            return

        def f(n, pre, id, parent):
            global generatedId
            generatedId = id
            children = self.nodes[n].ch

            if len(children) == 0:
                print("-- ", self.nodes[n].sub, id)
                verts.append((generatedId, self.nodes[n].sub, "1.10.1870.10"))

                self.count += 1
                return
            print("+-", self.nodes[n].sub, id)
            if self.nodes[n].sub != '':
                verts.append((generatedId, self.nodes[n].sub, "1.10.1870.10"))
            else:
                verts.append((generatedId, "root", "1.10.1870.10"))
            for c in children[:-1]:
                print(pre, "+- ", id,  end='')
                edges.append((id, generatedId + 1))
                f(c, pre + " | ", generatedId + 1, self.nodes[n].sub)

            print(pre, "+- ", id, end='')
            edges.append((id, generatedId + 1))
            f(children[-1], pre + "  ", generatedId + 1, self.nodes[n].sub)

        f(0, "", generatedId, "null")
        print(self.count)
        print(verts)
        print(edges)
        return verts, edges

    def proper_to_graphframe(self, id):
        # this method saves a word as a node rather than a letter per node...
        verts = []
        edges = []
        global generatedId
        generatedId = id
        self.count = 0
        if len(self.nodes) == 0:
            print("<empty>")
            return

        def f(n, pre, id, parent):
            global generatedId
            generatedId = id
            children = self.nodes[n].ch

            if len(children) == 0:
                print("-- ", self.nodes[n].sub, id)

                if(len(self.nodes[n].sub) > 1):
                    id = recursive_word_vertice_add(generatedId,self.nodes[n].sub, "1.10.1870.10",  self.nodes[n].family )
                    generatedId = id
                else:
                    verts.append((generatedId, self.nodes[n].sub, "1.10.1870.10"))

                self.count += 1
                return
            print("+-", self.nodes[n].sub, id)
            if self.nodes[n].sub != '':
                if(len(self.nodes[n].sub) > 1):
                    id = recursive_word_vertice_add(generatedId,self.nodes[n].sub, "1.10.1870.10",  self.nodes[n].family)
                    generatedId = id
                else:
                    verts.append((generatedId, self.nodes[n].sub, "1.10.1870.10"))
            else:
                verts.append((generatedId, "root", "1.10.1870.10"))

            for c in children[:-1]:
                print(pre, "+- ", id, end='')
                edges.append((id, generatedId + 1,  self.nodes[n].family))
                f(c, pre + " | ", generatedId + 1, self.nodes[n].sub)

            print(pre, "+- ", id, end='')
            edges.append((id, generatedId + 1,  self.nodes[n].family))
            f(children[-1], pre + "  ", generatedId + 1, self.nodes[n].sub)
            return id

        def recursive_word_vertice_add(id, word, superfam, family):
            verts.append((id, word[0], superfam))


            for letter in word[1:]:
                # print(letter)
                id += 1
                #add letter to array
                verts.append((id, letter, superfam))
                edges.append((id-1, id, family))
                #increment id


            return id

        f(0, "", generatedId, "null")
        print(self.count)
        print(verts)
        print(edges)
        return verts, edges



#
# strie = SuffixTree("banana")
# strie.add("ethan", 'a1')
# strie.add("ethanol", 'a2')
# strie.visualize()
# # strie.add("ethanol")
# # strie.add("ethanols")
#
# vertices, edges = strie.to_graphframe(0)
# print(vertices)
# print(edges)
