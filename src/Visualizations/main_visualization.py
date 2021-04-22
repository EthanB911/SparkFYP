import networkx as nx
import matplotlib.pyplot as plt


def Convert(lst):
    res_dct = {lst[i]: lst[i + 1] for i in range(0, len(lst), 2)}
    return res_dct

def PlotGraph(edges, vertices):
    gp = nx.from_pandas_edgelist(edges.toPandas(), 'src', 'dst', create_using=nx.OrderedMultiDiGraph())
    new_vertices = Convert(vertices)
    print(new_vertices)
    G= nx.relabel_nodes(gp, new_vertices, copy=False)
    G.remove_edges_from(nx.selfloop_edges(G))
    nx.draw(G, with_labels=True)
    plt.show()c