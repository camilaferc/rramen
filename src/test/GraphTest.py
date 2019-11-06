'''
Created on Oct 5, 2019

@author: camila
'''

from database.ImportGraph import buildGraph
import networkx as nx
import matplotlib.pyplot as plt

def isConnected(graph):
    comp = list(nx.connected_components(graph.graph))
    if len(comp) > 1:
        return False
    else:
        return True
    


G = nx.petersen_graph()
plt.subplot(121)
nx.draw(G, with_labels=True, font_weight='bold')
plt.show()

plt.subplot(122)
nx.draw_shell(G, nlist=[range(5, 10), range(5)], with_labels=True, font_weight='bold')
plt.show()