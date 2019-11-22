'''
Created on Oct 22, 2019

@author: camila
'''
from network.MultimodalNetwork import MultimodalNetwork
from load.LoadRoadNetwork import LoadRoadNetwork

def test():
    graph = MultimodalNetwork()
    load = LoadRoadNetwork("berlin", graph)
    load.load()
    print(graph.getNumNodes())
    print(graph.getNumEdges())
    
test()
