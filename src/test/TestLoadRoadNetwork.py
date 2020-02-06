'''
Created on Oct 22, 2019

@author: camila
'''
from load.LoadRoadNetwork import LoadRoadNetwork
from network.MultimodalNetwork import MultimodalNetwork

def test():
    region = "berlin"
    print (region)
    
    graph = MultimodalNetwork()
    load = LoadRoadNetwork(region, graph)
    load.load()
    print(graph.getNumNodes())
    print(graph.getNumEdges())
    
test()
