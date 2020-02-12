'''
Created on Oct 20, 2019

@author: camila
'''

from load.LoadRoadNetwork import LoadRoadNetwork
from network.MultimodalNetwork import MultimodalNetwork
from transit.LoadTransportationNetwork import LoadTransportationNetwork


class LoadMultimodalNetwork:
    
    def __init__(self, region):
        self.region = region
    
    def load(self):
        graph = MultimodalNetwork()
        
        loadRoad = LoadRoadNetwork(self.region, graph)
        loadRoad.load()
        print("#nodes:" + str(graph.getNumNodes()))
        print("#edges:"+ str(graph.getNumEdges()))
        loadTransportation = LoadTransportationNetwork(self.region, graph)
        loadTransportation.load()
        print("#nodes:" + str(graph.getNumNodes()))
        print("#edges:"+ str(graph.getNumEdges()))
        
        return graph
    
    
