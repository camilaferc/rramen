'''
Created on Oct 20, 2019

@author: camila
'''
from network.MultimodalNetwork import MultimodalNetwork
from load.LoadRoadNetwork import LoadRoadNetwork
from load.LoadTransportationNetwork import LoadTransportationNetwork

class LoadMultimodalNetwork:
    
    def __init__(self, region):
        self.region = region
    
    def load(self):
        graph = MultimodalNetwork()
        loadRoad = LoadRoadNetwork(self.region, graph)
        loadRoad.load()
        print("#nodes:" + str(graph.getNumNodes()))
        print("#edges:"+ str(graph.getNumEdges()))
        print(loadRoad.getMbr())
        graph.setOsmMapping(loadRoad.getOsmMapping())
        loadTransportation = LoadTransportationNetwork(self.region, graph, loadRoad.getMbr())
        loadTransportation.load()
        print("#nodes:" + str(graph.getNumNodes()))
        print("#edges:"+ str(graph.getNumEdges()))
        return graph
    
    def getOsmMapping(self):
        return self.osmMapping
    
    
#load = LoadMultimodalNetwork("berlin")
#load.load()