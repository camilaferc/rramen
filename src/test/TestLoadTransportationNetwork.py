'''
Created on Oct 22, 2019

@author: camila
'''
from load.LoadTransportationNetwork import LoadTransportationNetwork
from network.MultimodalNetwork import MultimodalNetwork

def test():
    region = "edmonton"
    graph = MultimodalNetwork()
    load = LoadTransportationNetwork(region, graph)
    load.load()
    
test()
