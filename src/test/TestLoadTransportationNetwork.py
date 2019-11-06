'''
Created on Oct 22, 2019

@author: camila
'''
from load.LoadTransportationNetwork import LoadTransportationNetwork
from network.MultimodalNetwork import MultimodalNetwork

def test():
    graph = MultimodalNetwork()
    load = LoadTransportationNetwork("berlin", graph)
    load.load()
    
test()
