'''
Created on Oct 22, 2019

@author: camila
'''
from transit.CreateTransportationNetwork import CreateTransportationNetwork

def test():
    region = "berlin"
    create = CreateTransportationNetwork(region)
    create.run()
    
test()
