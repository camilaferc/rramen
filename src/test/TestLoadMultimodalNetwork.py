'''
Created on Oct 22, 2019

@author: camila
'''
from load.LoadMultimodalNetwork import LoadMultimodalNetwork

def test():
    region = "berlin"
    print (region)
    
    load = LoadMultimodalNetwork(region)
    load.load()
    
test()
