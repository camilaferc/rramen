'''
Created on Oct 22, 2019

@author: camila
'''
from load.LoadMultimodalNetwork import LoadMultimodalNetwork

def test():
    load = LoadMultimodalNetwork("berlin")
    graph = load.load()
    
test()
