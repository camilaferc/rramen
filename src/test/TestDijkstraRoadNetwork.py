'''
Created on Oct 21, 2019

@author: camila
'''
from load.LoadMultimodalNetwork import LoadMultimodalNetwork
from shortest_path.Dijkstra import Dijsktra
import random
from _datetime import datetime
import time

def test():
    load = LoadMultimodalNetwork("berlin")
    graph = load.load()
    
    '''
    s = random.randint(0, graph.getNumNodes())
    print(str(s) + ":" + str(graph.getNode(s)))
    targets = set()
    for i in range(20):
        targets.add(random.randint(0, graph.getNumNodes()))
    '''
    
    
    s = 34443
    print(str(s) + ":" + str(graph.getNode(s)))
    targets = set()
    targets.add(38653)
    targets.add(29070)
    targets.add(17841)
    targets.add(36018)
    targets.add(13305)
    
    
    
    dij = Dijsktra(graph)
    
    start = time.time()
    dij.shortestPathToSet(s, datetime.today(), targets, {graph.PRIVATE})
    total = time.time() - start
    print ("Process time: " + str(total))
     
test()