'''
Created on Oct 21, 2019

@author: camila
'''
from load.LoadMultimodalNetwork import LoadMultimodalNetwork
from shortest_path.Dijkstra import Dijsktra
import random
from _datetime import datetime
from datetime import timedelta
import time
import sys

def test():
    load = LoadMultimodalNetwork("berlin")
    graph = load.load()
    
    #nodes in the road network = 38728
    max_id = 38728
    targets = set()
    
    '''
    s = random.randint(0, max_id)
    print(str(s) + ":" + str(graph.getNode(s)))
    for i in range(5):
        targets.add(random.randint(0, max_id))
    '''
        
    s = 35558
    targets.add(25355)
    targets.add(25084)
    targets.add(30140)
    targets.add(11809)
    targets.add(19091)
    
    '''
    s = 34443
    targets = set()
    targets.add(38653)
    targets.add(29070)
    targets.add(17841)
    targets.add(36018)
    targets.add(13305)
    '''
    '''    
    s = 24400
    targets = set()
    targets.add(27470)
    targets.add(13676)
    targets.add(38691)
    targets.add(7505)
    targets.add(10484)
    '''
    
    '''
    s = 20653
    targets.add(1678)
    '''
    
    print(str(s) + ":" + str(graph.getNode(s)))
    dij = Dijsktra(graph)
    
    timestamp = datetime(2019, 10, 28, 18, 0, 0)
    
    start = time.time()
    dij.shortestPathToSetPublic(s, timestamp, targets, {graph.PEDESTRIAN, graph.PUBLIC})
    total = time.time() - start
    print ("Process time: " + str(total))
    
    start = time.time()
    path = dij.reconstructPathToNode(30140)
    total = time.time() - start
    print ("Process time: " + str(total))
    
    
    previous = -1
    arrival_time = timestamp
    for node in path:
        if previous != -1:
            edge = graph.getEdge(previous, node)
            ##print(previous, node, edge)
            tt1 = sys.maxsize
            tt2 = sys.maxsize
            if graph.PEDESTRIAN in edge['travel_time_functions']:
                tt1 = edge['travel_time_functions'][graph.PEDESTRIAN].getTravelTime(arrival_time)
            if graph.PUBLIC in edge['travel_time_functions']:
                tt2 = edge['travel_time_functions'][graph.PUBLIC].getTravelTime(arrival_time)
            min_tt = min(tt1, tt2)
            arrival_time = arrival_time + timedelta(seconds=min_tt)
        print(node, arrival_time, graph.getNode(node))
        previous = node
    
    '''
    neig = graph.getTravelTimeToNeighbors(51629, timestamp, {graph.PEDESTRIAN, graph.PUBLIC})
    for n in neig:
        print(graph.getNode(n), neig[n])
    '''
    
    start = time.time()
    dij.shortestPathToSetPrivate(s, timestamp, targets, {graph.PRIVATE})
    total = time.time() - start
    print ("Process time: " + str(total))
    
    '''
    start = time.time()
    path = dij.reconstructPathToNode(7505)
    total = time.time() - start
    print ("Process time: " + str(total))
    #for node in path:
    #    print(node, graph.getNode(node))
    
    '''
    
     
test()