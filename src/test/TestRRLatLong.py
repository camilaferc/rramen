'''
Created on Nov 4, 2019

@author: camila
'''

from _datetime import datetime
from datetime import timedelta
import sys
import time

from database.PostgisDataManager import PostgisDataManager
from load.LoadMultimodalNetwork import LoadMultimodalNetwork
from shortest_path.Dijkstra import Dijsktra
from web.server_rr_test import createVirtualNodeEdge


load = LoadMultimodalNetwork("berlin")
graph = load.load()

timestamp = datetime(2019, 11, 4, 18, 0, 0)
dataManager = PostgisDataManager()


(edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat) = dataManager.getClosestEdgeRatio(52.5123759867379, 13.3799182055483, "berlin")
source = createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, osm_source, osm_target, source_ratio)
print(source)

targets = set()
(edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat) = dataManager.getClosestEdgeRatio(52.5033010992503, 13.4070841962566, "berlin")
target = createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, osm_source, osm_target, source_ratio)
print(target)
targets.add(target)

dij = Dijsktra(graph)
    
start = time.time()
tt_public = dij.shortestPathToSetPublic(source, timestamp, targets, {graph.PEDESTRIAN, graph.PUBLIC})
total = time.time() - start
print ("Process time: " + str(total))


for t in targets:
    path = dij.reconstructPathToNode(t)
    
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
        
#edge = graph.getEdge(49036,49037)
#print(edge['travel_time_functions'][graph.PUBLIC].getTravelTime(datetime(2019, 11, 4, 18, 20, 0)))
      
'''    
start = time.time()
tt_private = dij.shortestPathToSetPrivate(source, timestamp, targets, {graph.PRIVATE})
total = time.time() - start
print ("Process time: " + str(total))
'''
