'''
Created on Oct 21, 2019

@author: camila
'''
from _datetime import datetime
from datetime import timedelta
import random
import sys
import time

from database.PostgisDataManager import PostgisDataManager
from load.LoadMultimodalNetwork import LoadMultimodalNetwork
from path.Path import Path
from shortest_path.Dijkstra import Dijsktra
from web.server_rr_test import createVirtualNodeEdge


def test():
    region = "edmonton"
    load = LoadMultimodalNetwork(region)
    graph = load.load()
    
    #nodes in the road network = 38728
    #max_id = 38728
    targets_public = set()
    targets_private = set()
    
    '''
    s = random.randint(0, max_id)
    print(str(s) + ":" + str(graph.getNode(s)))
    for i in range(5):
        targets.add(random.randint(0, max_id))
    '''
    
    '''   
    s = 35558
    targets.add(25355)
    targets.add(25084)
    targets.add(30140)
    targets.add(11809)
    targets.add(19091)
    '''
    
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
    dataManager = PostgisDataManager()
    startLat = 53.523794
    startLon = -113.511714
    
    
    targetLat = 53.504094
    targetLon = -113.498496
    
    timestamp = datetime(2020, 2, 5, 13, 45, 0)
    
    dij = Dijsktra(graph)
    
    
    (edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat) = dataManager.getClosestEdgeByClass(startLat, startLon, region, graph.PEDESTRIAN_WAYS)
    print(edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat)
    s_public = createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, osm_source, osm_target, source_ratio)
    
    
    
    (edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat) = dataManager.getClosestEdgeByClass(targetLat, targetLon, region, graph.PEDESTRIAN_WAYS)
    print(edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat)
    t_public = createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, osm_source, osm_target, source_ratio)
    #print(t_public, graph.graph[t_public])
    targets_public.add(t_public)
    
    #targets_public.add(graph.osmMapping[3697399470])
    
    #print(str(s_public) + ":" + str(graph.getNode(s_public)))
    #print(targets_public)
    
    print(s_public, targets_public)
    
    start = time.time()
    travel_time_public, parents_public = dij.shortestPathToSetPublic(s_public, timestamp, targets_public, {graph.PEDESTRIAN, graph.PUBLIC})
    print(travel_time_public)
    total = time.time() - start
    print ("Process time: " + str(total))
    
    start = time.time()
    path = Path(parents_public).reconstructPathToNode(t_public)
    print(path)
    total = time.time() - start
    print ("Process time: " + str(total))
    
    
    previous = -1
    arrival_time = timestamp
    for node in path:
        if previous != -1:
            edge = graph.getEdge(previous, node)
            #if "original_edge_id" in edge:
            #    print(edge["original_edge_id"])
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
    (edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat) = dataManager.getClosestEdge(startLat, startLon, region)
    s_private = createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, osm_source, osm_target, source_ratio)
    
    (edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat) = dataManager.getClosestEdge(targetLat, targetLon, region)
    
    print(edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat)
    t_private = createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, osm_source, osm_target, source_ratio)
    print(t_private, graph.graph[t_private])
    targets_private.add(t_private)
    
    start = time.time()
    dij.shortestPathToSetPrivate(s_private, timestamp, targets_private)
    total = time.time() - start
    print ("Process time: " + str(total))
    '''
    
    
    '''
    start = time.time()
    path = dij.reconstructPathToNode(7505)
    total = time.time() - start
    print ("Process time: " + str(total))
    #for node in path:
    #    print(node, graph.getNode(node))
    '''
    
    
     
test()