'''
Created on Oct 29, 2019

@author: camila
'''

from _datetime import datetime
from datetime import timedelta
import sys
import time

from flask import Flask, request, session, g, redirect, \
    url_for, abort, render_template, flash
from flask.cli import ScriptInfo
from geojson import Feature, Point, dumps, GeometryCollection, FeatureCollection, dump, loads
from geojson.geometry import LineString, MultiLineString
import requests

from database.PostgisDataManager import PostgisDataManager
from load.LoadMultimodalNetwork import LoadMultimodalNetwork
from path.Path import Path
from shortest_path.Dijkstra import Dijsktra


app = Flask(__name__)
app.config.from_object(__name__)

MAPBOX_ACCESS_KEY = 'pk.eyJ1IjoiY2FtaWxhZmVyYyIsImEiOiJjazB3aGJ5emkwMzNqM29tbWxkZ2t3OWJwIn0.LYcGltgmo4yj5zqhDJGoEA'

#app.config.from_envvar('APP_CONFIG_FILE', silent=True)
#MAPBOX_ACCESS_KEY = app.config['MAPBOX_ACCESS_KEY']


graph = None
dataManager = PostgisDataManager()
region = "berlin"
id_map = {}
parent_tree_public = {}
parent_tree_private = {}
tt_public = {}
tt_private = {}

@app.route('/rr_test')
def mapbox_gl():
    global graph
    
    if not graph:
        load = LoadMultimodalNetwork("berlin")
        graph = load.load()
    
    return render_template('rr_test.html', 
        ACCESS_KEY=MAPBOX_ACCESS_KEY
    )
    
@app.route('/rr_region_test')
def rr_region():
    global graph
    if not graph:
        load = LoadMultimodalNetwork("berlin")
        graph = load.load()
    
    return render_template('rr_region_test.html', 
        ACCESS_KEY=MAPBOX_ACCESS_KEY
    )
    
@app.route('/rr_boundaries_test')
def rr_boundaries():
    global graph
    
    #load = LoadMultimodalNetwork("berlin")
    #graph = load.load()
    
    return render_template('rr_boundaries_test.html', 
        ACCESS_KEY=MAPBOX_ACCESS_KEY
    )
    
@app.route('/rr_neighborhood_test')
def rr_neighborhood():
    global graph
    
    if not graph:
        load = LoadMultimodalNetwork("berlin")
        graph = load.load()
    polygons = dataManager.getNeighborhoodsPolygons(region)
    return render_template('rr_neighborhood_test.html', 
        ACCESS_KEY=MAPBOX_ACCESS_KEY,
        polygons = polygons
    )
    
@app.route('/rr_rectangle_test')
def rr_rectangle():
    global graph
    #load = LoadMultimodalNetwork("berlin")
    #graph = load.load()
    
    return render_template('rr_rectangle_test.html', 
        ACCESS_KEY=MAPBOX_ACCESS_KEY
    )
    
    
@app.route('/receiver', methods = ['POST', 'GET'])
def worker():
    # read json + reply
    global graph, dataManager, region, id_map, tt_public, tt_private
    data = request.get_json(force=True)
    
    
    map_coord = {}
    
    startLat = data['startLat']
    startLon = data['startLon']
    (edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat) = dataManager.getClosestEdge(startLat, startLon, region)
    source = createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, osm_source, osm_target, source_ratio)
    print(graph.getNode(source))
    
    
    targets_coord = data['targets']
    targets = set()
    id_map = {}
    for i in range(len(targets_coord)):
        t = targets_coord[i]
        (edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat) = dataManager.getClosestEdge(t['lat'], t['lon'], region)
        
        node_id = createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, osm_source, osm_target, source_ratio)
        targets.add(node_id)
        map_coord[node_id] = (t['lat'], t['lon'])
        id_map[i] = node_id
    
    print(source) 
    print(targets) 
    
    #timestamp = datetime.today()
    time = data['timestamp']
    timestamp = datetime.fromtimestamp(time/1000)
    print(timestamp)
    
    
    target_colors, tt_public, tt_private = computeRelativeReachability(source, targets, timestamp)
    print(target_colors)
    
    target_markers = []
    for t in target_colors:
        t_coord = map_coord[t]
        point = Point([t_coord[1], t_coord[0]])
        properties = {'marker-color': target_colors[t]}
        feature = Feature(geometry = point, properties = properties)
        target_markers.append(feature)
    
    gc = FeatureCollection(target_markers)
    
    #print(gc)
    
    return gc
    #return dumps(res)
    #return gc, ml_public
    

@app.route('/receiver_region', methods = ['POST', 'GET'])
def worker_region():
    # read json + reply
    global graph, dataManager, region, id_map, tt_public, tt_private
    data = request.get_json(force=True)
    
    
    startLat = data['startLat']
    startLon = data['startLon']
    (edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat) = dataManager.getClosestEdge(startLat, startLon, region)
    source = createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, osm_source, osm_target, source_ratio)
    print(graph.getNode(source))
    
    
    polygon_coord = data['coordinates'][0]
    polygon_points = dataManager.getPointsWithinPolygon(region, polygon_coord)
    #print(polygon_points)
    
    targets = set()
    osm_mapping = graph.getOsmMapping()
    map_coord = {}
    for p in polygon_points:
        node_id = osm_mapping[p]
        targets.add(node_id)
        node = graph.getNode(node_id)
        map_coord[node_id] = (node['lat'], node['lon'])
    
    print(source) 
    print(len(targets)) 
    
    #timestamp = datetime.today()
    time = data['timestamp']
    timestamp = datetime.fromtimestamp(time/1000)
    print(timestamp)
    
    
    target_colors, tt_public, tt_private = computeRelativeReachability(source, targets, timestamp)
    print(target_colors)
    
    target_markers = []
    for t in target_colors:
        t_coord = map_coord[t]
        point = Point([t_coord[1], t_coord[0]])
        properties = {'marker-color': target_colors[t]}
        feature = Feature(geometry = point, properties = properties)
        target_markers.append(feature)
    
    gc = FeatureCollection(target_markers)
    
    return gc
    
    
@app.route('/path', methods = ['POST', 'GET'])
def getPathGeometry():
    global id_map, tt_public, tt_private
    marker_id = int(request.get_json(force=True))
    print(marker_id)
    node_id = id_map[marker_id]
    paths = []
    
    pathPublic = Path(parent_tree_public)
    pathPublicGeom = pathPublic.getPathGeometry(graph, node_id, region)
    
    properties = {'line-color': 'b'}
    feature = Feature(geometry = pathPublicGeom, properties = properties)
    paths.append(feature)
    
    pathPrivate = Path(parent_tree_private)
    pathPrivateGeom = pathPrivate.getPathGeometry(graph, node_id, region)
    properties = {'line-color': 'r'}
    feature = Feature(geometry = pathPrivateGeom, properties = properties)
    paths.append(feature)
    
    tt_node_public = round(tt_public[node_id]/60)
    tt_node_private = round(tt_private[node_id]/60)
    
    fc = FeatureCollection(paths)
    res = {"path_geom": fc, "tt_public": tt_node_public, "tt_private": tt_node_private}
    print(res)
    return res
    

def computeRelativeReachability(source, targets, timestamp): 
    global parent_tree_public, parent_tree_private
    dij = Dijsktra(graph)
    parent_tree_public = {}
    parent_tree_private = {}
    
    start = time.time()
    tt_public = dij.shortestPathToSetPublic(source, timestamp, targets, {graph.PEDESTRIAN, graph.PUBLIC})
    parent_tree_public = dij.getParentTree()
    total = time.time() - start
    print ("Process time: " + str(total))
    
    '''
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
    '''
        
    start = time.time()
    tt_private = dij.shortestPathToSetPrivate(source, timestamp, targets, {graph.PRIVATE})
    parent_tree_private = dij.getParentTree()
    total = time.time() - start
    print ("Process time: " + str(total))
    
    target_colors = {}
    for t in targets:
        if t not in tt_public:
            print(t)
            print("not found by public transit")
            target_colors[t] = 'n'
        elif t not in tt_private:
            print(t)
            print("not found by car")
            target_colors[t] = 'n'
        elif tt_public[t] <=  tt_private[t]:
            target_colors[t] = 'b'
        else: 
            target_colors[t] = 'r'
            
    return target_colors, tt_public, tt_private


def createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, osm_source, osm_target, source_ratio):
    osm_mapping = graph.getOsmMapping()
    if source_ratio == 0.0:
        # node is equal to the source
        return osm_mapping[osm_source]
    elif source_ratio == 1.0:
        #node is equal to the target
        return osm_mapping[osm_target]
    else:
        node_id = graph.getNumNodes()
        graph.addNode(node_id, node_lat, node_lon, graph.ROAD)
        
        #splitting edge
        source = osm_mapping[osm_source]
        target = osm_mapping[osm_target]
        
        original_edge = graph.getEdge(source, target)
        
        modes = original_edge['modes']
        left_functions = {}
        right_functions = {}
        
        for mode in modes:
            
            function = original_edge['travel_time_functions'][mode]
            left, right = function.splitFunctionRatio(source_ratio)
            
            #print(left, right)
            
            left_functions[mode] = left
            right_functions[mode] = right
        
        graph.addEdge(source, node_id, graph.ROAD, modes, left_functions, edge_id, 1)
        graph.addEdge(node_id, target, graph.ROAD, modes, right_functions, edge_id, 2)
        
        edge_rev = graph.getEdge(target, source)
        if edge_rev:
            #print("Reverse edge exists")
            modes_rev = edge_rev['modes']
            if modes_rev == modes:
                #print("Same modes")
                graph.addEdge(node_id, source, graph.ROAD, modes_rev, left_functions, edge_id, 1)
                graph.addEdge(target, node_id, graph.ROAD, modes_rev, right_functions, edge_id, 2)
            else:
                left_functions_rev = {}
                right_functions_rev = {}
                for mode in modes_rev:
                    if mode in left_functions:
                        left_functions_rev[mode] = left_functions[mode]
                        right_functions_rev[mode] = right_functions[mode]
                    else:
                        #print("New mode:" + str(mode))
                        function = edge_rev['travel_time_functions'][mode]
                        left, right = function.splitFunctionRatio(source_ratio)
                        left_functions[mode] = left
                        right_functions[mode] = right
                graph.addEdge(node_id, source, graph.ROAD, modes_rev, left_functions_rev, edge_id, 1)
                graph.addEdge(target, node_id, graph.ROAD, modes_rev, right_functions_rev, edge_id, 2)
        return node_id
    
 

if __name__ == '__main__':
    # run!
    app.run()