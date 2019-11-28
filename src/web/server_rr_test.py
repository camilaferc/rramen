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
from builtins import str


app = Flask(__name__)
app.config.from_object(__name__)

MAPBOX_ACCESS_KEY = 'pk.eyJ1IjoiY2FtaWxhZmVyYyIsImEiOiJjazB3aGJ5emkwMzNqM29tbWxkZ2t3OWJwIn0.LYcGltgmo4yj5zqhDJGoEA'

#app.config.from_envvar('APP_CONFIG_FILE', silent=True)
#MAPBOX_ACCESS_KEY = app.config['MAPBOX_ACCESS_KEY']


graph = None
dataManager = PostgisDataManager()
region = "berlin"
id_map_source_public = {}
id_map_target_public = {}
id_map_source_private = {}
id_map_target_private = {}
parent_tree_public = {}
parent_tree_private = {}
tt_public = {}
tt_private = {}
map_source_coord = {}
map_target_coord = {}
ONE_TO_MANY = 0
MANY_TO_ONE = 1
MANY_TO_MANY = 2

@app.route('/rr_test')
def mapbox_gl():
    global graph
    
    if not graph:
        load = LoadMultimodalNetwork("berlin")
        graph = load.load()
    
    return render_template('rr_test.html', 
        ACCESS_KEY=MAPBOX_ACCESS_KEY
    )

@app.route('/rr_planner_test')
def rr_planner():
    global graph, dataManager, region
    
    if not graph:
        load = LoadMultimodalNetwork("berlin")
        graph = load.load()
    
    routes = dataManager.getRoutes(region)
    return render_template('rr_planner_test.html', 
        ACCESS_KEY=MAPBOX_ACCESS_KEY,
        routes = routes
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
    
@app.route('/receiver', methods = ['POST', 'GET'])
def worker():
    # read json + reply
    global graph, dataManager, region, id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public, tt_public, tt_private 
    global map_source_coord, map_target_coord
    data = request.get_json(force=True)
    
    map_source_coord = {}
    map_target_coord = {}
    
    id_map_source_public = {}
    id_map_target_public = {}
    id_map_source_private = {}
    id_map_target_private = {}
    
    sources_coordinates = data['sources']
    targets_coordinates = data['targets']
    polygon_source_coords = data['polygon_source_coords']
    polygon_target_coords = data['polygon_target_coords']
    
    removed_routes = None
    if "removed_routes" in data and data["removed_routes"]:
        removed_routes = data["removed_routes"];
    
    if sources_coordinates:
        print("Source is a set of markers " + str(len(sources_coordinates)))
        sources_private, sources_public = getNodesFromMarkersCoordinates(sources_coordinates, "source")
    elif polygon_source_coords:
        print("Source is a polygon")
        sources_private, sources_public = getNodesWithinPolygon(polygon_source_coords, "source")
        
    if targets_coordinates:
        print("Target is a set of markers " + str(len(targets_coordinates)))
        targets_private, targets_public = getNodesFromMarkersCoordinates(targets_coordinates, "target")
    elif polygon_target_coords:
        print("Target is a polygon")
        targets_private, targets_public = getNodesWithinPolygon(polygon_target_coords, "target")
    
    print(sources_private)
    print(sources_public)
    print(targets_private)
    print(targets_public)
    
    print("maps:")
    print(map_source_coord)
    print(map_target_coord)
    
    #timestamp = datetime.today()
    time = data['timestamp']
    timestamp = datetime.fromtimestamp(time/1000)
    print(timestamp)
    
    
    tt_public, tt_private, node_colors, colored_type = computeRelativeReachability(sources_public, targets_public, sources_private, targets_private, 
                                                                                   timestamp, removed_routes)
    print(node_colors)
    
    if colored_type == "source":
        map_coord = map_source_coord
    else:
        map_coord = map_target_coord
    
    markers = []
    print(map_coord)
    for n in node_colors:
        n_coord = map_coord[n]
        point = Point([n_coord[1], n_coord[0]])
        properties = {'marker-color': node_colors[n]}
        feature = Feature(geometry = point, properties = properties)
        markers.append(feature)
    
    gc = FeatureCollection(markers)
    #print(gc)
    return gc
    #return dumps(res)
    #return gc, ml_public
    

def getNodesFromMarkersCoordinates(coordinates, location_type): 
    global id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public
    nodes_private = set()
    nodes_public = set()
    for i in range(len(coordinates)):
        c = coordinates[i]
        
        (edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat) = dataManager.getClosestEdge(c['lat'], c['lon'], region)
        print(edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat)
        node_id_private = createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, osm_source, osm_target, source_ratio)
        nodes_private.add(node_id_private)
        if location_type == "source":
            id_map_source_private[i] = node_id_private
            map_source_coord[i] = (c['lat'], c['lon'])
        else:
            id_map_target_private[i] = node_id_private
            map_target_coord[i] = (c['lat'], c['lon'])
        
        
        (edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat) = dataManager.getClosestEdgeByClass(c['lat'], c['lon'], region, graph.PEDESTRIAN_WAYS)
        node_id_public = createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, osm_source, osm_target, source_ratio)
        nodes_public.add(node_id_public)
        if location_type == "source":
            id_map_source_public[i] = node_id_public
            map_source_coord[i] = (c['lat'], c['lon'])
        else:
            id_map_target_public[i] = node_id_public
            map_target_coord[i] = (c['lat'], c['lon'])
    
    return nodes_private, nodes_public

def getNodesWithinPolygon(polygon_coordinates, location_type):
    global map_source_coord, map_target_coord
    global id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public
    polygon_points = dataManager.getPointsWithinPolygon(region, polygon_coordinates)
    #print(polygon_coordinates)
    print(len(polygon_points))
    if location_type == "source":
        map_coord = map_source_coord
        id_map_public = id_map_source_public
        id_map_private = id_map_source_private
    else:
        map_coord = map_target_coord
        id_map_public = id_map_target_public
        id_map_private = id_map_target_private
    
    nodes = set()
    osm_mapping = graph.getOsmMapping()
    i = 0
    for p in polygon_points:
        node_id = osm_mapping[p]
        nodes.add(node_id)
        node = graph.getNode(node_id)
        
        map_coord[i] = (node['lat'], node['lon']) 
        
        id_map_public[i] = node_id
        id_map_private[i] = node_id
        i+=1
    
    return nodes, nodes      
        
    
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
    
    
    tt_public, tt_private, node_colors, colored_type = computeRelativeReachability(source, targets, timestamp)
    print(node_colors)
    
    if colored_type == "source":
        map_coord = map_source_coord
    else:
        map_coord = map_target_coord
    
    target_markers = []
    for n in node_colors:
        n_coord = map_coord[n]
        point = Point([n_coord[1], n_coord[0]])
        properties = {'marker-color': node_colors[n]}
        feature = Feature(geometry = point, properties = properties)
        target_markers.append(feature)
    
    gc = FeatureCollection(target_markers)
    
    return gc


@app.route('/route', methods = ['POST', 'GET'])
def getRouteGeometry():
    global dataManager
    data = request.get_json(force=True)
    
    route_id = data['route_id']
    print(route_id)
    route_geom = dataManager.getRouteGeometry(route_id, region)
    
    feature = Feature(geometry = route_geom)
    route_geom = [feature]
    fc = FeatureCollection(route_geom)
    
    res = {"route_geom": fc}
    return res
        
@app.route('/path', methods = ['POST', 'GET'])
def getPathGeometry():
    global id_map_source_private, id_map_source_public, id_map_target_private, id_map_target_public, tt_public, tt_private
    
    data = request.get_json(force=True)
    
    marker_id = int(data['id'])
    location_type = data['location_type']
    print(marker_id, location_type)
    
    if location_type == "source":
        node_id_public = id_map_source_public[marker_id]
        node_id_private = id_map_source_private[marker_id]
    else:
        print(id_map_target_public)
        print(id_map_target_private)
        node_id_public = id_map_target_public[marker_id]
        node_id_private = id_map_target_private[marker_id]
        
    paths = []
    
    source_public = id_map_source_public[0]
    print(source_public)
    source_private = id_map_source_private[0]
    print(source_private)
    
    pathPublic = Path(parent_tree_public[source_public])
    pathPublicGeom = pathPublic.getPathGeometry(graph, node_id_public, region)
    
    properties = {'line-color': 'b'}
    feature = Feature(geometry = pathPublicGeom, properties = properties)
    paths.append(feature)
    
    pathPrivate = Path(parent_tree_private[source_private])
    pathPrivateGeom = pathPrivate.getPathGeometry(graph, node_id_private, region)
    properties = {'line-color': 'r'}
    feature = Feature(geometry = pathPrivateGeom, properties = properties)
    paths.append(feature)
    
    tt_node_public = round(tt_public[source_public][node_id_public]/60)
    tt_node_private = round(tt_private[source_private][node_id_private]/60)
    
    fc = FeatureCollection(paths)
    res = {"path_geom": fc, "tt_public": tt_node_public, "tt_private": tt_node_private}
    #print(res)
    return res
    

def computeRelativeReachability(sources_public, targets_public, sources_private, targets_private, timestamp, removed_routes=None): 
    global parent_tree_public, parent_tree_private, id_map_private, id_map_public
    dij = Dijsktra(graph)
    parent_tree_public = {}
    parent_tree_private = {}
    
    print('Removed routes:' + str(removed_routes))
    
    start = time.time()
    tt_public, parent_tree_public = dij.manyToManyPublic(sources_public, targets_public, timestamp, removed_routes)
    #dij.shortestPathToSetPublic(source_public, timestamp, targets_public, {graph.PEDESTRIAN, graph.PUBLIC})
    #parent_tree_public = dij.getParentTree()
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
    tt_private, parent_tree_private = dij.manyToManyPrivate(sources_private, targets_private, timestamp)
    total = time.time() - start
    print ("Process time: " + str(total))
    
    node_colors, colored_type = getNodesColors(tt_public, tt_private)
    
    return tt_public, tt_private, node_colors, colored_type


def getNodesColors(tt_public, tt_private):
    global id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public
    colored_type = "target"
    
    if len(id_map_source_public) > 1 and len(id_map_target_public) == 1:
        colored_type = "source"
        
    if colored_type == "source":
        node_colors = colorSources(tt_public, tt_private)
        return node_colors, colored_type
    else:
        node_colors = colorTargets(tt_public, tt_private)
        return node_colors, colored_type
            
def colorSources(tt_public, tt_private):
    global id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public
    node_colors = {}
    for i in range (len(id_map_source_private)):
        s_public = id_map_source_public[i]
        s_private = id_map_source_private[i]
        if s_public not in tt_public:
            print(s_public)
            print("not found by public transit")
            node_colors[i] = 0.5
        elif s_private not in tt_private:
            print(s_private)
            print("not found by car")
            node_colors[i] = 0.5
        else:
            tt_public_s = tt_public[s_public]
            tt_private_s = tt_private[s_private]
            num_public = 0
            num_total = 0
            for j in range (len(id_map_target_private)):
                t_public = id_map_target_public[j]
                t_private = id_map_target_private[j]
                
                if t_public in tt_public_s and t_private in tt_private_s:
                    if tt_public_s[t_public] <=  tt_private_s[t_private]:
                        num_public += 1
                    num_total += 1
                else:
                    print("Node node found by both:" +  str(t_public) + "," + str(t_private))
            if num_total > 0:
                node_colors[i] = float(num_public/num_total)
            else:
                node_colors[i] = 0.5
    return node_colors

def colorTargets(tt_public, tt_private):
    print("Coloring targets...")
    global id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public
    node_colors = {}
    targets = {}
    for i in range (len(id_map_source_private)):
        s_public = id_map_source_public[i]
        s_private = id_map_source_private[i]
        if s_public in tt_public and s_private in tt_private:
            tt_public_s = tt_public[s_public]
            tt_private_s = tt_private[s_private]
            #print(tt_public_s)
            #print(tt_private_s)
            for j in range (len(id_map_target_private)):
                t_public = id_map_target_public[j]
                t_private = id_map_target_private[j]
                #print(t_public, t_private)
                num_public = 0
                if t_public not in tt_public_s:
                    print(str(t_public) + " not found by public transit")
                    #node_colors[j] = 0.5
                elif t_private not in tt_private_s:
                    print(str(t_private) + " not found by public transit")
                    #node_colors[j] = 0.5
                else: 
                    if tt_public_s[t_public] <=  tt_private_s[t_private]:
                        num_public += 1
                    if j in targets:
                        targets[j][0] += num_public
                        targets[j][1] += 1
                    else:
                        targets[j] = [num_public, 1]
    
    for t in targets:
        t_num = targets[t]
        node_colors[t] = float(t_num[0]/t_num[1])
    
    return node_colors
                

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