'''
Created on Oct 29, 2019

@author: camila
'''

from flask import Flask, request, session, g, redirect, \
    url_for, abort, render_template, flash
from geojson import Feature, Point, dumps, GeometryCollection, FeatureCollection
import requests

from database.PostgisDataManager import PostgisDataManager
from load.LoadMultimodalNetwork import LoadMultimodalNetwork
from shortest_path.Dijkstra import Dijsktra
from _datetime import datetime
import time
import sys
from datetime import timedelta


app = Flask(__name__)
app.config.from_object(__name__)

MAPBOX_ACCESS_KEY = 'pk.eyJ1IjoiY2FtaWxhZmVyYyIsImEiOiJjazB3aGJ5emkwMzNqM29tbWxkZ2t3OWJwIn0.LYcGltgmo4yj5zqhDJGoEA'

#app.config.from_envvar('APP_CONFIG_FILE', silent=True)
#MAPBOX_ACCESS_KEY = app.config['MAPBOX_ACCESS_KEY']

ROUTE = [
    {"lat": 64.0027441, "long": -22.7066262, "name": "Keflavik Airport", "is_stop_location": True},
    {"lat": 64.0317168, "long": -22.1092311, "name": "Hafnarfjordur", "is_stop_location": True},
    {"lat": 63.99879, "long": -21.18802, "name": "Hveragerdi", "is_stop_location": True},
    {"lat": 63.4194089, "long": -19.0184548, "name": "Vik", "is_stop_location": True},
    {"lat": 63.5302354, "long": -18.8904333, "name": "Thakgil", "is_stop_location": True},
    {"lat": 64.2538507, "long": -15.2222918, "name": "Hofn", "is_stop_location": True},
    {"lat": 64.913435, "long": -14.01951, "is_stop_location": False},
    {"lat": 65.2622588, "long": -14.0179538, "name": "Seydisfjordur", "is_stop_location": True},
    {"lat": 65.2640083, "long": -14.4037548, "name": "Egilsstadir", "is_stop_location": True},
    {"lat": 66.0427545, "long": -17.3624953, "name": "Husavik", "is_stop_location": True},
    {"lat": 65.659786, "long": -20.723364, "is_stop_location": False},
    {"lat": 65.3958953, "long": -20.9580216, "name": "Hvammstangi", "is_stop_location": True},
    {"lat": 65.0722555, "long": -21.9704238, "is_stop_location": False},
    {"lat": 65.0189519, "long": -22.8767959, "is_stop_location": False},
    {"lat": 64.8929619, "long": -23.7260926, "name": "Olafsvik", "is_stop_location": True},
    {"lat": 64.785334, "long": -23.905765, "is_stop_location": False},
    {"lat": 64.174537, "long": -21.6480148, "name": "Mosfellsdalur", "is_stop_location": True},
    {"lat": 64.0792223, "long": -20.7535337, "name": "Minniborgir", "is_stop_location": True},
    {"lat": 64.14586, "long": -21.93955, "name": "Reykjavik", "is_stop_location": True},
]

graph = None

@app.route('/rr_test')
def mapbox_gl():
    global graph
    #load = LoadMultimodalNetwork("berlin")
    #graph = load.load()

    return render_template('rr_test.html', 
        ACCESS_KEY=MAPBOX_ACCESS_KEY
    )
    
@app.route('/receiver', methods = ['POST', 'GET'])
def worker():
    # read json + reply
    global graph
    data = request.get_json(force=True)
    dataManager = PostgisDataManager()
    
    region = "berlin"
    
    map_coord = {}
    
    startLat = data['startLat']
    startLon = data['startLon']
    (edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat) = dataManager.getClosestEdge(startLat, startLon, region)
    source = createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, osm_source, osm_target, source_ratio)
    print(graph.getNode(source))
    
    
    targets_coord = data['targets']
    targets = set()
    for t in targets_coord:
        (edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat) = dataManager.getClosestEdge(t['lat'], t['lon'], region)
        
        node_id = createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, osm_source, osm_target, source_ratio)
        targets.add(node_id)
        map_coord[node_id] = (t['lat'], t['lon'])
    
    print(source) 
    print(targets) 
    
    #timestamp = datetime.today()
    time = data['timestamp']
    timestamp = datetime.fromtimestamp(time/1000)
    print(timestamp)
    
    '''
    properties_red = {
            'icon': 'campsite',
            'marker-color': '#ff0000',
    }
    
    properties_blue = {
            'icon': 'campsite',
            'marker-color': '#0000FF',
    }
    '''
    
    target_colors = computeRelativeReachability(source, targets, timestamp)
    print(target_colors)
    
    target_markers = []
    for t in target_colors:
        t_coord = map_coord[t]
        point = Point([t_coord[1], t_coord[0]])
        properties = {'marker-color': target_colors[t]}
        feature = Feature(geometry = point, properties = properties)
        target_markers.append(feature)
    
    gc = FeatureCollection(target_markers)
    print(gc)
    #return dumps(gc)
    return gc

def computeRelativeReachability(source, targets, timestamp): 
    dij = Dijsktra(graph)
    
    start = time.time()
    tt_public = dij.shortestPathToSetPublic(source, timestamp, targets, {graph.PEDESTRIAN, graph.PUBLIC})
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
    total = time.time() - start
    print ("Process time: " + str(total))
    
    target_colors = {}
    for t in targets:
        if tt_public[t] <=  tt_private[t]:
            target_colors[t] = 'b'
        else: 
            target_colors[t] = 'r'
            
    return target_colors
            
           

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
    

ROUTE_URL = "https://api.mapbox.com/directions/v5/mapbox/driving/{0}.json?access_token={1}&overview=full&geometries=geojson"

def create_route_url():
    # Create a string with all the geo coordinates
    lat_longs = ";".join(["{0},{1}".format(point["long"], point["lat"]) for point in ROUTE])
    # Create a url with the geo coordinates and access token
    url = ROUTE_URL.format(lat_longs, MAPBOX_ACCESS_KEY)
    return url

def get_route_data():
    # Get the route url
    route_url = create_route_url()
    # Perform a GET request to the route API
    result = requests.get(route_url)
    # Convert the return value to JSON
    data = result.json()

    # Create a geo json object from the routing data
    geometry = data["routes"][0]["geometry"]
    route_data = Feature(geometry = geometry, properties = {})

    return route_data


def create_stop_locations_details():
    stop_locations = []
    for location in ROUTE:
        # Skip anything that is not a stop location
        if not location["is_stop_location"]:
            continue
        # Create a geojson object for stop location
        point = Point([location['long'], location['lat']])
        properties = {
            'title': location['name'],
            'icon': 'campsite',
            'marker-color': '#3bb2d0',
            'marker-symbol': len(stop_locations) + 1
        }
        feature = Feature(geometry = point, properties = properties)
        stop_locations.append(feature)
    return stop_locations

    

if __name__ == '__main__':
    # run!
    app.run()