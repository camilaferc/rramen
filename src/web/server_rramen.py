'''
Created on Oct 29, 2019

@author: camila
'''

from _datetime import datetime
from builtins import str
import pathlib
import sys
from threading import Thread

from flask import Flask, request, render_template
from flask_iniconfig import INIConfig
from geojson import Feature, Point, FeatureCollection

from ..database.PostgisDataManager import PostgisDataManager
from ..gtfs import GTFS
from ..load.LoadMultimodalNetwork import LoadMultimodalNetwork
from ..path.Path import Path
from ..shortest_path.Dijkstra import Dijsktra

app = Flask(__name__)
INIConfig(app)
app.config.from_inifile(str(pathlib.Path(__file__).resolve().parents[2]) + '/config.ini')

MAPBOX_ACCESS_KEY = (app.config.get('mapbox') or {}).get('MAPBOX_ACCESS_KEY')
if not MAPBOX_ACCESS_KEY:
    raise SystemExit("No mapbox token provided.")

region = (app.config.get('map') or {}).get('region')
if not region:
    raise SystemExit("No region provided.")

dataManager = PostgisDataManager()

def loadData():
    global graph
    load = LoadMultimodalNetwork(region)
    graph = load.load()

loadData()

@app.route('/')
def rramen():

    polygons = dataManager.getNeighborhoodsPolygons(region)
    routes, stops, stop_routes, stop_level = dataManager.getRoutes(region)
    stop_locations = dataManager.getStopsLocation(region, stop_level)

    network_center = dataManager.getNetworkCenter(region)

    return render_template('rramen.html',
        ACCESS_KEY=MAPBOX_ACCESS_KEY,
        polygons = polygons,
        routes = routes,
        stops = stops,
        stop_routes = stop_routes,
        transp_mapping = GTFS.ROUTE_TYPE,
        stop_locations = stop_locations,
        network_center = network_center
    )

@app.route('/receiver', methods = ['POST', 'GET'])
def worker():
    # read json + reply
    global graph, dataManager, region
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
    selected_neighborhoods = data['selected_neighborhoods']

    removed_routes = None
    removed_stops = None
    removed_segments = None
    
    target_markers = False

    if "removed_routes" in data and data["removed_routes"]:
        removed_routes = data["removed_routes"]

    if "removed_stops" in data and data["removed_stops"]:
        removed_stops = getRemovedStops(data["removed_stops"])

    if "removed_segments" in data and data["removed_segments"]:
        removed_segments = {int(old_key): set(val) for old_key, val in data["removed_segments"].items()}

    if sources_coordinates:
        #Source is a set of markers
        sources_private, sources_public = getNodesFromMarkersCoordinates(sources_coordinates, "source", id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public , map_source_coord, map_target_coord)
    elif polygon_source_coords:
        #Source is a polygon"
        sources_private, sources_public = getNodesWithinPolygon(polygon_source_coords, "source", id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public, map_source_coord, map_target_coord)

    if targets_coordinates:
        #Target is a set of markers
        target_markers = True
        targets_private, targets_public = getNodesFromMarkersCoordinates(targets_coordinates, "target", id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public , map_source_coord, map_target_coord)
    elif polygon_target_coords:
        #Target is a polygon
        targets_private, targets_public = getNodesWithinPolygon(polygon_target_coords, "target", id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public, map_source_coord, map_target_coord)
    elif selected_neighborhoods:
        #Target is a neighborhood
        targets_private, targets_public = getNodesWithinNeighborhoods(selected_neighborhoods, "target", id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public, map_source_coord, map_target_coord)

    time = data['timestamp']
    timestamp = datetime.fromtimestamp(time/1000)

    node_colors, colored_type, path_markers = computeRelativeReachability(target_markers, sources_public, targets_public, sources_private, targets_private, id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public,
                                                                                   timestamp, removed_routes, removed_stops, removed_segments)
    if colored_type == "source":
        map_coord = map_source_coord

    else:
        map_coord = map_target_coord

    markers = []
    allColored = True
    for n in map_coord:
        if n not in node_colors:
            allColored = False
            continue
        n_coord = map_coord[n]
        point = Point([n_coord[1], n_coord[0]])
        properties = {'marker-color': node_colors[n]}
        feature = Feature(geometry = point, properties = properties)
        markers.append(feature)

    gc = FeatureCollection(markers)
    return {"geom": gc, "allColored": allColored, "pathGeom": path_markers}


def getNodesFromMarkersCoordinates(map_coordinates, location_type, id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public, map_source_coord, map_target_coord):
    nodes_private = set()
    nodes_public = set()
    for i in map_coordinates:
        c = map_coordinates[i]

        i = int(i)

        node_id_private = dataManager.getClosestVertex(c['lat'], c['lon'], region)
        # node_id_private = createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, source, target, source_ratio)
        nodes_private.add(node_id_private)
        if location_type == "source":
            id_map_source_private[i] = node_id_private
            map_source_coord[i] = (c['lat'], c['lon'])
        else:
            id_map_target_private[i] = node_id_private
            map_target_coord[i] = (c['lat'], c['lon'])


        node_id_public  = dataManager.getClosestVertex_CLASSDEPENDANT(c['lat'], c['lon'], region, graph.PEDESTRIAN_WAYS)
        # node_id_public = createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, source, target, source_ratio)
        nodes_public.add(node_id_public)
        if location_type == "source":
            id_map_source_public[i] = node_id_public
            map_source_coord[i] = (c['lat'], c['lon'])

        else:
            id_map_target_public[i] = node_id_public
            map_target_coord[i] = (c['lat'], c['lon'])

    return nodes_private, nodes_public

def getNodesWithinPolygon(polygon_coordinates, location_type, id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public, map_source_coord, map_target_coord):

    polygon_points = dataManager.getPointsWithinPolygon(region, polygon_coordinates)

    id_map_public = {}
    id_map_private = {}
    
    if location_type == "source":
        map_coord = map_source_coord
        id_map_public = id_map_source_public
        id_map_private = id_map_source_private
    else:
        map_coord = map_target_coord
        id_map_public = id_map_target_public
        id_map_private = id_map_target_private

    nodes = set()
    i = 0
    for node_id in polygon_points:
        nodes.add(node_id)
        node = graph.getNode(node_id)

        map_coord[i] = (node['lat'], node['lon'])

        id_map_public[i] = node_id
        id_map_private[i] = node_id
        i+=1

    return nodes, nodes


def getNodesWithinNeighborhoods(selected_neighborhoods, location_type, id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public, map_source_coord, map_target_coord):
    neig_points = dataManager.getPointsWithinNeighborhoods(region, selected_neighborhoods)
    id_map_public = {}
    id_map_private = {}
    
    if location_type == "source":
        map_coord = map_source_coord
        id_map_public = id_map_source_public
        id_map_private = id_map_source_private
    else:
        map_coord = map_target_coord
        id_map_public = id_map_target_public
        id_map_private = id_map_target_private

    nodes = set()
    i = 0
    for node_id in neig_points:
        nodes.add(node_id)
        node = graph.getNode(node_id)

        map_coord[i] = (node['lat'], node['lon'])

        id_map_public[i] = node_id
        id_map_private[i] = node_id
        i+=1

    return nodes, nodes


@app.route('/route', methods = ['POST', 'GET'])
def getRouteGeometry():
    global dataManager
    data = request.get_json(force=True)

    route_name = data['route_name']
    transp_id = data['transp_id']
    route_geom = dataManager.getRouteGeometry(route_name, transp_id, region)

    fc = FeatureCollection(route_geom)

    res = {"route_geom": fc}
    return res

#@app.route('/path', methods = ['POST', 'GET'])
def getPathGeometry(id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public, tt_public, tt_private, parent_tree_public, parent_tree_private):
    global graph
    markers = {}
    # data = request.get_json(force=True)
    # marker_id = int(data['id'])
    # location_type = data['location_type']

    for marker_id in id_map_source_public:
        for target_id in id_map_target_public:
            res_none = {"path_geom": None, "tt_public": None, "tt_private": None}

            # if location_type == "source":
            #     if len(id_map_source_public) <= 1 or len(id_map_target_public) > 1:
            #         markers[marker_id] = {target_id : res_none}

            source_public = id_map_source_public[marker_id]
            source_private = id_map_source_private[marker_id]

                # target_id = None
                # for key in id_map_target_public:
                #     target_id = key

            target_public = id_map_target_public[target_id]
            target_private = id_map_target_private[target_id]
            # else:
            #     if len(id_map_source_public) > 1:
            #         res = {"path_geom": -1, "tt_public": -1, "tt_private": -1}
            #         markers[marker_id] = {target_id : res}
            #
            #
            #     if len(id_map_source_public) > 1 and len(id_map_target_public) == 1:
            #         markers[marker_id] = {target_id : res_none}
            #
            #     source_id = None
            #     for key in id_map_source_public:
            #         source_id = key
            #
            #     source_public = id_map_source_public[source_id]
            #     source_private = id_map_source_private[source_id]
            #
            #     node_id_public = id_map_target_public[marker_id]
            #     node_id_private = id_map_target_private[marker_id]

            paths = []
            pathPublic = Path(parent_tree_public[source_public])
            pathPublicGeom = pathPublic.getPathGeometry(graph, target_public, region)
            if pathPublicGeom is None:
                if marker_id in markers:
                    markers[marker_id][target_id] = res_none
                else:
                    markers[marker_id] = {target_id: res_none}
                continue

            properties = {'line-color': 'b'}
            feature = Feature(geometry = pathPublicGeom, properties = properties)
            paths.append(feature)

            pathPrivate = Path(parent_tree_private[source_private])

            pathPrivateGeom = pathPrivate.getPathGeometry(graph, target_private, region)
            if pathPrivateGeom is None:
                if marker_id in markers:
                    markers[marker_id][target_id] = res_none
                else:
                    markers[marker_id] = {target_id: res_none}
                continue
            properties = {'line-color': 'r'}
            feature = Feature(geometry = pathPrivateGeom, properties = properties)
            paths.append(feature)

            tt_node_public = round(tt_public[source_public][target_public]/60)
            tt_node_private = round(tt_private[source_private][target_private]/60)

            fc = FeatureCollection(paths)
            res = {"path_geom": fc, "tt_public": tt_node_public, "tt_private": tt_node_private}
            if marker_id in markers:
                markers[marker_id][target_id] = res
            else:
                markers[marker_id] = {target_id: res}

    return markers

@app.route('/segment', methods = ['POST', 'GET'])
def getSegmentGeometry():
    data = request.get_json(force=True)

    lat = data['latitude']
    lon = data['longitude']

    edge_id, source, target, geometry = dataManager.getClosestEdgeGeometry(lat, lon, region)

    feature = Feature(geometry = geometry)

    res = {"segment": feature, "segment_id":edge_id, "source":source, "target":target}
    return res


def computeRelativeReachability(markers, sources_public, targets_public, sources_private, targets_private, id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public, timestamp,
                                removed_routes=None, removed_stops=None, removed_segments=None):
    dij = Dijsktra(graph)
    parent_tree_public = {}
    parent_tree_private = {}
    threads = []


    # We start one thread per Dijkstra call.
    tt_public = {}
    parent_tree_public = {}
    process_public = Thread(target=dij.manyToManyPublic, args=[sources_public, targets_public, timestamp, removed_routes,removed_stops,
                                                              tt_public, parent_tree_public])
    process_public.start()
    threads.append(process_public)


    tt_private = {}
    parent_tree_private = {}
    process_private = Thread(target=dij.manyToManyPrivate, args=[sources_private, targets_private, timestamp, removed_segments,tt_private,parent_tree_private])
    process_private.start()
    threads.append(process_private)

    # We now pause execution on the main thread by 'joining' all of our started threads.
    process_public.join()
    process_private.join()

    node_colors, colored_type = getNodesColors(tt_public, tt_private, id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public)
    path_markers = None
    if markers:
        path_markers = getPathGeometry(id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public, tt_public, tt_private, parent_tree_public, parent_tree_private)
    return node_colors, colored_type, path_markers


def getRemovedStops(map_stops):
    map_stops_children = {}
    for stop in map_stops:
        routes = map_stops[stop]
        children = dataManager.getChildrenStops(region, stop)
        if not children:
            map_stops_children[stop] = set(routes)
        else:
            for c in children:
                if c in map_stops_children:
                    map_stops_children[c].update(routes)
                else:
                    map_stops_children[c] = set(routes)
    return map_stops_children

def getNodesColors(tt_public, tt_private, id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public):
    colored_type = "target"
    if len(id_map_source_public) > 1 and len(id_map_target_public) == 1:
        colored_type = "source"

    if colored_type == "source":
        node_colors = colorSources(tt_public, tt_private, id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public)
        return node_colors, colored_type
    else:
        node_colors = colorTargets(tt_public, tt_private, id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public)
        return node_colors, colored_type

def colorSources(tt_public, tt_private, id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public):

    node_colors = {}
    for i in id_map_source_private:
        s_public = id_map_source_public[i]
        s_private = id_map_source_private[i]

        if s_public in tt_public and s_private in tt_private:
            tt_public_s = tt_public[s_public]
            tt_private_s = tt_private[s_private]
            num_public = 0
            num_total = 0
            for j in id_map_target_private:
                t_public = id_map_target_public[j]
                t_private = id_map_target_private[j]

                if t_public in tt_public_s and t_private in tt_private_s:
                    if tt_public_s[t_public] <=  tt_private_s[t_private]:
                        num_public += 1
                    num_total += 1
            if num_total > 0:
                node_colors[i] = float(num_public/num_total)
    return node_colors

def colorTargets(tt_public, tt_private, id_map_source_public, id_map_source_private, id_map_target_private, id_map_target_public):
    node_colors = {}
    targets = {}
    for i in id_map_source_private:
        for i in id_map_source_private:
            s_public = id_map_source_public[i]
            s_private = id_map_source_private[i]
            s_private = id_map_source_private[i]
            if s_public in tt_public and s_private in tt_private:
                tt_public_s = tt_public[s_public]
                tt_private_s =tt_private[s_private]
                for j in id_map_target_private:
                    t_public = id_map_target_public[j]
                    t_private = id_map_target_private[j]
                    num_public = 0
                    if t_public in tt_public_s and t_private in tt_private_s:
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


def createVirtualNodeEdge(graph, node_lat, node_lon, edge_id, source, target, source_ratio):
    if source_ratio == 0.0:
        # node is equal to the source
        return source
    elif source_ratio == 1.0:
        #node is equal to the target
        return target
    else:
        node_id = graph.getNumNodes() + 1
        graph.addNode(node_id, node_lat, node_lon, graph.ROAD)

        #splitting edge
        original_edge = graph.getEdge(source, target)

        modes = original_edge['modes']
        left_functions = {}
        right_functions = {}

        for mode in modes:

            function = original_edge['travel_time_functions'][mode]
            left, right = function.splitFunctionRatio(source_ratio)

            left_functions[mode] = left
            right_functions[mode] = right

        graph.addEdge(source, node_id, graph.ROAD, modes, left_functions, edge_id, 1)
        graph.addEdge(node_id, target, graph.ROAD, modes, right_functions, edge_id, 2)

        edge_rev = graph.getEdge(target, source)
        if edge_rev:
            modes_rev = edge_rev['modes']
            if modes_rev == modes:
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
                        function = edge_rev['travel_time_functions'][mode]
                        left, right = function.splitFunctionRatio(source_ratio)
                        left_functions[mode] = left
                        right_functions[mode] = right
                graph.addEdge(node_id, source, graph.ROAD, modes_rev, left_functions_rev, edge_id, 1)
                graph.addEdge(target, node_id, graph.ROAD, modes_rev, right_functions_rev, edge_id, 2)
        return node_id


if __name__ == '__main__':
    app.run(threaded=True)
