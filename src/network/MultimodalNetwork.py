'''
Created on Oct 5, 2019

@author: camila
'''
import networkx as nx

class MultimodalNetwork:
    #modes
    PRIVATE = 1
    PUBLIC = 2
    PEDESTRIAN = 3
    
    #types
    ROAD = 1
    TRANSPORTATION = 2
    TRANSFER = 3
    SUPER_NODE = 4
    
    PEDESTRIAN_SPEED = 4.5 #km/h
    
    MIN_TRANSFER_TIME = 15
    
    PEDESTRIAN_WAYS = {41, 42, 51, 63, 62, 71, 72, 91, 92, 21, 22, 31, 32, 43, 15, 16}
    #PEDESTRIAN_MAPPING_WAYS = {41, 42, 63, 62, 71, 72}
    CAR_WAYS = {11, 12, 13, 14, 15, 16, 21, 22, 31, 32, 41, 42, 43, 51, 63, 71}
    
    def __init__(self):
        self.graph = nx.DiGraph()
        
    def addNode(self, node_id, lat, lon, node_type, stop_id=None, route_id=None):
        if stop_id and route_id:
            self.graph.add_node(node_id, lat=lat, lon=lon, type=node_type, stop_id=stop_id, route_id=route_id)
        else:   
            self.graph.add_node(node_id, lat=lat, lon=lon, type=node_type)
    
    def addEdge(self, node_from, node_to, edge_type, modes, travel_time_functions, original_edge_id=None, edge_position=None):
        if not original_edge_id:
            self.graph.add_edge(node_from, node_to, type=edge_type, modes = modes, travel_time_functions = travel_time_functions)
        else:
            if edge_position:
                self.graph.add_edge(node_from, node_to, type=edge_type, modes = modes, travel_time_functions = travel_time_functions,
                                    original_edge_id=original_edge_id, edge_position=edge_position)
            else:
                self.graph.add_edge(node_from, node_to, type=edge_type, modes = modes, travel_time_functions = travel_time_functions,
                                    original_edge_id=original_edge_id)
    def getNodesIds(self):
        return list(self.graph.nodes)
    
    def getNeighbors(self, node_id):
        return list(self.graph.adj[node_id])
    
    def getTravelTimeToNeighbors(self, node_id, arrival_time, modes):
        neig_travel_times = {}
        try:
            neigs = self.graph[node_id]
            for node_to in neigs:
                edge = neigs[node_to]
                for mode in edge["modes"]:
                    if mode in modes:
                        if node_to in neig_travel_times:
                            cur_travel_time = neig_travel_times[node_to][0]
                            new_travel_time = edge['travel_time_functions'][mode].getTravelTime(arrival_time)
                            if new_travel_time < cur_travel_time:
                                neig_travel_times[node_to] = [new_travel_time, mode]
                        else:
                            neig_travel_times[node_to] = [edge['travel_time_functions'][mode].getTravelTime(arrival_time), mode]
        except (Exception) as error:
            print(error)
        
        return neig_travel_times
    
    def getEdge(self, node_from, node_to):
        if node_from in self.graph:
            if node_to in self.graph[node_from]:
                return self.graph[node_from][node_to]
        return None
    
    def getNode(self, node_id):
        if self.graph.has_node(node_id):
            return self.graph.nodes[node_id]
        return None
    
    def getNumNodes(self):
        return self.graph.number_of_nodes()
    
    def getNumEdges(self):
        return self.graph.number_of_edges()
    
    def getEdges(self):
        return self.graph.edges
    
    def getVertices(self):
        return self.graph.vertices
    
