'''
Created on Oct 20, 2019

@author: camila
'''

from database.PostGISConnection import PostGISConnection
import psycopg2
import sys
from travel_time_function.PiecewiseLinearFunction import PiecewiseLinearFunction
from travel_time_function.ConstantFunction import ConstantFunction
from network.MultimodalNetwork import MultimodalNetwork
from _datetime import datetime

class LoadRoadNetwork:
    def __init__(self, region, graph):
        self.region = region
        self.mbr = None
        self.graph = graph
        self.node_id = graph.getNumNodes()
        self.osm_mapping = {}

    def load(self):
        print("Loading road network...")
        sql = """SELECT id, osm_source_id, osm_target_id, clazz, km, kmh, ST_X(source_location), ST_Y(source_location), 
            ST_X(target_location), ST_Y(target_location), reverse_cost
            FROM roadnet_{};
            """
        sql = sql.format(self.region)
        conn = None
        min_lat = sys.float_info.max
        min_long = sys.float_info.max
        max_lat = sys.float_info.min
        max_long = sys.float_info.min
        try:
            conn = PostGISConnection()
            conn.connect()
            cursor = conn.conn.cursor()
            cursor.execute(sql)
            
            row = cursor.fetchone()
            
            duplicated_edges = 0
            
            while row is not None:
                (edge_id, osm_source_id, osm_target_id, clazz, length, speed, x_source, y_source, 
                 x_target, y_target, reverse_cost) = row
                #print(id, osm_source_id, osm_target_id)
                
                min_lat = min(min_lat, y_source)
                max_lat = max(max_lat, y_source)
                min_long = min(min_long, x_source)
                max_long = max(max_long, x_source)
                    
                if osm_source_id not in self.osm_mapping:
                    self.graph.addNode(self.node_id, y_source, x_source, self.graph.ROAD)
                    self.osm_mapping[osm_source_id] = self.node_id
                    self.node_id += 1
                    
                
                if osm_target_id not in self.osm_mapping:
                    self.graph.addNode(self.node_id, y_target, x_target, self.graph.ROAD)
                    self.osm_mapping[osm_target_id] = self.node_id
                    self.node_id += 1
                    
                node_from = self.osm_mapping[osm_source_id]
                node_to = self.osm_mapping[osm_target_id]
                
                modes = set()
                travel_time_functions = {}
                if clazz in self.graph.PEDESTRIAN_WAYS:
                    modes.add(self.graph.PEDESTRIAN)
                    travel_time_functions[self.graph.PEDESTRIAN] = self.generatePedestrianTravelTimeFunction(length, self.graph.PEDESTRIAN_SPEED)
                if clazz in self.graph.CAR_WAYS:
                    modes.add(self.graph.PRIVATE)
                    travel_time_functions[self.graph.PRIVATE] = self.generateTravelTimeFunction(length, speed)
                
                if self.graph.getEdge(node_from, node_to):
                    #print("Edge already exists:" + str(node_from) + "," + str(node_to))
                    if modes != self.graph.getEdge(node_from, node_to)['modes']:
                        self.handleDuplicateEdges(node_from, node_to, modes, travel_time_functions, edge_id)
                else:
                    self.graph.addEdge(node_from, node_to, self.graph.ROAD, modes, travel_time_functions, edge_id)
                
                
                #adding reverse edge
                if reverse_cost < 1000000:
                    if self.graph.getEdge(node_to, node_from):
                        #print("Edge already exists:" + str(node_to) + "," + str(node_from))
                        if modes != self.graph.getEdge(node_to, node_from)['modes']:
                            self.handleDuplicateEdges(node_to, node_from, modes, travel_time_functions, edge_id)
                    else:
                        self.graph.addEdge(node_to, node_from, self.graph.ROAD, modes, travel_time_functions, edge_id)
                else:
                    if clazz in self.graph.PEDESTRIAN_WAYS:
                        #print("Adding extra pedestrian way!")
                        modes = set()
                        travel_time_functions = {}  
                        modes.add(self.graph.PEDESTRIAN)
                        travel_time_functions[self.graph.PEDESTRIAN] = self.generatePedestrianTravelTimeFunction(length, self.graph.PEDESTRIAN_SPEED)
                        self.graph.addEdge(node_to, node_from, self.graph.ROAD, modes, travel_time_functions, edge_id)
                    
                row = cursor.fetchone()
            self.mbr = (min_lat, min_long, max_lat, max_long)   
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if conn is not None:
                conn.close()
        #self.generateExtraPedestrianEdges(self.graph)
        
    def handleDuplicateEdges(self, node_from, node_to, modes, travel_time_functions, edge_id):
        edge = self.graph.getEdge(node_from, node_to)
        if len(edge['modes']) < len(modes):
            edge['modes'] = modes
            edge['travel_time_functions'] = travel_time_functions
            edge['original_edge_id'] = edge_id
        elif len(edge['modes']) == len(modes):
            for m in modes:
                edge['modes'].add(m) 
                edge['travel_time_functions'][m] = travel_time_functions[m]
            
    '''
    def generateExtraPedestrianEdges(self, graph):
        for edge in graph.getEdges():
            node_from = edge[0]
            node_to = edge[1]
            
            if not graph.getEdge(node_to, node_from):
                edge_rev =  graph.getEdge(node_from, node_to)
                graph.addEdge(node_to, node_from, graph.ROAD, {graph.PEDESTRIAN}, {graph.PEDESTRIAN:edge_rev['travel_time_functions'][graph.PEDESTRIAN]},
                              edge_rev['original_edge_id'])
    '''
                                   
    def generateTravelTimeFunction(self, length, speed):
        list_speeds = {0:1, 1:1, 2:1, 3:1, 4:1, 5:1, 6:0.8, 7:0.4, 8:0.4, 9:0.4, 10:0.8, 11:0.8, 12:0.6, 
                       13:0.8, 14:0.8, 15:0.8, 16:0.6, 17:0.3, 18:0.3, 19:0.4, 20:0.7, 21: 0.8, 22:0.9, 23:1}
        x = []
        y = []
        for i in range(24):
            current_speed = speed*list_speeds[i]
            travel_time = length/current_speed
            travel_time = travel_time*3600
            x.append(i*3600)
            y.append(round(travel_time))
        return PiecewiseLinearFunction(x, y)
        '''
        travel_times = []
        for i in range(24):
            current_speed = speed*list_speeds[i]
            travel_time = length/current_speed
            travel_time = travel_time*3600
            travel_times.append([i*3600, round(travel_time)])
        return PiecewiseLinearFunction(travel_times)
        '''
        
    def generatePedestrianTravelTimeFunction(self, length, speed):
        travel_time = length/speed
        travel_time = travel_time*3600
        return ConstantFunction(round(travel_time))
    
    def getMbr(self):
        return self.mbr
    
    def getOsmMapping(self):
        return self.osm_mapping
        
    