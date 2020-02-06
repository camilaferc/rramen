'''
Created on Oct 20, 2019

@author: camila
'''

from database.PostGISConnection import PostGISConnection
import psycopg2
import sys
from travel_time_function.PiecewiseLinearFunction import PiecewiseLinearFunction
from travel_time_function.ConstantFunction import ConstantFunction

import time

class LoadRoadNetwork:
    def __init__(self, region, graph):
        self.region = region
        self.graph = graph

    def load(self):
        print("Loading road network...")
        sql = """
            SELECT r.id, r.source, r.target, clazz, km, kmh, ST_X(source_location), ST_Y(source_location), 
            ST_X(target_location), ST_Y(target_location), reverse_cost
            FROM roadnet_{0} r
            ;
            """
        sql = sql.format(self.region)
        conn = None
        try:
            start_time = time.time()
            conn = PostGISConnection()
            conn.connect()
            cursor = conn.conn.cursor()
            cursor.execute(sql)
            total_time = time.time() - start_time
            print(total_time)
            
            row = cursor.fetchone()
            
            time_nodes = 0
            time_edges = 0
            time_functions = 0
            
            while row is not None:
                (edge_id, source_id, target_id, clazz, length, speed, x_source, y_source, 
                 x_target, y_target, reverse_cost) = row
                #print(edge_id, source_id, target_id)
                
                start_time_node = time.time()
                if not self.graph.getNode(source_id):
                    self.graph.addNode(source_id, y_source, x_source, self.graph.ROAD)
                    #print(source_id)
                    
                if not self.graph.getNode(target_id):
                    self.graph.addNode(target_id, y_target, x_target, self.graph.ROAD)
                    #print(target_id)
                    
                total_time_node = time.time() - start_time_node
                time_nodes += total_time_node
                
                start_time_edge = time.time()
                modes = set()
                travel_time_functions = {}
                original_edge_ids = {}
                if clazz in self.graph.PEDESTRIAN_WAYS:
                    modes.add(self.graph.PEDESTRIAN)
                    travel_time_functions[self.graph.PEDESTRIAN] = self.generatePedestrianTravelTimeFunction(length, self.graph.PEDESTRIAN_SPEED)
                    original_edge_ids[self.graph.PEDESTRIAN] = edge_id
                if clazz in self.graph.CAR_WAYS:
                    modes.add(self.graph.PRIVATE)
                    original_edge_ids[self.graph.PRIVATE] = edge_id
                    start_time_function = time.time()
                    travel_time_functions[self.graph.PRIVATE] = self.generateTravelTimeFunction(length, speed)
                    total_time_function = time.time() - start_time_function
                    time_functions += total_time_function
                
                edge_dup = self.graph.getEdge(source_id, target_id)
                if edge_dup:
                    #print("Edge already exists:" + str(source_id) + "," + str(target_id))
                    self.handleDuplicateEdges(edge_dup, modes, travel_time_functions, edge_id)
                else:
                    self.graph.addEdge(source_id, target_id, self.graph.ROAD, modes, travel_time_functions, original_edge_ids)
                
                
                #adding reverse edge
                if reverse_cost < 1000000:
                    rev_edge_dup = self.graph.getEdge(target_id, source_id)
                    if rev_edge_dup:
                        #print("Edge already exists:" + str(target_id) + "," + str(source_id))
                        #print(osm_source_id, osm_target_id)
                        #print(travel_time_functions)
                        #print(self.graph.getEdge(node_from, node_to)['travel_time_functions'])
                        self.handleDuplicateEdges(rev_edge_dup, modes, travel_time_functions, edge_id)
                    else:
                        self.graph.addEdge(target_id, source_id, self.graph.ROAD, modes, travel_time_functions, original_edge_ids)
                else:
                    #Adding extra pedestrian way
                    if clazz in self.graph.PEDESTRIAN_WAYS:
                        modes = set()
                        travel_time_functions = {} 
                        original_edge_ids = {} 
                        modes.add(self.graph.PEDESTRIAN)
                        travel_time_functions[self.graph.PEDESTRIAN] = self.generatePedestrianTravelTimeFunction(length, self.graph.PEDESTRIAN_SPEED)
                        original_edge_ids[self.graph.PEDESTRIAN] = edge_id
                        self.graph.addEdge(target_id, source_id, self.graph.ROAD, modes, travel_time_functions, original_edge_ids)
                        
                total_time_edge = time.time() - start_time_edge
                time_edges += total_time_edge
                    
                row = cursor.fetchone()
            total_time = time.time() - start_time
            print(total_time)
            print(time_nodes)
            print(time_edges)
            print(time_functions)
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
    '''    
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
    
    def handleDuplicateEdges(self, edge, modes, travel_time_functions, edge_id):
        edge_modes = edge['modes']
        edge_functions = edge['travel_time_functions']
        edge_original_id = edge['original_edge_id']
        for mode in edge_modes:
            if mode in modes:
                comp = edge_functions[mode].comp(travel_time_functions[mode])
                if comp == 1:
                    edge_functions[mode] = travel_time_functions[mode]
                    edge_original_id[mode] = edge_id
                    #print(edge)
                    
        for mode in modes:
            if mode not in edge_modes:
                edge_functions[mode] = travel_time_functions[mode]
                edge_original_id[mode] = edge_id
                edge_modes.add(mode)
                #print(edge)
            
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
        
    def generatePedestrianTravelTimeFunction(self, length, speed):
        travel_time = length/speed
        travel_time = travel_time*3600
        return ConstantFunction(round(travel_time))
    
    def getMbr(self):
        return self.mbr
        
    