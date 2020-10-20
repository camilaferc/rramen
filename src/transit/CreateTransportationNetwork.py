'''
Created on Oct 20, 2019

@author: camila
'''
import bisect
import sys

import psycopg2
from psycopg2.sql import Identifier, SQL

from ..database.PostGISConnection import PostGISConnection
from ..database.PostgisDataManager import PostgisDataManager
from ..network.MultimodalNetwork import MultimodalNetwork
from ..travel_time_function.ConstantFunction import ConstantFunction
from ..travel_time_function.TimeTable import TimeTable


class CreateTransportationNetwork:
    
    def __init__(self, region):
        self.region = region
        self.graph = MultimodalNetwork()
        dataManager = PostgisDataManager()
        self.mbr = dataManager.getNetworkMBR(region)
        self.current_node_id = dataManager.getNumNodes(region) + 1
        
        self.calendar = {}
        self.parent_stops={}
        self.stops = {}
        self.routes_id = []
        self.links = {}
        
        self.route_transfers = {}
        self.stop_transfers = {}
        
        self.edges_timetable = {}
        
        self.conn = PostGISConnection()
    
    def run(self):
        self.conn.connect()
        self.load()
        self.createTables()
        self.populateTables()
        self.conn.close()
        
    def createTables(self): 
        self.createTableNodes()  
        self.createTableEdges()
        self.createTableTimetables()
        
    def populateTables(self): 
        self.populateTableNodes() 
        self.populateTableEdgesAndTimetables()
        
    def createTableNodes(self):
        #node_id, lat, lon, node_type, stop_id=None, route_id=None
        sql = """
        CREATE TABLE IF NOT EXISTS {0} (
            node_id bigint NOT NULL,
            lat double precision NOT NULL,
            long double precision NOT NULL,
            type integer NOT NULL,  
            stop_id varchar NULL,  
            route_id varchar NULL,    
            CONSTRAINT {1} PRIMARY KEY (node_id)
        );
        """
        sql = SQL(sql).format(Identifier("transportation_nodes_"+str(self.region)), 
                              Identifier("transportation_nodes_"+str(self.region)+"_pkey"))
        self.conn.executeCommand(sql)
        
    def populateTableNodes(self):
        print("Creating table nodes...")
        sql = """INSERT INTO {0}(node_id, lat, long, type, stop_id, route_id) 
                 VALUES (%s, %s, %s, %s, %s, %s);
            """
        sql = SQL(sql).format(Identifier("transportation_nodes_"+str(self.region)))
        try:
            cursor = self.conn.getCursor()
            for node_id in self.graph.getNodesIds():
                #print(node_id)
                node = self.graph.getNode(node_id)
                if 'type' not in node:
                    continue
                #print(self.region, node_id, node['lat'], node['lon'], node['type'], stop_id, route_id)
                cursor.execute(sql, (node_id, node['lat'], node['lon'], node['type'], node.get('stop_id'), node.get('route_id')))
            self.conn.commit()
            
   
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        
    def createTableEdges(self):
        #node_from, node_to, edge_type, modes, travel_time_functions, original_edge_id=None, edge_position=None
        sql = """
        CREATE TABLE IF NOT EXISTS {0} (
            source bigint NOT NULL,
            target bigint NOT NULL,
            type integer NOT NULL,  
            modes integer[] NOT NULL,
            cost integer NULL,
            timetable_id bigint NULL,
            original_edge_id bigint NULL,  
            edge_position integer NULL,    
            CONSTRAINT {1} PRIMARY KEY (source, target)
        );
        """
        sql = SQL(sql).format(Identifier("transportation_edges_"+str(self.region)),
                              Identifier("transportation_edges_"+str(self.region) + "_pkey"))
        self.conn.executeCommand(sql)
        
    def createTableTimetables(self):
        sql = """
        CREATE TABLE IF NOT EXISTS {0} (
            id bigint NOT NULL,
            monday time[] NULL,
            tuesday time[] NULL,
            wednesday time[] NULL,
            thursday time[] NULL,
            friday time[] NULL,
            saturday time[] NULL,
            sunday time[] NULL,    
            CONSTRAINT {1} PRIMARY KEY (id)
        );
        """
        sql = SQL(sql).format(Identifier("transportation_timetables_"+str(self.region)),
                              Identifier("transportation_timetables_"+str(self.region) + "_pkey"))
        self.conn.executeCommand(sql)
        
    def populateTableEdgesAndTimetables(self):
        print("Creating table edges...")
        sql = """INSERT INTO {0}(source, target, type, modes, cost, timetable_id, original_edge_id, edge_position) 
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
        sql = SQL(sql).format(Identifier("transportation_edges_"+str(self.region)))
        
        sql_timetable = """INSERT INTO {}(id, monday, tuesday, wednesday, thursday, friday, saturday, sunday) 
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
        sql_timetable = SQL(sql_timetable).format(Identifier("transportation_timetables_"+str(self.region)))
        curr_timetable_id = 0;
        try:
            count = 0
            records = []
            cursor = self.conn.getCursor()
            for edge_id in self.graph.getEdges():
                edge = self.graph.getEdge(edge_id[0], edge_id[1])
                
                travel_time_functions = edge['travel_time_functions']
                cost = None
                timetable_id = None
                for mode in travel_time_functions:
                    if mode == self.graph.PUBLIC:
                        timetable_id = curr_timetable_id
                        timetable = travel_time_functions[mode].getTable()
                        cursor.execute(sql_timetable, (timetable_id, timetable.get(0), timetable.get(1), 
                                                       timetable.get(2), timetable.get(3), timetable.get(4), timetable.get(5), timetable.get(6)))
                            
                        curr_timetable_id += 1
                    else:
                        cost = travel_time_functions[mode].getTravelCost()
                    
                records.append((edge_id[0], edge_id[1], edge['type'], list(edge['modes']), cost, timetable_id, 
                                       edge.get('original_edge_id'), edge.get('edge_position')))
                count += 1
                if count % 1000 == 0:
                    #print("Commiting changes...")
                    cursor.executemany(sql, records)
                    self.conn.commit()
                    records = []
                    
            cursor.executemany(sql, records)
            self.conn.commit()
            cursor.close()
   
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        
        
    def load(self):
        print("Loading GTFS data...")
        self.loadCalendar()
        self.loadStops()
        self.loadRoutes()
        self.loadTrips()
        del self.calendar
        del self.routes_id
        del self.edges_timetable
        
        #creating links within stations and to the road network
        self.loadLinks()
        self.loadTransferTimes()
        self.addTransfers()
        
        del self.stops
        del self.parent_stops
        del self.links
        del self.route_transfers
        del self.stop_transfers
        
    def loadCalendar(self):
        print("Loading calendar...")
        sql = """SELECT service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday
        FROM {};
        """
        sql = SQL(sql).format(Identifier("calendar_"+str(self.region)))
        try:
            cursor = self.conn.getCursor()
            cursor.execute(sql)
            
            row = cursor.fetchone()
            
            while row is not None:
                (service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday) = row
                self.calendar[service_id] = {0: monday, 1:tuesday, 2:wednesday, 3: thursday,
                                        4:friday, 5:saturday, 6:sunday}
                row = cursor.fetchone()
            cursor.close()    
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
                
    def loadStops(self):
        print("Loading stops...")
        sql = """SELECT stop_id, stop_lat, stop_lon, stop_type, stop_parent
        FROM {};
        """
        sql = SQL(sql).format(Identifier("stops_"+str(self.region)))
        
        min_lat = float(self.mbr[1])
        min_long = float(self.mbr[0])
        max_lat = float(self.mbr[3])
        max_long = float(self.mbr[2])
        
        try:
            cursor = self.conn.getCursor()
            cursor.execute(sql)
            
            row = cursor.fetchone()
            
            while row is not None:
                (stop_id, stop_lat, stop_lon, stop_type, stop_parent) = row
                if stop_lat >= min_lat and stop_lat <= max_lat and stop_lon >= min_long and stop_lon <= max_long:
                    self.stops[stop_id] = {'lat':stop_lat, 'lon':stop_lon, 'type':stop_type, 'parent':stop_parent}
                
                row = cursor.fetchone()
            cursor.close()
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
    
    def loadLinks(self):
        print("Loading links...")
        sql = """SELECT link_id, edge_id, stop_id, source, target, edge_dist, source_ratio, edge_length, ST_X(point_location), ST_Y(point_location)
        FROM {0};
        """
        sql = SQL(sql).format(Identifier("links_"+str(self.region)))
        
        try:
            cursor = self.conn.getCursor()
            cursor.execute(sql)
            
            row = cursor.fetchone()
            
            #parents = self.parent_stops.keys()
            while row is not None:
                (link_id, edge_id, stop_id, source, target, edge_dist, source_ratio, edge_length, lon, lat) = row
                #if stop_id in parents:
                self.links[stop_id] = {'link_id': link_id, 'edge_id': edge_id, 'source':source, 'target':target, 'edge_dist':edge_dist, 
                                        'source_ratio':source_ratio, 'edge_length':edge_length, 'lat':lat, 'lon':lon}
                row = cursor.fetchone()
            cursor.close()
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
                
    def loadTransferTimes(self):
        if self.parent_stops:
            
            sql_exists = """SELECT EXISTS (
                               SELECT 1
                               FROM   information_schema.tables 
                               WHERE  table_name = %s
                           );
            """
            #sql_exists = SQL(sql_exists).format(Identifier("transfers_"+str(self.region)))
            
            
            try:
                cursor = self.conn.getCursor()
                
                cursor.execute(sql_exists, ('transfers_'+str(self.region), ))
                (exists,) = cursor.fetchone()
                if not exists:
                    print("Table does not exist!")
                    return
                
                print("Loading transfer times...")
                sql = """SELECT from_stop_id, to_stop_id, transfer_type, min_transfer_time, from_route_id, to_route_id, from_trip_id
                FROM {};
                """
                sql = SQL(sql).format(Identifier("transfers_"+str(self.region)))
                cursor.execute(sql)
                
                row = cursor.fetchone()
                
                while row is not None:
                    (from_stop_id, to_stop_id, transfer_type, min_transfer_time, from_route_id, to_route_id, from_trip_id) = row
                    if not min_transfer_time:
                        min_transfer_time = MultimodalNetwork.MIN_TRANSFER_TIME
                    if from_stop_id != to_stop_id and not from_trip_id:
                        if not from_route_id:
                            if from_stop_id in self.stop_transfers:
                                self.stop_transfers[from_stop_id][to_stop_id] = min_transfer_time
                            else:
                                self.stop_transfers[from_stop_id] = {to_stop_id : min_transfer_time}
                        else:
                            if from_route_id in self.route_transfers:
                                if to_route_id in self.route_transfers[from_route_id]:
                                    if from_stop_id in self.route_transfers[from_route_id][to_route_id]:
                                        self.route_transfers[from_route_id][to_route_id][from_stop_id][to_stop_id] = min_transfer_time
                                    else: 
                                        self.route_transfers[from_route_id][to_route_id][from_stop_id] = {to_stop_id:min_transfer_time}
                                else:
                                    self.route_transfers[from_route_id][to_route_id] = {from_stop_id: {to_stop_id:min_transfer_time}}
                            else:
                                self.route_transfers[from_route_id] = {to_route_id : {from_stop_id: {to_stop_id:min_transfer_time}}}
                    row = cursor.fetchone()
                
                cursor.close()   
            except IOError as e:
                print("I/O error({0}): {1}".format(e.errno, e.strerror))
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
            except: 
                print("Unexpected error:", sys.exc_info()[0])
                    
    def loadRoutes(self):
        print("Loading routes...")
        sql = """SELECT route_id FROM {};
        """
        sql = SQL(sql).format(Identifier("routes_"+str(self.region)))
        
        try:
            cursor = self.conn.getCursor()
            cursor.execute(sql)
            
            row = cursor.fetchone()
            
            while row is not None:
                (route_id,) = row
                self.routes_id.append(route_id)
                row = cursor.fetchone()
            
            cursor.close()    
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
                
    def loadTrips(self):
        print("Loading trips...")
        sql = """SELECT s.*, t.service_id FROM {0} as s, {1} as t
                WHERE s.trip_id in
                (select trip_id from {1} where route_id=%s) and
                t.trip_id = s.trip_id
                order by trip_id, stop_sequence
                ;
        """
        sql = SQL(sql).format(Identifier("stop_times_"+str(self.region)), Identifier("trips_"+str(self.region)))
        
        try:
            cursor = self.conn.getCursor()
            
            for route in self.routes_id:
                #print(route)
                self.edges_timetable = {}
                node_mapping = {}
                
                cursor.execute(sql, (route, ))
            
                row = cursor.fetchone()
            
                previous_trip_id = -1
                previous_node = -1
                previous_departure_time = None
                while row is not None:
                    (trip_id, arrival_time, departure_time, stop_id, stop_sequence, service_id) = row
                    #print(trip_id)
                    
                    if trip_id != previous_trip_id:
                        previous_node = -1
                    
                    if stop_id in node_mapping:
                        node_id = node_mapping[stop_id]
                    
                    else:
                        if stop_id not in self.stops:
                            previous_node = -1
                            row = cursor.fetchone()
                            continue
                        
                        node_id = self.current_node_id
                        node_mapping[stop_id] = node_id
                            
                        stop = self.stops[stop_id]
                        if self.graph.getNode(node_id):
                            print("duplicate node!!", node_id)
                        self.graph.addNode(node_id, stop['lat'], stop['lon'], MultimodalNetwork.TRANSPORTATION, stop_id, route)
                        
                        #print(node_id)
                        if self.stops[stop_id]['parent']:
                            parent = self.stops[stop_id]['parent']
                            if parent in self.parent_stops:
                                self.parent_stops[parent].append(node_id)
                            else:
                                self.parent_stops[parent] = [node_id]
                        elif self.stops[stop_id]['type'] == 0 and not self.stops[stop_id]['parent']:
                            self.parent_stops[stop_id] = [node_id]
                            #print(stop_id, node_id)
                            
                        self.current_node_id += 1
                    row = cursor.fetchone()
                    
                    if previous_node != -1:
                        #create edge from previous_node to node_id
                        self.createTransportationEdge(previous_node, node_id, previous_departure_time, arrival_time, service_id)
                    
                    previous_node = node_id
                    previous_trip_id = trip_id
                    previous_departure_time = departure_time
                    
                self.addTransportationEdges()
                    
            cursor.close()    
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
                
    def addTransportationEdges(self): 
        for node_from in self.edges_timetable:
            for node_to in self.edges_timetable[node_from]:
                timetable = self.edges_timetable[node_from][node_to] 
                self.graph.addEdge(node_from, node_to, MultimodalNetwork.TRANSPORTATION, {MultimodalNetwork.PUBLIC}, {MultimodalNetwork.PUBLIC:TimeTable(timetable)})
                #print(self.graph.getEdge(node_from, node_to))
                    
    def createTransportationEdge(self, from_node, to_node, departure_time, arrival_time, service_id):
        #print(str(from_node) + "," + str(to_node))
        service = self.calendar[service_id]
        if from_node in self.edges_timetable:
            if to_node in self.edges_timetable[from_node]:
                timetable = self.edges_timetable[from_node][to_node]
                for day in service:
                    if service[day]:
                        self.addNewDeparture(timetable[day], departure_time, arrival_time)
            else:
                timetable = {}
                for i in range(7):
                    timetable[i] = []
                    
                for day in service:
                    if service[day]:
                        self.addNewDeparture(timetable[day], departure_time, arrival_time)
                        
                self.edges_timetable[from_node][to_node] = timetable
        else:
            self.edges_timetable[from_node] = {}
            timetable = {}
            for i in range(7):
                timetable[i] = []
                
            for day in service:
                if service[day]:
                    self.addNewDeparture(timetable[day], departure_time, arrival_time)
            self.edges_timetable[from_node][to_node] = timetable
    
    def addNewDeparture(self, list_departures, departure_time, arrival_time): 
        if not list_departures: 
            bisect.insort(list_departures, [departure_time, arrival_time])
        else:
            pos = bisect.bisect_left(list_departures, [departure_time, arrival_time])
            if pos == len(list_departures) or list_departures[pos] != [departure_time, arrival_time]:
                bisect.insort(list_departures, [departure_time, arrival_time])
        '''
        i = 0
        while i < len(list_departures):
            if list_departures[i][0] == departure_time:
                break
            elif list_departures[i][0] > departure_time:        
                list_departures.insert(i, [departure_time, arrival_time])  
                break
            else:
                i+=1
        if i == len(list_departures):
            list_departures.append([departure_time, arrival_time])  
    
        '''
    def addTransfers(self):
        print("Adding transfers...")
        for stop in self.links:
            #print(stop)
            #connect the super node to the road network
            stop_id = self.addLinkSuperNodeToRoadNetwork(stop)
            
            if stop in self.parent_stops:
                list_nodes = self.parent_stops[stop]
                #print(stop, list_nodes)
                for node_from in list_nodes:
                    self.addLinkToSuperNode(node_from, stop_id)
                    for node_to in list_nodes:
                        if node_from != node_to:
                            transfer_time = self.getTransferTime(node_from, node_to)
                            #transfer_time = 0
                            self.graph.addEdge(node_from, node_to, MultimodalNetwork.TRANSFER, {MultimodalNetwork.PEDESTRIAN}, 
                                           {MultimodalNetwork.PEDESTRIAN:ConstantFunction(transfer_time)})
    
    def getTransferTime(self, node_from_id, node_to_id):
        node_from = self.graph.getNode(node_from_id)
        node_to = self.graph.getNode(node_to_id)
        
        stop_from = node_from['stop_id']
        stop_to = node_to['stop_id']
        route_from = node_from['route_id']
        route_to = node_to['route_id']
        
        transfer_time_stop = MultimodalNetwork.MIN_TRANSFER_TIME
        if stop_from in self.stop_transfers and stop_to in self.stop_transfers[stop_from]:
            transfer_time_stop = self.stop_transfers[stop_from][stop_to]
                
        if stop_from == stop_to:
            return MultimodalNetwork.MIN_TRANSFER_TIME
        if route_from in self.route_transfers:
            if route_to in self.route_transfers[route_from]:
                if stop_from in self.route_transfers[route_from][route_to] and stop_to in self.route_transfers[route_from][route_to][stop_from]:
                    return self.route_transfers[route_from][route_to][stop_from][stop_to]
                else:
                    return transfer_time_stop
                                
            else:
                return transfer_time_stop
        else:
            return transfer_time_stop
    
    def addLinkSuperNodeToRoadNetwork(self, super_stop):
        #create super stop node
        if super_stop not in self.stops:
            return
        stop_id = self.current_node_id
        self.current_node_id += 1
        stop = self.stops[super_stop]
        if self.graph.getNode(stop_id):
            print("2 duplicate node!!", stop_id)
                            
        self.graph.addNode(stop_id, stop['lat'], stop['lon'], MultimodalNetwork.SUPER_NODE)
        #print("Adding node:" + str(stop_id))
            
        link = self.links[super_stop]
        edge_id = link['edge_id']
        source = link['source']
        target = link['target']
        edge_dist = link['edge_dist']
        edge_time = round((edge_dist/MultimodalNetwork.PEDESTRIAN_SPEED)*3600)
        source_ratio = link['source_ratio']
        
        if source_ratio == 0.0:
            #transfer node is equal to the source
            self.graph.addEdge(stop_id, source, MultimodalNetwork.TRANSFER, {MultimodalNetwork.PEDESTRIAN}, 
                    {MultimodalNetwork.PEDESTRIAN:ConstantFunction(edge_time)})
            self.graph.addEdge(source, stop_id, MultimodalNetwork.TRANSFER, {MultimodalNetwork.PEDESTRIAN}, 
                    {MultimodalNetwork.PEDESTRIAN:ConstantFunction(edge_time)})
            
        elif source_ratio == 1.0:
            #transfer node is equal to the target
            self.graph.addEdge(stop_id, target, MultimodalNetwork.TRANSFER, {MultimodalNetwork.PEDESTRIAN}, 
                    {MultimodalNetwork.PEDESTRIAN:ConstantFunction(edge_time)})
            self.graph.addEdge(target, stop_id, MultimodalNetwork.TRANSFER, {MultimodalNetwork.PEDESTRIAN}, 
                    {MultimodalNetwork.PEDESTRIAN:ConstantFunction(edge_time)})
            
        else:
            node_id = self.current_node_id
            self.current_node_id += 1
            
            if self.graph.getNode(node_id):
                print("3 duplicate node!!", node_id)
                            
            self.graph.addNode(node_id, link['lat'], link['lon'], MultimodalNetwork.TRANSFER)
            
            #splitting edge
            length = link['edge_length']
            time_source = round(((source_ratio*length)/MultimodalNetwork.PEDESTRIAN_SPEED)*3600)
            time_target = round((((1-source_ratio)*length)/MultimodalNetwork.PEDESTRIAN_SPEED)*3600)
            
            self.graph.addEdge(source, node_id, MultimodalNetwork.TRANSFER, {MultimodalNetwork.PEDESTRIAN}, 
                    {MultimodalNetwork.PEDESTRIAN:ConstantFunction(time_source)}, edge_id, 1)
            self.graph.addEdge(node_id, source, MultimodalNetwork.TRANSFER, {MultimodalNetwork.PEDESTRIAN}, 
                    {MultimodalNetwork.PEDESTRIAN:ConstantFunction(time_source)}, edge_id, 1)
            self.graph.addEdge(target, node_id, MultimodalNetwork.TRANSFER, {MultimodalNetwork.PEDESTRIAN}, 
                    {MultimodalNetwork.PEDESTRIAN:ConstantFunction(time_target)},edge_id, 2)
            self.graph.addEdge(node_id, target, MultimodalNetwork.TRANSFER, {MultimodalNetwork.PEDESTRIAN}, 
                    {MultimodalNetwork.PEDESTRIAN:ConstantFunction(time_target)}, edge_id, 2)
            
            #connecting new node to super node
            self.graph.addEdge(stop_id, node_id, MultimodalNetwork.TRANSFER, {MultimodalNetwork.PEDESTRIAN}, 
                    {MultimodalNetwork.PEDESTRIAN:ConstantFunction(edge_time)})
            self.graph.addEdge(node_id, stop_id, MultimodalNetwork.TRANSFER, {MultimodalNetwork.PEDESTRIAN}, 
                    {MultimodalNetwork.PEDESTRIAN:ConstantFunction(edge_time)})
            
            
        return stop_id
        
    def addLinkToSuperNode(self, node_id, parent_node_id):
        self.graph.addEdge(parent_node_id, node_id, MultimodalNetwork.TRANSFER, {MultimodalNetwork.PEDESTRIAN}, 
                    {MultimodalNetwork.PEDESTRIAN:ConstantFunction(0)})
        self.graph.addEdge(node_id, parent_node_id, MultimodalNetwork.TRANSFER, {MultimodalNetwork.PEDESTRIAN}, 
                    {MultimodalNetwork.PEDESTRIAN:ConstantFunction(0)})
        
    '''    
    def addLinkToRoadNetworkSourceTarget(self, node_id, parent):
        link = self.links[parent]
        source = self.osm_mapping[link['source']]
        target = self.osm_mapping[link['target']]
        #print(source, node_id, target)
        source_dist = link['source_dist']
        source_time = round((source_dist/self.graph.PEDESTRIAN_SPEED)*3600)
        target_dist = link['source_dist']
        target_time = round((target_dist/self.graph.PEDESTRIAN_SPEED)*3600)
        
        
        
        #each node is connected to the source and target nodes in the road network
        
        self.graph.addEdge(source, node_id, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                                       {self.graph.PEDESTRIAN:ConstantFunction(source_time)})
        self.graph.addEdge(node_id, source, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                                       {self.graph.PEDESTRIAN:ConstantFunction(source_time)})
        self.graph.addEdge(node_id, target, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                                       {self.graph.PEDESTRIAN:ConstantFunction(target_time)})
        self.graph.addEdge(target, node_id, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                                       {self.graph.PEDESTRIAN:ConstantFunction(target_time)})
    
    '''     