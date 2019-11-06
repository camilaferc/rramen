'''
Created on Oct 20, 2019

@author: camila
'''
from database.PostGISConnection import PostGISConnection
import psycopg2
import sys
import bisect
from travel_time_function.ConstantFunction import ConstantFunction
from travel_time_function.TimeTable import TimeTable

class LoadTransportationNetwork:
    
    def __init__(self, region, graph, mbr):
        self.region = region
        self.graph = graph
        self.osm_mapping = graph.getOsmMapping()
        self.mbr = mbr
        
        self.calendar = {}
        self.parent_stops={}
        self.stops = {}
        self.routes_id = []
        self.links = {}
        
        self.route_transfers = {}
        self.stop_transfers = {}
        
        self.current_node_id = graph.getNumNodes()
        self.edges_timetable = {}
        
    def load(self):
        print("Loading GTFS data...")
        self.loadCalendar()
        self.loadStops()
        self.loadRoutes()
        self.loadTrips()
        del self.calendar
        del self.routes_id
        del self.edges_timetable
        
        #print(self.graph.getNumNodes())
        #print(self.graph.getNumEdges())
        
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
        FROM calendar_{};
        """
        sql = sql.format(self.region)
        conn = None
        
        try:
            conn = PostGISConnection()
            conn.connect()
            cursor = conn.conn.cursor()
            cursor.execute(sql)
            
            row = cursor.fetchone()
            
            while row is not None:
                (service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday) = row
                self.calendar[service_id] = {0: monday, 1:tuesday, 2:wednesday, 3: thursday,
                                        4:friday, 5:saturday, 6:sunday}
                row = cursor.fetchone()
                
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if conn is not None:
                conn.close()
                
    def loadStops(self):
        print("Loading stops...")
        sql = """SELECT stop_id, stop_lat, stop_lon, stop_type, stop_parent
        FROM stops_{};
        """
        sql = sql.format(self.region)
        conn = None
        min_lat = self.mbr[0]
        min_long = self.mbr[1]
        max_lat = self.mbr[2]
        max_long = self.mbr[3]
        
        try:
            conn = PostGISConnection()
            conn.connect()
            cursor = conn.conn.cursor()
            cursor.execute(sql)
            
            row = cursor.fetchone()
            
            count = 0
            while row is not None:
                (stop_id, stop_lat, stop_lon, stop_type, stop_parent) = row
                if stop_lat >= min_lat and stop_lat <= max_lat and stop_lon >= min_long and stop_lon <= max_long:
                    self.stops[stop_id] = {'lat':stop_lat, 'lon':stop_lon, 'type':stop_type, 'parent':stop_parent}
                else:
                    #print(str(stop_id) + " outside the mbr!")
                    count += 1
                
                row = cursor.fetchone()
            print(count)
                
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if conn is not None:
                conn.close()
    
    def loadLinks(self):
        print("Loading links...")
        sql = """SELECT link_id, stop_id, osm_source, osm_target, edge_dist, source_ratio, edge_length, ST_X(point_location), ST_Y(point_location)
        FROM links_{};
        """
        sql = sql.format(self.region)
        conn = None
        
        try:
            conn = PostGISConnection()
            conn.connect()
            cursor = conn.conn.cursor()
            cursor.execute(sql)
            
            row = cursor.fetchone()
            
            parents = self.parent_stops.keys()
            while row is not None:
                (link_id, stop_id, osm_source, osm_target, edge_dist, source_ratio, edge_length, lon, lat) = row
                if stop_id in parents:
                    self.links[stop_id] = {'link_id': link_id, 'source':osm_source, 'target':osm_target, 'edge_dist':edge_dist, 
                                           'source_ratio':source_ratio, 'edge_length':edge_length, 'lat':lat, 'lon':lon}
                row = cursor.fetchone()
                
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if conn is not None:
                conn.close()
                
    def loadTransferTimes(self):
        print("Loading transfer times...")
        sql = """SELECT from_stop_id, to_stop_id, transfer_type, min_transfer_time, from_route_id, to_route_id, from_trip_id
        FROM transfers_{};
        """
        sql = sql.format(self.region)
        conn = None
        
        try:
            conn = PostGISConnection()
            conn.connect()
            cursor = conn.conn.cursor()
            cursor.execute(sql)
            
            row = cursor.fetchone()
            
            while row is not None:
                (from_stop_id, to_stop_id, transfer_type, min_transfer_time, from_route_id, to_route_id, from_trip_id) = row
                if not min_transfer_time:
                        min_transfer_time = self.graph.MIN_TRANSFER_TIME
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
            
            #for stop in self.transfer_times:
            #    print(self.transfer_times[stop])    
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if conn is not None:
                conn.close()
    
                    
    def loadRoutes(self):
        print("Loading routes...")
        sql = """SELECT route_id FROM routes_{};
        """
        sql = sql.format(self.region)
        conn = None
        
        try:
            conn = PostGISConnection()
            conn.connect()
            cursor = conn.conn.cursor()
            cursor.execute(sql)
            
            row = cursor.fetchone()
            
            while row is not None:
                (route_id,) = row
                self.routes_id.append(route_id)
                row = cursor.fetchone()
                
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if conn is not None:
                conn.close()
                
    def loadTrips(self):
        print("Loading trips...")
        sql = """SELECT s.*, t.service_id FROM stop_times_{0} as s, trips_{0} as t
                WHERE s.trip_id in
                (select trip_id from trips_berlin where route_id='{1}') and
                t.trip_id = s.trip_id
                order by trip_id, stop_sequence
                ;
        """
        conn = None
        
        try:
            conn = PostGISConnection()
            conn.connect()
            cursor = conn.conn.cursor()
            
            for route in self.routes_id:
                #print(route)
                self.edges_timetable = {}
                node_mapping = {}
                
                sql_route = sql.format(self.region, route)
                cursor.execute(sql_route)
            
                row = cursor.fetchone()
            
                previous_trip_id = -1
                previous_node = -1
                previous_departure_time = None
                while row is not None:
                    (trip_id, arrival_time, departure_time, stop_id, stop_sequence, service_id) = row
                    
                    if trip_id != previous_trip_id:
                        previous_node = -1
                    
                    if stop_id in node_mapping:
                        node_id = node_mapping[stop_id]
                    else:
                        if stop_id not in self.stops:
                            #print(str(stop_id) + " not valid")
                            previous_node = -1
                            row = cursor.fetchone()
                            continue
                        
                        node_id = self.current_node_id
                        node_mapping[stop_id] = node_id
                            
                        stop = self.stops[stop_id]
                        self.graph.addNode(node_id, stop['lat'], stop['lon'], self.graph.TRANSPORTATION, stop_id, route)
                        
                        #print(node_id)
                        if self.stops[stop_id]['parent']:
                            parent = self.stops[stop_id]['parent']
                            if parent in self.parent_stops:
                                self.parent_stops[parent].append(node_id)
                            else:
                                self.parent_stops[parent] = [node_id]
                            
                        self.current_node_id += 1
                    row = cursor.fetchone()
                    
                    if previous_node != -1:
                        #create edge from previous_node to node_id
                        self.createTransportationEdge(previous_node, node_id, previous_departure_time, arrival_time, service_id)
                    
                    previous_node = node_id
                    previous_trip_id = trip_id
                    previous_departure_time = departure_time
                    
                #count += 1
                #if count == 1:
                #    break;
                self.addTransportationEdges()
                    
                
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if conn is not None:
                conn.close()
                
    def addTransportationEdges(self): 
        for node_from in self.edges_timetable:
            for node_to in self.edges_timetable[node_from]:
                timetable = self.edges_timetable[node_from][node_to] 
                self.graph.addEdge(node_from, node_to, self.graph.TRANSPORTATION, {self.graph.PUBLIC}, {self.graph.PUBLIC:TimeTable(timetable)})
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
        for parent in self.parent_stops:
            #connect the super node (parent) to the road network
            parent_id = self.addLinkSuperNodeToRoadNetwork(parent)
            
            list_nodes = self.parent_stops[parent]
            #print(parent, len(list_nodes))
            for node_from in list_nodes:
                self.addLinkToSuperNode(node_from, parent_id)
                for node_to in list_nodes:
                    if node_from != node_to:
                        transfer_time = self.getTransferTime(node_from, node_to)
                        #transfer_time = 0
                        self.graph.addEdge(node_from, node_to, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                                       {self.graph.PEDESTRIAN:ConstantFunction(transfer_time)})
    
    def getTransferTime(self, node_from_id, node_to_id):
        node_from = self.graph.getNode(node_from_id)
        node_to = self.graph.getNode(node_to_id)
        
        stop_from = node_from['stop_id']
        stop_to = node_to['stop_id']
        route_from = node_from['route_id']
        route_to = node_to['route_id']
        
        transfer_time_stop = self.graph.MIN_TRANSFER_TIME
        if stop_from in self.stop_transfers and stop_to in self.stop_transfers[stop_from]:
            transfer_time_stop = self.stop_transfers[stop_from][stop_to]
                
        if stop_from == stop_to:
            return self.graph.MIN_TRANSFER_TIME
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
    
    def addLinkSuperNodeToRoadNetwork(self, parent_stop):
        #create parent node
        parent_id = self.current_node_id
        self.current_node_id += 1
        stop = self.stops[parent_stop]
        self.graph.addNode(parent_id, stop['lat'], stop['lon'], self.graph.SUPER_NODE)
            
        link = self.links[parent_stop]
        edge_id = link['link_id']
        source = self.osm_mapping[link['source']]
        target = self.osm_mapping[link['target']]
        edge_dist = link['edge_dist']
        edge_time = round((edge_dist/self.graph.PEDESTRIAN_SPEED)*3600)
        source_ratio = link['source_ratio']
        
        if source_ratio == 0.0:
            #transfer node is equal to the source
            self.graph.addEdge(parent_id, source, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                    {self.graph.PEDESTRIAN:ConstantFunction(edge_time)})
            self.graph.addEdge(source, parent_id, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                    {self.graph.PEDESTRIAN:ConstantFunction(edge_time)})
        elif source_ratio == 1.0:
            #transfer node is equal to the target
            self.graph.addEdge(parent_id, target, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                    {self.graph.PEDESTRIAN:ConstantFunction(edge_time)})
            self.graph.addEdge(target, parent_id, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                    {self.graph.PEDESTRIAN:ConstantFunction(edge_time)})
        else:
            node_id = self.current_node_id
            self.current_node_id += 1
            self.graph.addNode(node_id, link['lat'], link['lon'], self.graph.TRANSFER)
            
            #splitting edge
            length = link['edge_length']
            time_source = round(((source_ratio*length)/self.graph.PEDESTRIAN_SPEED)*3600)
            time_target = round((((1-source_ratio)*length)/self.graph.PEDESTRIAN_SPEED)*3600)
            
            self.graph.addEdge(source, node_id, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                    {self.graph.PEDESTRIAN:ConstantFunction(time_source)}, edge_id, 1)
            self.graph.addEdge(node_id, source, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                    {self.graph.PEDESTRIAN:ConstantFunction(time_source)}, edge_id, 1)
            self.graph.addEdge(target, node_id, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                    {self.graph.PEDESTRIAN:ConstantFunction(time_target)},edge_id, 2)
            self.graph.addEdge(node_id, target, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                    {self.graph.PEDESTRIAN:ConstantFunction(time_target)}, edge_id, 2)
            
            #connecting new node to super node
            self.graph.addEdge(parent_id, node_id, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                    {self.graph.PEDESTRIAN:ConstantFunction(edge_time)})
            self.graph.addEdge(node_id, parent_id, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                    {self.graph.PEDESTRIAN:ConstantFunction(edge_time)})
        return parent_id
        
    def addLinkToSuperNode(self, node_id, parent_node_id):
        self.graph.addEdge(parent_node_id, node_id, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                    {self.graph.PEDESTRIAN:ConstantFunction(0)})
        self.graph.addEdge(node_id, parent_node_id, self.graph.TRANSFER, {self.graph.PEDESTRIAN}, 
                    {self.graph.PEDESTRIAN:ConstantFunction(0)})
        
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
         