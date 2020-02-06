'''
Created on Nov 25, 2019

@author: camila
'''
import sys

import psycopg2

from database.PostGISConnection import PostGISConnection
from load.LoadRoadNetwork import LoadRoadNetwork
from network.MultimodalNetwork import MultimodalNetwork
import networkx as nx
import osmapi
from psycopg2.extensions import AsIs
import json
from geojson.geometry import LineString

class RoadNetworkPostProcessing:
    def __init__(self, region):
        self.region = region
        self.connection = PostGISConnection()
        self.max_id = self.getMaxId()
        
    def run(self):
        self.removeSelfLoops()
        self.handleDuplicateReverseEdges()
        self.handleDuplicateEdges()
    
    def getMaxId(self):
        try:
            self.connection.connect()
            cursor = self.connection.getCursor()
            
            sql_max = """SELECT max(id) FROM roadnet_{}; """
            sql_max = sql_max.format(self.region)
                 
            cursor.execute(sql_max)
            
            (max_id, ) = cursor.fetchone()
            print(max_id)
            return max_id
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if self.connection is not None:
                self.connection.close()
        
    def removeSelfLoops(self):
        sql = """
            SELECT id, osm_id, osm_name, osm_meta, osm_source_id, osm_target_id, clazz, 
            flags, km, kmh, cost, reverse_cost, st_asgeojson(geom_way), source_location, target_location
            from roadnet_{0} WHERE osm_source_id = osm_target_id;
        """
        
        sql = sql.format(self.region)
        
        try:
            self.connection.connect()
            cursor = self.connection.getCursor()
            
            cursor.execute(sql)
            row = cursor.fetchone()
            while row is not None:
                self.breakEdge(row)
                row = cursor.fetchone()
                
            print("Commiting changes...")
            self.connection.conn.commit()
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if self.connection is not None:
                self.connection.close()
                
    def handleDuplicateReverseEdges(self):
        sql = """
            SELECT r.id, r.osm_id, r.osm_name, r.osm_meta, r.osm_source_id, r.osm_target_id, r.clazz, 
            r.flags, r.km, r.kmh, r.cost, r.reverse_cost, st_asgeojson(r.geom_way), r.source_location, r.target_location
            from roadnet_{0} as r,
            (select osm_source_id, osm_target_id from roadnet_{0}) as r2
            where r.osm_source_id = r2.osm_target_id and
            r.osm_target_id = r2.osm_source_id;
        """
        #reverse_cost < 1000
        
        sql = sql.format(self.region)
        
        try:
            self.connection.connect()
            cursor = self.connection.getCursor()
            
            cursor.execute(sql)
            row = cursor.fetchone()
            count = 0
            while row is not None:
                self.breakEdge(row)
                count += 1
                row = cursor.fetchone()
                #if count > 5:
                #    break
            print("Commiting changes...")
            self.connection.conn.commit()
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if self.connection is not None:
                self.connection.close()
                
    def breakEdge(self, row):  
        (edge_id, osm_id, osm_name, osm_meta, osm_source_id, osm_target_id, clazz, flags, km, kmh, cost, reverse_cost, geometry,
        source_location, target_location) = row
        print(edge_id, osm_source_id, osm_target_id)
        
        sql_delete = """DELETE FROM roadnet_{}
             WHERE id={}; """
             
        sql_insert = """INSERT INTO roadnet_{0}(id, osm_id, osm_name, osm_meta, osm_source_id, osm_target_id, clazz, flags, km, kmh, cost, reverse_cost, 
            geom_way, source_location, target_location) 
             VALUES ({1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, ST_SetSRID(ST_GeomFromGeoJSON('{13}'), 4326), '{14}', '{15}');
        """        
        
        try:
            cursor = self.connection.getCursor()
            
            coord = json.loads(geometry)['coordinates']
            print(coord)
            
            if len(coord) <= 2:
                print("geometry is too short")
                return
            
            api = osmapi.OsmApi()
            
            try:
                way = api.WayGet(osm_id)
            except (osmapi.ElementDeletedApiError, osmapi.OsmApiError) as error:
                print(error)
                sql_delete_id = sql_delete.format(self.region, edge_id)
                cursor.execute(sql_delete_id)
                print("Removing:" + str(edge_id))
                return
            
            i = 1
            pos = way["nd"].index(osm_source_id)
            node_id = way["nd"][pos + i]
            print(node_id)
            
            while node_id == osm_source_id or node_id == osm_target_id:
                i += 1
                pos = way["nd"].index(osm_source_id)
                node_id = way["nd"][pos + i]
                print(node_id)
            
            edge1 = LineString(coord[0:i+1])
            edge2 = LineString(coord[i:])
            
            sql_point = """SELECT ST_SetSRID(ST_MakePoint({}, {}), 4326) as point;"""
            sql_point = sql_point.format(coord[1][0], coord[1][1])
            cursor.execute(sql_point)
            (point, ) = cursor.fetchone()
            
            sql_ratio = """SELECT ST_LineLocatePoint(geom_way, '{1}') as source_ratio
                        FROM roadnet_{0} where id={2};
                        """
            sql_ratio = sql_ratio.format(self.region, point, edge_id)
            cursor.execute(sql_ratio)
    
            (ratio, ) = cursor.fetchone()
            if reverse_cost != 1000000:
                reverse_cost1 = reverse_cost*ratio
                reverse_cost2 = reverse_cost*(1-ratio)
            else:
                reverse_cost1 = 1000000
                reverse_cost2 = 1000000
                
            if not osm_name:
                osm_name = AsIs(osm_name)
            else:
                osm_name = "'" + osm_name.replace("'", "''") + "'"
                
            if not osm_meta:
                osm_meta = AsIs(osm_meta)
            else:
                osm_meta = "'" + osm_meta + "'"
                
            
            self.max_id += 1
            sql_insert1 = sql_insert.format(self.region, self.max_id, osm_id, osm_name, osm_meta, osm_source_id, 
                            node_id, clazz, flags, km*ratio, kmh, cost*ratio, 
                            reverse_cost1, json.dumps(edge1), source_location, point)
            cursor.execute(sql_insert1)
            print("Adding:" + str(self.max_id))
            print(osm_source_id, node_id)
            
            self.max_id+=1
            sql_insert2 = sql_insert.format(self.region, self.max_id, osm_id, osm_name, osm_meta, node_id, 
                            osm_target_id, clazz, flags, km*(1-ratio), kmh, cost*(1-ratio), 
                            reverse_cost2, json.dumps(edge2), point, target_location)
            cursor.execute(sql_insert2)
            print("Adding:" + str(self.max_id))
            print(node_id, osm_target_id)
            
            sql_delete_id = sql_delete.format(self.region, edge_id)
            
            cursor.execute(sql_delete_id)
            print("Removing:" + str(edge_id))
            print(osm_source_id, osm_target_id)
            
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
            
    def handleDuplicateEdges(self):   
        sql = """
            SELECT a.id, a.osm_id, a.osm_name, a.osm_meta, a.osm_source_id, a.osm_target_id, a.clazz, 
            a.flags, a.km, a.kmh, a.cost, a.reverse_cost, st_asgeojson(a.geom_way), source_location, target_location
            FROM roadnet_{0} a
            JOIN (SELECT osm_source_id, osm_target_id, COUNT(*)
            FROM roadnet_{0} 
            GROUP BY osm_source_id, osm_target_id
            HAVING count(*) > 1 ) b
            ON a.osm_source_id = b.osm_source_id
            AND a.osm_target_id = b.osm_target_id
            ORDER BY a.osm_source_id, a.osm_target_id;
        """
        sql = sql.format(self.region)
        
        try:
            self.connection.connect()
            cursor = self.connection.getCursor()
            cursor.execute(sql)
            
            row = cursor.fetchone()
            
            previous_source_id = -1
            previous_target_id = -1
            
            duplicate = []
            
            while row is not None:
                (edge_id, osm_id, osm_name, osm_meta, osm_source_id, osm_target_id, clazz, flags, km, kmh, cost, reverse_cost, geometry,
                 source_location, target_location) = row
                print(edge_id, osm_source_id, osm_target_id)
                
                if osm_source_id == previous_source_id and osm_target_id == previous_target_id:
                    duplicate.append([edge_id, osm_id, osm_name, osm_meta, osm_source_id, osm_target_id, clazz, flags, km, kmh, cost, reverse_cost, geometry,
                                      source_location, target_location])
                else:
                    self.handleDuplicate(duplicate)
                    duplicate = [[edge_id, osm_id, osm_name, osm_meta, osm_source_id, osm_target_id, clazz, flags, km, kmh, cost, reverse_cost, geometry,
                                  source_location, target_location]]
                    
                previous_source_id = osm_source_id
                previous_target_id = osm_target_id
                
                row = cursor.fetchone()
            self.handleDuplicate(duplicate)
            print("Commiting changes...")
            self.connection.conn.commit()
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if self.connection is not None:
                self.connection.close()
                
    def handleDuplicate(self, duplicate):
        if duplicate:
            #print(duplicate)
            #print(len(duplicate))
            clazz1 = duplicate[0][6]
            clazz2 = duplicate[1][6]
            length1 = duplicate[0][8]
            length2 = duplicate[1][8]
                
            sql_delete = """DELETE FROM roadnet_{}
                 WHERE id={}; """
                 
            sql_insert = """INSERT INTO roadnet_{0}(id, osm_id, osm_name, osm_meta, osm_source_id, osm_target_id, clazz, flags, km, kmh, cost, reverse_cost, 
                geom_way, source_location, target_location) 
                 VALUES ({1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, ST_SetSRID(ST_GeomFromGeoJSON('{13}'), 4326), '{14}', '{15}');
            """
            
            sql_max = """SELECT max(id) FROM roadnet_{}; """
            sql_max = sql_max.format(self.region)
                 
            cursor = self.connection.getCursor()
            
            cursor.execute(sql_max)
            
            (max_id, ) = cursor.fetchone()
            max_id += 1
            
            api = osmapi.OsmApi()
            
            
            #same class and same km: leave one
            if clazz1 == clazz2 and length1 == length2:
                #print("same class and same length")
                for i in range(1, len(duplicate)):
                    edge_id = duplicate[i][0]
                    #remove edge with this id
                    sql_delete_id = sql_delete.format(self.region, edge_id)
                    cursor.execute(sql_delete_id)
                    #print("Removing:" + str(edge_id))
                    
            #different classes and different length: break all but one
            #same class and different length: break all but one
            elif length1 != length2:
                print("different length")
                shortest = -1
                num_coord_min = sys.maxsize
                
                for i in range(len(duplicate)):
                    geom = json.loads(duplicate[i][12])
                    coord = geom['coordinates']
                    
                    if len(coord) < num_coord_min:
                        shortest = i
                        num_coord_min = len(coord)
                del duplicate[shortest]
                
                #break edges
                for dup in duplicate:
                    coord = json.loads(dup[12])['coordinates']
                    way_id = dup[1]
                    
                    try:
                        way = api.WayGet(way_id)
                    except (osmapi.ElementDeletedApiError, osmapi.OsmApiError) as error:
                        print(error)
                        sql_delete_id = sql_delete.format(self.region, dup[0])
                        cursor.execute(sql_delete_id)
                        print("Removing:" + str(dup[0]))
                        return
                    
                    source = dup[4]
                    target = dup[5]
                    i = 1
                    pos = way["nd"].index(source)
                    node_id = way["nd"][pos + i]
                    print(node_id)
                    
                    while node_id == source or node_id == target:
                        i += 1
                        pos = way["nd"].index(source)
                        node_id = way["nd"][pos + i]
                        print(node_id)
                    
                    edge1 = LineString(coord[0:i+1])
                    edge2 = LineString(coord[i:])
                    
                    sql_point = """SELECT ST_SetSRID(ST_MakePoint({}, {}), 4326) as point;"""
                    sql_point = sql_point.format(coord[1][0], coord[1][1])
                    cursor.execute(sql_point)
                    (point, ) = cursor.fetchone()
                    
                    sql_ratio = """SELECT ST_LineLocatePoint(geom_way, '{1}') as source_ratio
                                FROM roadnet_{0} where id={2};
                                """
                    sql_ratio = sql_ratio.format(self.region, point, dup[0])
                    cursor.execute(sql_ratio)
            
                    (ratio, ) = cursor.fetchone()
                    reverse_cost = dup[11]
                    if reverse_cost != 1000000:
                        reverse_cost1 = dup[11]*ratio
                        reverse_cost2 = dup[11]*(1-ratio)
                    else:
                        reverse_cost1 = 1000000
                        reverse_cost2 = 1000000
                        
                    osm_name = dup[2]
                    if not osm_name:
                        osm_name = AsIs(osm_name)
                    else:
                        osm_name = "'" + osm_name.replace("'", "''") + "'"
                        
                    osm_meta = dup[3]
                    if not osm_meta:
                        osm_meta = AsIs(osm_meta)
                    else:
                        osm_meta = "'" + osm_meta + "'"
                        
                    
                    sql_insert1 = sql_insert.format(self.region, max_id, way_id, osm_name, osm_meta, source, 
                                    node_id, dup[6], dup[7], dup[8]*ratio, dup[9], dup[10]*ratio, 
                                    reverse_cost1, json.dumps(edge1), dup[13], point)
                    cursor.execute(sql_insert1)
                    print("Adding:" + str(max_id))
                    print(dup[4], node_id)
                    max_id+=1
                    
                    sql_insert2 = sql_insert.format(self.region, max_id, way_id, osm_name, osm_meta, node_id, 
                                    target, dup[6], dup[7], dup[8]*(1-ratio), dup[9], dup[10]*(1-ratio), 
                                    reverse_cost2, json.dumps(edge2), point, dup[14])
                    cursor.execute(sql_insert2)
                    print("Adding:" + str(max_id))
                    print(node_id, dup[5])
                    max_id+=1
                    
                    sql_delete_id = sql_delete.format(self.region, dup[0])
                    
                    cursor.execute(sql_delete_id)
                    print("Removing:" + str(dup[0]))
                    print(source, target)
            #same length and different classes: leave both
            else:
                print("same length and different classes")
                
    
    
    def checkNetworkConnection(self, network_type):
        graph = self.loadNetork(network_type)
        if nx.is_connected(graph):
            print("Graph is connected!")
            return
        else:
            print("Graph is disconnected!")
            connected_components = nx.connected_components(graph)
            num_nodes = graph.number_of_nodes()
            count = 0
            
            sql_remove = """
                DELETE FROM roadnet_{0}
                WHERE id = {1};
            """
            
            connection = PostGISConnection()
            connection.connect()
            cursor = connection.getCursor()
            
            for c in sorted(connected_components, key=len, reverse=True):
                print(len(c))
                count += 1
                
                if len(c) > num_nodes * 0.01:
                    print("Component is large:" + str(len(c)))
                    continue
                else:
                    for n1 in c:
                        #print(n1)
                        #print(graph_undirected[n1])
                        for n2 in c:
                            #print(n1, n2)
                            edge = None
                            if n1 in graph:
                                if n2 in graph[n1]:
                                    edge = graph[n1][n2]
                            if edge:
                                original_edge = edge["original_edge_id"]
                                sql_remove_id = sql_remove.format(self.region, original_edge)
                                #cursor.execute(sql_remove_id)
                                
                        #print("Commiting changes...")
                        #connection.conn.commit()
            print(count)
        
    
    def loadNetork(self, network_type):
        print("Loading road network...")
        sql = """SELECT id, osm_source_id, osm_target_id, clazz, km, kmh, ST_X(source_location), ST_Y(source_location), 
            ST_X(target_location), ST_Y(target_location), reverse_cost
            FROM roadnet_{};
            """
        sql = sql.format(self.region)
        conn = None
        osm_mapping = {}
        graph = nx.Graph()
        node_id = 0
        try:
            conn = PostGISConnection()
            conn.connect()
            cursor = conn.conn.cursor()
            cursor.execute(sql)
            
            row = cursor.fetchone()
            
            while row is not None:
                (edge_id, osm_source_id, osm_target_id, clazz, length, speed, x_source, y_source, 
                 x_target, y_target, reverse_cost) = row
                #print(id, osm_source_id, osm_target_id)
                
                if network_type == MultimodalNetwork.PEDESTRIAN:
                    if clazz not in MultimodalNetwork.PEDESTRIAN_WAYS:
                        row = cursor.fetchone()
                        continue
                elif network_type == MultimodalNetwork.PRIVATE:
                    if clazz not in MultimodalNetwork.CAR_WAYS:
                        row = cursor.fetchone()
                        continue
                
                if osm_source_id not in osm_mapping:
                    graph.add_node(node_id, lat=x_source, lon=y_source)
                    osm_mapping[osm_source_id] = node_id
                    node_id += 1
                    
                
                if osm_target_id not in osm_mapping:
                    graph.add_node(node_id, lat=x_target, lon=y_target)
                    osm_mapping[osm_target_id] = node_id
                    node_id += 1
                    
                node_from = osm_mapping[osm_source_id]
                node_to = osm_mapping[osm_target_id]
                
                graph.add_edge(node_from, node_to, cost=length, original_edge_id = edge_id)
                    
                row = cursor.fetchone()
            return graph
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if conn is not None:
                conn.close()
    
    
    def removeDisconnectedComponents(self):
        network = MultimodalNetwork()
        load = LoadRoadNetwork(self.region, network)
        load.load()
        
        print(network.getNumNodes())
        print(network.getNumEdges())
        
        print("Creating undirected graph...")
        graph_undirected = network.graph.to_undirected()
        network = None
        
        if nx.is_connected(graph_undirected):
            print("Graph is connected!")
            return
        else:
            print("Graph is disconnected!")
            connected_components = nx.connected_components(graph_undirected)
            num_nodes = graph_undirected.number_of_nodes()
            #largest_cc = max(nx.connected_components(graph_undirected), key=len)
            count = 0
            
            sql_remove = """
                DELETE FROM roadnet_{0}
                WHERE id = {1};
            """
            
            connection = PostGISConnection()
            connection.connect()
            cursor = connection.getCursor()
                
            for c in sorted(connected_components, key=len, reverse=True):
                print(len(c))
                count += 1
                if len(c) > num_nodes * 0.05:
                    print("Component is large:" + str(len(c)))
                    continue
                else:
                    for n1 in c:
                        #print(n1)
                        #print(graph_undirected[n1])
                        for n2 in c:
                            #print(n1, n2)
                            edge = None
                            if n1 in graph_undirected:
                                if n2 in graph_undirected[n1]:
                                    edge = graph_undirected[n1][n2]
                            if edge:
                                #print(edge)
                                original_edge = edge["original_edge_id"]
                                sql_remove_id = sql_remove.format(self.region, original_edge)
                                cursor.execute(sql_remove_id)
                
                
                    #print("Commiting changes...")
                    #connection.conn.commit()
            print(count)

    
#removeDisconnectedComponents("berlin")
#checkNetworkConnection("edmonton", MultimodalNetwork.PEDESTRIAN)
