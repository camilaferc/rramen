'''
Created on Nov 15, 2019

@author: camila
'''
import json
import sys

import psycopg2

from database.PostGISConnection import PostGISConnection
import osmapi
from psycopg2.extensions import AsIs
from geojson.geometry import LineString


class OSM(object):

    def __init__(self, region):
        self.region = region
        self.connection = PostGISConnection()
    
    def postProcessing(self):
        #self.handleDuplicatedEdges()
        self.removeDisconnectedEdges()
        
    def removeDisconnectedEdges(self):
        sql = """
            SELECT id, osm_source_id, osm_target_id
            FROM roadnet_{0}
            WHERE id >= 150000;
        """
        sql = sql.format(self.region)
        
        try:
            self.connection.connect()
            cursor = self.connection.getCursor()
            cursor.execute(sql)
            toRemove = set()
            
            row = cursor.fetchone()
            
            while row is not None:
                (id, source, target) = row
                #print(id, osm_source_id, osm_target_id)
                #print(id)
                
                if self.isDisconnected(source, target):
                    print("DISCONNECTED!")
                    print(id, source, target)
                    
                    toRemove.add(id)
                    
                row = cursor.fetchone()
                
            self.removeEdges(toRemove)
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
                
    def removeEdges(self, toRemove):
        print(len(toRemove))
        sql_remove = """
            DELETE FROM roadnet_{0}
            WHERE id = {1};
        """
        try:
            self.connection.connect()
            cursor = self.connection.getCursor()
            
            for r in toRemove:
                sql_remove_id = sql_remove.format(self.region, r)
                cursor.execute(sql_remove_id)
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
            
    def isDisconnected(self, source, target): 
        sql = """
            select count(*) from
            (select id from roadnet_{0} where osm_source_id = {1} or osm_target_id={1}
            UNION
            select id from roadnet_{0} where osm_source_id = {2} or osm_target_id={2}) as a;
        """
        sql = sql.format(self.region, source, target) 
        
        try:
            cursor = self.connection.getCursor()
            cursor.execute(sql)
            
            (count, ) = cursor.fetchone()
            
            if count == 1:
                return True
            else:
                return False
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
             
         
    def handleDuplicatedEdges(self):   
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
                #print(id, osm_source_id, osm_target_id)
                
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
            #print(max_id)
            
            api = osmapi.OsmApi()
            
            
            #same class and same km: remove one
            if clazz1 == clazz2 and length1 == length2:
                #print("same class and same length")
                for i in range(1, len(duplicate)):
                    edge_id = duplicate[i][0]
                    #remove edge with this id
                    sql_delete_id = sql_delete.format(self.region, edge_id)
                    cursor.execute(sql_delete_id)
                    print("Removing:" + str(edge_id))
                    
            #different classes and different km: break the longest
            #same class and different length: break the longest
            elif length1 != length2:
                #print("different length")
                geom1 = json.loads(duplicate[0][12])
                geom2 = json.loads(duplicate[1][12])
                
                
                coord1 = geom1['coordinates']
                coord2 = geom2['coordinates']
                
                if len(coord1) > len(coord2):
                    longest_edge = duplicate[0]
                    coord = coord1
                else:
                    longest_edge = duplicate[1]
                    coord = coord2
                
                #break longest edge
                edge1 = LineString(coord[0:2])
                edge2 = LineString(coord[1:])
                
                way_id = longest_edge[1]
                #print(way_id)
                try:
                    way = api.WayGet(way_id)
                except (osmapi.ElementDeletedApiError, osmapi.OsmApiError) as error:
                    print(error)
                    sql_delete_id = sql_delete.format(self.region, longest_edge[0])
                    cursor.execute(sql_delete_id)
                    print("Removing:" + str(longest_edge[0]))
                    return
                
                source = longest_edge[4]
                pos = way["nd"].index(source)
                node_id = way["nd"][pos + 1]
                print(node_id)
                
                sql_point = """SELECT ST_SetSRID(ST_MakePoint({}, {}), 4326) as point;"""
                sql_point = sql_point.format(coord[1][0], coord[1][1])
                cursor.execute(sql_point)
                (point, ) = cursor.fetchone()
                
                sql_ratio = """SELECT ST_LineLocatePoint(geom_way, '{1}') as source_ratio
                            FROM roadnet_{0} where id={2};
                            """
                sql_ratio = sql_ratio.format(self.region, point, longest_edge[0])
                cursor.execute(sql_ratio)
        
                (ratio, ) = cursor.fetchone()
                #print(coord1[1][0], coord1[1][1])
                #print(ratio)
                reverse_cost = longest_edge[11]
                if reverse_cost != 1000000:
                    reverse_cost1 = longest_edge[11]*ratio
                    reverse_cost2 = longest_edge[11]*(1-ratio)
                else:
                    reverse_cost1 = 1000000
                    reverse_cost2 = 1000000
                    
                osm_name = longest_edge[2]
                if not osm_name:
                    osm_name = AsIs(osm_name)
                else:
                    osm_name = "'" + osm_name + "'"
                    
                osm_meta = longest_edge[3]
                if not osm_meta:
                    osm_meta = AsIs(osm_meta)
                else:
                    osm_meta = "'" + osm_meta + "'"
                    
                
                sql_insert1 = sql_insert.format(self.region, max_id, way_id, osm_name, osm_meta, longest_edge[4], 
                                node_id, longest_edge[6], longest_edge[7], longest_edge[8]*ratio, longest_edge[9], longest_edge[10]*ratio, 
                                reverse_cost1, json.dumps(edge1), longest_edge[13], point)
                cursor.execute(sql_insert1)
                print("Adding:" + str(max_id))
                print(longest_edge[4], node_id)
                max_id+=1
                
                sql_insert2 = sql_insert.format(self.region, max_id, way_id, osm_name, osm_meta, node_id, 
                                longest_edge[5], longest_edge[6], longest_edge[7], longest_edge[8]*(1-ratio), longest_edge[9], longest_edge[10]*(1-ratio), 
                                reverse_cost2, json.dumps(edge2), point, longest_edge[14])
                cursor.execute(sql_insert2)
                print("Adding:" + str(max_id))
                print(node_id, longest_edge[5])
                max_id+=1
                
                sql_delete_id = sql_delete.format(self.region, longest_edge[0])
                
                cursor.execute(sql_delete_id)
                print("Removing:" + str(longest_edge[0]))
                print(longest_edge[4], longest_edge[5])
                
            #same length and different classes: leave both
            
            
            
osm = OSM("berlin")
osm.postProcessing()