'''
Created on Oct 31, 2019

@author: camila
'''
import json
import time

from geojson import Feature, FeatureCollection
from geojson.geometry import LineString
from geojson import loads
import psycopg2

from database.PostGISConnection import PostGISConnection
from gtfs import GTFS
from network.MultimodalNetwork import MultimodalNetwork


class PostgisDataManager:
    
    def __init__(self):
        self.connection = PostGISConnection()
        self.timeGeom = 0
        
    def getNeighborhoodsPolygons(self, region):
        sql = """SELECT id, name, level, parent, ST_AsGeoJSON(polygon) as polygon
                FROM neighborhoods_{};
            ;   
            """
        sql = sql.format(region);
        
        try:
            features = []
            self.connection.connect();
 
            cursor = self.connection.getCursor()
            
            cursor.execute(sql)
            row = cursor.fetchone()
            
            while row is not None:
                (nid, name, level, parent, polygon) = row
                properties = {'name': name, 'level':level, 'parent': parent}
                #print(nid, name, level, parent)
                #print(polygon)
                geometry = json.loads(polygon)
                feature = Feature(geometry = geometry, properties = properties, id = nid)
                #print(feature)
                features.append(feature)
    
                row = cursor.fetchone()
            
            self.connection.close()
            return FeatureCollection(features)
        
        except (Exception, psycopg2.DatabaseError) as error:
            print(error) 
            
    def getRoutes(self, region):
        #sql = """SELECT route_id, route_short_name, route_type
        #        FROM routes_{}
        #        WHERE ;
        #    ;   
        #    """
        '''
        sql = """
                SELECT r.route_id, r.route_short_name, r.route_type
                FROM routes_{0} r, routes_geometry_{0} rg
                WHERE r.route_id = rg.route_id and
                ST_Intersects(rg.route_geom, 
                (SELECT polygon from neighborhoods_{0} where level = (SELECT min(level) FROM neighborhoods_{0}))
            );
            """
        '''
            
        sql = """ SELECT r.route_id, r.route_short_name, r.route_type
            FROM routes_{0} r
            WHERE r.route_id in (SELECT route_id from routes_geometry_{0});
            """
        sql = sql.format(region);
        
        try:
            print("Loading routes...")
            routes = {}
            self.connection.connect();
 
            cursor = self.connection.getCursor()
            
            cursor.execute(sql)
            row = cursor.fetchone()
            
            while row is not None:
                (route_id, name, route_type) = row
                
                #print(route_id, name, route_type)
                
                if route_type in GTFS.ROUTE_TYPE:
                    trans_type = GTFS.ROUTE_TYPE[route_type]
                    if trans_type in routes:
                        list_routes = routes[trans_type]
                        list_routes[name] = route_id
                    else:
                        routes[trans_type] = {name: route_id}
                
                row = cursor.fetchone()
                    
            self.connection.close()
            return routes
        
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)  
    
    def getPointsWithinPolygon(self, region, coordinates):
        sql = """SELECT osm_source_id as id FROM roadnet_{0} as p,
                (SELECT {1}  as polygon) as pol
                 WHERE ST_Within(source_location, polygon)
                UNION 
                SELECT osm_target_id as id FROM roadnet_{0} as p,
                (SELECT {1} as polygon) as pol
                WHERE ST_Within(target_location, polygon)
            ;   
            """
        sql_polygon = "ST_Polygon('LINESTRING("
        
        for c in coordinates[0]:
            sql_polygon += str(c[0]) + " " + str(c[1]) + ","
        
        sql_polygon = sql_polygon[:-1]
        
        sql_polygon += ")'::geometry, 4326)"
        sql = sql.format(region, sql_polygon)
        
        try:
            ids = set()
            self.connection.connect();
 
            cursor = self.connection.getCursor()
            
            cursor.execute(sql)
            row = cursor.fetchone()
            
            while row is not None:
                (node_id,) = row
                ids.add(node_id)
                row = cursor.fetchone()
            
            self.connection.close()
            return ids
        
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    
    
    def getPointsWithinNeighborhoods(self, region, selected_neig):
        sql = """SELECT nodes from neighborhood_nodes_{0} where id = {1};   
            """
        points = []
        try:
            self.connection.connect();
 
            cursor = self.connection.getCursor()
            
            for n in selected_neig:
                #print(n)
                sql_neig = sql.format(region, n)
                cursor.execute(sql_neig)
                (nodes, ) = cursor.fetchone()
                #print(nodes)
                points.extend(nodes)
                #print(len(points))
            
            
            self.connection.close()
            return points
        
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
    
    def getClosestEdge(self, lat, lon, region):
        sql = """SELECT id, osm_source_id, osm_target_id, 
                ST_LineLocatePoint(geom_way, point) as source_ratio,
                ST_X(ST_LineInterpolatePoint(geom_way, ST_LineLocatePoint(geom_way, point))) as lon,
                ST_Y(ST_LineInterpolatePoint(geom_way, ST_LineLocatePoint(geom_way, point))) as lat
                from roadnet_{},
                (SELECT ST_SetSRID(ST_MakePoint({}, {}),4326) as point) as p
                ORDER BY point <-> geom_way
                LIMIT 1;    
            """
        sql = sql.format(region, lon, lat)
        
        try:
            self.connection.connect();
 
            cursor = self.connection.getCursor()
            
            cursor.execute(sql)
            (edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat) = cursor.fetchone()
            
            #print(edge_id, source_ratio)
            
            self.connection.close()
            return (edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat)
        
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
    def getClosestEdgeByClass(self, lat, lon, region, class_list):
        sql = """SELECT id, osm_source_id, osm_target_id, 
                ST_LineLocatePoint(geom_way, point) as source_ratio,
                ST_X(ST_LineInterpolatePoint(geom_way, ST_LineLocatePoint(geom_way, point))) as lon,
                ST_Y(ST_LineInterpolatePoint(geom_way, ST_LineLocatePoint(geom_way, point))) as lat
                from roadnet_{},
                (SELECT ST_SetSRID(ST_MakePoint({}, {}),4326) as point) as p
                WHERE clazz = ANY('{}'::int[])
                ORDER BY point <-> geom_way
                LIMIT 1;    
            """
        sql = sql.format(region, lon, lat, class_list)
        
        try:
            self.connection.connect();
 
            cursor = self.connection.getCursor()
            
            cursor.execute(sql)
            (edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat) = cursor.fetchone()
            
            #print(edge_id, source_ratio)
            
            self.connection.close()
            return (edge_id, osm_source, osm_target, source_ratio, node_lon, node_lat)
        
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
    def getRoadGeometry(self, edge_id, region):
        sql = """SELECT ST_AsGeoJSON(geom_way) from roadnet_{}
            WHERE id = {} ;    
            """
        sql = sql.format(region, edge_id)
        
        try:
            start = time.time()
            self.connection.connect();
 
            cursor = self.connection.getCursor()
            
            cursor.execute(sql)
            (geometry, ) = cursor.fetchone()
            
            #print(geometry)
            
            self.connection.close()
            total = time.time() - start
            self.timeGeom += total
            return geometry
        
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    
    def getRouteGeometry(self, route_id, region):
        sql = """SELECT ST_AsGeoJSON(route_geom) from routes_geometry_{}
            WHERE route_id = '{}' ;    
            """
        sql = sql.format(region, route_id)
        
        try:
            self.connection.connect();
 
            cursor = self.connection.getCursor()
            
            cursor.execute(sql)
            (geometry, ) = cursor.fetchone()
            
            #print(geometry)
            
            self.connection.close()
            line = loads(geometry)
            return LineString(line.coordinates)
        
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
    def getLinkGeometry(self, edge_id, edge_pos, region):
        if edge_pos == 1:
            sql = """SELECT ST_AsGeoJSON(source_point_geom) from links_{}
                WHERE link_id = {} ;    
                """
                
        if edge_pos == 2:
            sql = """SELECT ST_AsGeoJSON(point_target_geom) from links_{}
                WHERE link_id = {} ;    
                """
                
        sql = sql.format(region, edge_id)
        
        try:
            self.connection.connect();
 
            cursor = self.connection.getCursor()
            
            cursor.execute(sql)
            (geometry, ) = cursor.fetchone()
            
            #print(geometry)
            
            self.connection.close()
            return geometry
        
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        
        