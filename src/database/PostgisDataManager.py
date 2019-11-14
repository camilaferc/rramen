'''
Created on Oct 31, 2019

@author: camila
'''
import time

from geojson import Feature, FeatureCollection
import psycopg2
import json

from database.PostGISConnection import PostGISConnection


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
        sql_polygon = "ST_MakePolygon( ST_GeomFromText('LINESTRING("
        
        for c in coordinates:
            sql_polygon += str(c[0]) + " " + str(c[1]) + ","
        
        sql_polygon = sql_polygon[:-1]
        
        sql_polygon += ")'))"
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
        
        