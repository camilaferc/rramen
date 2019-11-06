'''
Created on Oct 31, 2019

@author: camila
'''
from database.PostGISConnection import PostGISConnection
import psycopg2

class PostgisDataManager:
    
    def __init__(self):
        self.connection = PostGISConnection()

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
            self.connection.connect();
 
            cursor = self.connection.getCursor()
            
            cursor.execute(sql)
            (geometry, ) = cursor.fetchone()
            
            #print(geometry)
            
            self.connection.close()
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
        
        