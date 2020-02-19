'''
Created on Nov 13, 2019

@author: camila
'''
import json
from os import listdir
from os.path import isfile, join
import queue

import psycopg2
from psycopg2.sql import SQL, Identifier

from database.PostGISConnection import PostGISConnection


class ImportBoundariesFromGeoJSON:
    def __init__(self, directory, region):
        self.directory = directory
        self.region = region
        self.connection = PostGISConnection()
    
    def parse(self):
        print("Importing boundaries...")
        self.connection.connect()
        self.createTableNeighborhoods()
        self.parseFilesDirectory()
        self.createTableNeighborhoodNodes()
        self.addPointsPolygon()
        self.connection.close()
        
    def createTableNeighborhoods(self):
        sql = """
        CREATE TABLE IF NOT EXISTS {} (
            id bigint NOT NULL,
            name character varying NOT NULL,
            level integer NOT NULL,
            parent bigint NOT NULL,
            polygon geometry NOT NULL,
            CONSTRAINT {} PRIMARY KEY (id)
        );
        """
        sql = SQL(sql).format(Identifier("neighborhoods_"+str(self.region)), 
                              Identifier("neighborhoods_"+str(self.region)+"_pkey"))
        self.connection.executeCommand(sql)
        
    def createTableNeighborhoodNodes(self):
        sql = """
        CREATE TABLE IF NOT EXISTS {} (
            id bigint NOT NULL,
            name character varying NOT NULL,
            level integer NOT NULL,
            parent bigint NOT NULL,
            nodes bigint[] NOT NULL,
            CONSTRAINT {} PRIMARY KEY (id)
        );
        """
        sql = SQL(sql).format(Identifier("neighborhood_nodes_"+str(self.region)), 
                              Identifier("neighborhood_nodes_"+str(self.region)+"_pkey"))
        self.connection.executeCommand(sql)
        
    def parseFilesDirectory(self):
        for f in listdir(self.directory):
            if isfile(join(self.directory, f)):
                self.parseFile(join(self.directory, f))
    
    def parseFile(self, file):
        with open(file) as f:
            data = json.load(f)
        
        self.addGeometryToDatabase(data)
        
    def addGeometryToDatabase(self, data):
        sql = """INSERT INTO {}(id, name, level, parent, polygon) 
                 VALUES (%s, %s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326));
            """
        sql = SQL(sql).format(Identifier("neighborhoods_"+str(self.region)))
        try:
            cursor = self.connection.getCursor()
            for feature in data['features']:
                nid = feature['properties']['id']
                name = feature['properties']['name']
                level = feature['properties']['admin_level']
                parentStr = feature['properties']['rpath']
                parent = parentStr.split(",")[1]
                geometry = json.dumps(feature['geometry'])
                
                cursor.execute(sql, (nid, name, level, parent, geometry))
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    
    def getChildren(self, parent):
        children = []
        sql = """SELECT id from {} where parent = %s;
            """   
        sql = SQL(sql).format(Identifier("neighborhoods_"+str(self.region)))
        
        try:
            cursor = self.connection.getCursor()
                
            cursor.execute(sql, (parent, ))
            row = cursor.fetchone()
                
            while row is not None:
                (node_id,) = row
                children.append(node_id)
                row = cursor.fetchone()
            
            cursor.close()
            return children
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    
    def getSuperParent(self):
        sql = """SELECT id from {0} WHERE level = (SELECT min(level) from {0});
            """   
        sql = SQL(sql).format(Identifier("neighborhoods_"+str(self.region)))
        
        try:
            cursor = self.connection.getCursor()
                
            cursor.execute(sql)
            (superParent, ) = cursor.fetchone()
            cursor.close()
            
            return superParent
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    
    def getNeighborhood(self, nid):
        sql = """SELECT id, name, level, parent from {} WHERE id = %s;
            """   
        sql = SQL(sql).format(Identifier("neighborhoods_"+str(self.region)))
        
        try:
            cursor = self.connection.getCursor()
                
            cursor.execute(sql, (nid, ))
            (neig_id, name, level, parent) = cursor.fetchone()
            cursor.close()
            return (neig_id, name, level, parent)
        
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    
    def addPointsPolygon(self):
        superParent = self.getSuperParent()
        
        q = queue.Queue()
        q.put(superParent)
        
        while not q.empty():
            nid = q.get()
            children = self.getChildren(nid)
            if children:
                for c in children:
                    q.put(c)
            else:
                #add points in the polygon
                self.insertPointsNeighborhood(nid)
                
    def insertPointsNeighborhood(self, nid):
        sql = """SELECT source as id FROM {0} as r, {1} n
                 WHERE n.id = %s and ST_Within(r.source_location, n.polygon)
                UNION 
                SELECT target as id FROM {0} as r, {1} n
                WHERE n.id = %s and ST_Within(r.target_location, n.polygon)
            ;   
            """
        sql = SQL(sql).format(Identifier("roadnet_"+str(self.region)),
                              Identifier("neighborhoods_"+str(self.region)))
        
        ids = []
        
        try:
            cursor = self.connection.getCursor()
            
            cursor.execute(sql, (nid, nid))
            row = cursor.fetchone()
            
            while row is not None:
                (node_id,) = row
                ids.append(node_id)
                row = cursor.fetchone()
            
            (neig_id, name, level, parent) = self.getNeighborhood(nid)
            sql_insert = """INSERT INTO {}(id, name, level, parent, nodes) 
                     VALUES (%s, %s, %s, %s, %s);
                """
            sql_insert = SQL(sql_insert).format(Identifier("neighborhood_nodes_"+str(self.region)))
            cursor.execute(sql_insert, (neig_id, name, level, parent, ids))
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
