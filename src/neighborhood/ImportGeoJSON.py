'''
Created on Nov 13, 2019

@author: camila
'''
import json
from os import listdir
from os.path import isfile, join
from database.PostGISConnection import PostGISConnection
import queue

class ImportGeoJSON:
    def __init__(self, directory, region):
        self.directory = directory
        self.region = region
        self.connection = PostGISConnection()
    
    def parse(self):
        self.connection.connect()
        #self.createTableNeighborhoods()
        #self.parseFilesDirectory()
        self.createTableNeighborhoodNodes()
        self.addPointsPolygon()
        self.connection.close()
        
    def createTableNeighborhoods(self):
        sql = """
        CREATE TABLE IF NOT EXISTS neighborhoods_{0} (
            id bigint NOT NULL,
            name character varying NOT NULL,
            level integer NOT NULL,
            parent bigint NOT NULL,
            polygon geometry NOT NULL,
            CONSTRAINT neighborhoods_{0}_pkey PRIMARY KEY (id)
        );
        """
        sql = sql.format(self.region)
        self.connection.executeCommand(sql)
        
    def createTableNeighborhoodNodes(self):
        sql = """
        CREATE TABLE IF NOT EXISTS neighborhood_nodes_{0} (
            id bigint NOT NULL,
            name character varying NOT NULL,
            level integer NOT NULL,
            parent bigint NOT NULL,
            nodes bigint[] NOT NULL,
            CONSTRAINT neighborhood_nodes_{0}_pkey PRIMARY KEY (id)
        );
        """
        sql = sql.format(self.region)
        self.connection.executeCommand(sql)
        
    def parseFilesDirectory(self):
        for f in listdir(self.directory):
            if isfile(join(self.directory, f)):
                self.parseFile(join(self.directory, f))
    
    def parseFile(self, file):
        print(file)
        with open(file) as f:
            data = json.load(f)
        
        self.addGeometryToDatabase(data)
        
    def addGeometryToDatabase(self, data):
        sql = """INSERT INTO neighborhoods_{0}(id, name, level, parent, polygon) 
                 VALUES ({1}, '{2}', {3}, {4}, ST_GeomFromGeoJSON('{5}'));
            """
        for feature in data['features']:
            nid = feature['properties']['id']
            name = feature['properties']['name']
            level = feature['properties']['admin_level']
            parentStr = feature['properties']['rpath']
            parent = parentStr.split(",")[1]
            print(nid, name, level, parent)
            geometry = json.dumps(feature['geometry'])
            
            #print(geometry)
            
            sql_neig = sql.format(self.region, nid, name, level, parent, geometry)
            self.connection.executeCommand(sql_neig)
    
    def getChildren(self, parent):
        children = []
        sql = """SELECT id from neighborhoods_{} where parent = {};
            """   
        sql = sql.format(self.region, parent) 
        
        cursor = self.connection.getCursor()
            
        cursor.execute(sql)
        row = cursor.fetchone()
            
        while row is not None:
            (node_id,) = row
            children.append(node_id)
            row = cursor.fetchone()
        
        #print(children)
        return children
    
    def getSuperParent(self):
        sql = """SELECT id from neighborhoods_{0} WHERE level = (SELECT min(level) from neighborhoods_{0});
            """   
        sql = sql.format(self.region) 
        
        cursor = self.connection.getCursor()
            
        cursor.execute(sql)
        (superParent, ) = cursor.fetchone()
        return superParent
    
    def getNeighborhood(self, nid):
        sql = """SELECT id, name, level, parent from neighborhoods_{0} WHERE id = {1};
            """   
        sql = sql.format(self.region, nid) 
        
        cursor = self.connection.getCursor()
            
        cursor.execute(sql)
        (id, name, level, parent) = cursor.fetchone()
        return (id, name, level, parent)
    
    def addPointsPolygon(self):
        superParent = self.getSuperParent()
        #print(superParent)
        
        q = queue.Queue()
        q.put(superParent)
        
        while not q.empty():
            nid = q.get()
            #print(nid)
            children = self.getChildren(nid)
            if children:
                for c in children:
                    q.put(c)
            else:
                #add points in the polygon
                #print("last level reached for:" + str(nid))
                self.getPointsNeighborhood(nid)
                
    def getPointsNeighborhood(self, nid):
        sql = """SELECT osm_source_id as id FROM roadnet_{0} as r, neighborhoods_{0} n
                 WHERE n.id = {1} and ST_Within(r.source_location, n.polygon)
                UNION 
                SELECT osm_target_id as id FROM roadnet_{0} as r, neighborhoods_{0} n
                WHERE n.id = {1} and ST_Within(r.target_location, n.polygon)
            ;   
            """
        sql = sql.format(self.region, nid)
        
        ids = set()
 
        cursor = self.connection.getCursor()
        
        cursor.execute(sql)
        row = cursor.fetchone()
        
        while row is not None:
            (node_id,) = row
            ids.add(node_id)
            row = cursor.fetchone()
        
        #print(ids)
        (id, name, level, parent) = self.getNeighborhood(nid)
        sql_insert = """INSERT INTO neighborhood_nodes_{0}(id, name, level, parent, nodes) 
                 VALUES ({1}, '{2}', {3}, {4}, '{5}');
            """
        str_ids = str(ids)
        sql_insert = sql_insert.format(self.region, nid, name, level, parent, ids)
        self.connection.executeCommand(sql_insert)
            
        
if __name__ == '__main__':
    directory = "/home/camila/BerlinBoundaries/exportedBoundaries_json_levels_land_20191108_154720"
    importJson = ImportGeoJSON(directory, "berlin")
    importJson.parse()