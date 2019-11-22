'''
Created on Oct 7, 2019

@author: camila
'''

import psycopg2
from database.config import config

edge_id = 0

def createEdgesFromWays():
    createTable()
    ways, count = readWays()
    nodes = readNodes()
    createEdges(ways, count, nodes)
    

def createEdges(ways, count, nodes_coord):
    aux = 0
    command = ""
    for way_id in ways:
        command += processWay(ways[way_id], count, nodes_coord)
        aux+=1
        if aux % 1000 == 0:
            addEdgesFromWay(command)
            command = ""
            print(aux)
    addEdgesFromWay(command)
    print("edges:" + str(aux))    
          
                
def processWay(way, count, nodes_coord):   
    nodes = way[1]
    tags = way[2]
    oneway = False
    if tags and "oneway" in tags:
        pos = tags.index("oneway")
        pos+=1
        if tags[pos] == "yes":
            oneway = True
    #print(nodes)
    i = 1
    start = nodes[0]
    (lat, lon) = nodes_coord[start]
    segment = [(lat, lon)]
    commands = ""
    while i < len(nodes):
        v = nodes[i]
        (lat, lon) = nodes_coord[v]
        segment.append((lat, lon))
        if i == len(nodes) -1:
            commands += createSQLCommand(way[0], start, v, segment, oneway)
        else:
            if count[v] > 1:
                commands += createSQLCommand(way[0], start, v, segment, oneway)
                start = v
                segment = [(lat, lon)]
        i+=1 
    return commands

def createSQLCommand(id_way, node_from, node_to, segment, oneway):
    #print(segment)
    global edge_id
    geometry = "ST_GeomFromText('LINESTRING("
    for (lat, lon) in segment:
        geometry+= str(lon) + " " + str(lat)+ ","
    geometry = geometry[:-1]
    geometry+= ")')"
    command =  ("INSERT INTO edges VALUES (" + str(edge_id) + ", " + str(node_from) + "," + str(node_to) + "," 
                + str(id_way) + ",ST_Length(" + geometry + "::geography)," + geometry + ");\n")
    edge_id += 1 
    
    if not oneway:
        #print("creating reverse edge")
        command += ("INSERT INTO edges VALUES (" + str(edge_id) + ", " + str(node_to) + "," + str(node_from) + "," 
                + str(id_way) + ",ST_Length(" + geometry + "::geography), ST_Reverse(" + geometry + "));\n")
        edge_id += 1
    return command

def addEdgesFromWay(command):
    #print(command)
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Creating edges for way...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        cur.execute(command)
 
        # close the communication with the PostgreSQL
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')
    
def readWays():
    """ Connect to the PostgreSQL database server """
    conn = None
    
    ways = {}
    count = {}
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Reading ways...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        command = "SELECT * from planet_osm_ways;"
        
        # execute a statement
        cur.execute(command)
 
        # display the result
        row = cur.fetchone()
        
        aux = 100
        while row is not None:
            (way_id, nodes, tags) = row
            ways[way_id] = (way_id, nodes, tags)
            row = cur.fetchone()
            
            for v in nodes:
                if v in count:
                    count[v]+=1
                else:
                    count[v]=1
            
            aux-=1        
                
        print("ways:" + str(len(ways)))
        # close the communication with the PostgreSQL
        cur.close()
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')
    
    return ways, count

def readNodes():
    """ Connect to the PostgreSQL database server """
    conn = None
    
    nodes = {}
    
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Reading nodes...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        command = "SELECT id, lat, lon from planet_osm_nodes;"
        
        # execute a statement
        cur.execute(command)
 
        # display the result
        row = cur.fetchone()
        
        while row is not None:
            (node_id, lat, lon) = row
            lat = float(lat)/(10**7)
            lon = float(lon)/(10**7)
            nodes[node_id] = (lat,lon)
            row = cur.fetchone()
            
        # close the communication with the PostgreSQL
        cur.close()
        
        print("nodes:" + str(len(nodes)))
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')
    
    return nodes
             
def createTable():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Creating table...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        commands = (
        """
        DROP TABLE IF EXISTS edges CASCADE;
        """,
        """ CREATE TABLE edges( 
                id                   bigint PRIMARY KEY,
                node_from            bigint NOT NULL,
                node_to              bigint NOT NULL,
                osm_way_id           bigint NOT NULL,
                length               double precision NOT NULL,
                geometry             geometry(LINESTRING) NOT NULL
            );
        """)
        
        # execute a statement
        for command in commands:
            cur.execute(command)
 
        # close the communication with the PostgreSQL
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')


createEdgesFromWays()        
