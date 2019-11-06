'''
Created on Oct 11, 2019

@author: camila
'''

import psycopg2
from database.config import config


def readMovementWays():
    """ Connect to the PostgreSQL database server """
    conn = None
    ways = set()
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Reading movement ways...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        command = "SELECT distinct osm_way_id from segments_movement;"
        
        # execute a statement
        cur.execute(command)
 
        # display the result
        row = cur.fetchone()
        
        while row is not None:
            (osm_way_id) = row[0]
            ways.add(osm_way_id)
            
            row = cur.fetchone()
            
        # close the communication with the PostgreSQL
        cur.close()
        print("#movement ways: "+ str(len(ways)))
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')
    print(len(ways))        
    return ways

def readMovementOSMWays():
    ways_id = readMovementWays()
    """ Connect to the PostgreSQL database server """
    conn = None
    ways_osm = {}
    
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Reading OSM ways...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        command = "SELECT id, nodes from planet_osm_ways;"
        
        # execute a statement
        cur.execute(command)
 
        # display the result
        row = cur.fetchone()
        
        while row is not None:
            (osm_way_id, nodes) = row
            if osm_way_id in ways_id:
                #print(osm_way_id)
                
                ways_osm[osm_way_id] = nodes
                
            row = cur.fetchone()
            
        # close the communication with the PostgreSQL
        cur.close()
        print("#osm ways: "+ str(len(ways_osm)))
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')
    return ways_osm

def processEdges():
    edges = readMovementOSMEdges()
    ways = readMovementOSMWays()
    for node_from in edges:
        for node_to in edges[node_from]:
            osm_way_id = edges[node_from][node_to]
            nodes = ways[osm_way_id]
            start = nodes.index(node_from)
            end = nodes.index(node_to)+1
            
            if not nodes:
                print("Empty nodes!!! : " + osm_way_id)
            if start > end:
                print("reverse edge")
                print(nodes)
                nodes_rev = nodes.reverse()
                print(nodes_rev)
                start = nodes_rev.index(node_to)
                end = nodes_rev.index(node_from)+1
                segment = nodes_rev[start:end]
            else:
                segment = nodes[start:end]
            
            print(node_from, node_to)
            print(segment)

def readMovementOSMEdges():
    ways_id = readMovementWays()
    """ Connect to the PostgreSQL database server """
    conn = None
    edges = {}
    
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Reading OSM ways...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        sql_edges = "SELECT distinct node_from, node_to, osm_way_id from speeds_movement_osm;"
        
        # execute a statement
        cur.execute(sql_edges)
 
        # display the result
        row = cur.fetchone()
        
        while row is not None:
            (node_from, node_to, osm_way_id) = row
            if osm_way_id in ways_id:
                
                if node_from in edges:
                    if node_to in edges[node_from]:
                        print("Repeated edge!")
                    edges[node_from][node_to] = osm_way_id
                else:
                    edges[node_from] = {node_to:osm_way_id}
                
            row = cur.fetchone()
            
        # close the communication with the PostgreSQL
        cur.close()
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')
    return edges

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
        DROP TABLE IF EXISTS edges_movement_osm CASCADE;
        """,
        """ CREATE TABLE edges_movement_osm( 
                id bigint PRIMARY KEY,
                node_from bigint NOT NULL,
                node_to bigint NOT NULL,
                osm_way_id bigint NOT NULL,
                length double precision NOT NULL,
                geometry geometry(LineString) NOT NULL
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
            
            
def getWayGeometries():
    ways_id = readMovementWays()
    """ Connect to the PostgreSQL database server """
    conn = None
    ways_osm = {}
    
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Reading OSM ways...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        command = "SELECT osm_id, way, ST_IsValid(way) from planet_osm_line;"
        
        # execute a statement
        cur.execute(command)
 
        # display the result
        row = cur.fetchone()
        
        commands = ""
        
        while row is not None:
            (osm_way_id, geometry, isvalid) = row
            if osm_way_id in ways_id:
                print(osm_way_id)
                
                print(geometry)
                print(isvalid)
                if isvalid:
                    commands+= ("INSERT INTO ways_movement_osm VALUES (" + str(osm_way_id) + "," + geometry + ");\n")
                else:
                    print("geometry not valid!!!")
            row = cur.fetchone()
            
        # close the communication with the PostgreSQL
        cur.execute(commands)
 
        # close the communication with the PostgreSQL
        cur.close()
        conn.commit()
        
        print("#osm ways: "+ str(len(ways_osm)))
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')
    return ways_osm
            
def createTableWays():
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
        DROP TABLE IF EXISTS ways_movement_osm CASCADE;
        """,
        """ CREATE TABLE ways_movement_osm( 
                osm_way_id bigint PRIMARY KEY,
                geometry geometry(LineString) NOT NULL
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

def readOsmNodes():
    """ Connect to the PostgreSQL database server """
    conn = None
    global nodes
    
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
            nodes[node_id] = (lat, lon)
            
            row = cur.fetchone()
            
        # close the communication with the PostgreSQL
        cur.close()
        print("#nodes: "+ str(len(nodes)))
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')
            
createTableWays()
getWayGeometries()