'''
Created on Oct 9, 2019

@author: camila
'''

import psycopg2
from database.config import config
from builtins import str

#nodes = {}
empty_timestamp = "2019-01-01 00:00:00"
edge_id = 0
nodes = {}
nodes_id = set()

def processWays():
    ways, count = readMovementOSMWays()
    readOsmNodes()
    commands = "" 
    aux = 0
    for way_id in ways:
        nodes = ways[way_id][0]
        oneway = ways[way_id][1]
        
        i = 1
        start = nodes[0]
        segment = [start]
        segments = []
        while i < len(nodes):
            v = nodes[i]
            segment.append(v)
            if i == len(nodes) -1:
                segments.append(segment)
            else:
                if count[v] > 1:
                    start = v
                    segments.append(segment)
                    segment = [start]
            i+=1 
        
        speed_data = retrieveSpeedDataForWay(way_id)
        if not speed_data:
            #print("Missing way:" + (str(way_id)))
            commands += createEdgesForMissingWay(way_id, segments, oneway)
        else:
            commands += checkWaySegments(way_id, speed_data, segments)
            if not oneway:
                commands += checkWaySegments(way_id, speed_data, reverseSegments(segments))
        aux += 1
        if aux %10 == 0:
            executeCommand(commands)
            print(aux)
            commands = ""
    
    executeCommand(commands)
        

def checkWaySegments(way_id, speed_data, segments):
    global empty_timestamp
    commands = ""   
    for seg in segments:
        start = seg[0]
        end = seg[-1]
        if start in speed_data and end in speed_data[start]:
            commands+= createEdge(start, end, way_id, seg, True)
            for data in speed_data[start][end]:
                (year, month, day, hour, timestamp, speed, stddev) = data
                commands += createCommandForSegment(start, end, way_id, year, month, day, hour, timestamp, speed, stddev)
        else:
            #print("missing:" + str(start) + "," +str(end))
            commands+= createEdge(start, end, way_id, seg, False)
            commands += createCommandForSegment(start, end, way_id, -1, -1, -1, -1, empty_timestamp, -1, -1)
            
    #for node_from in speed_data:
    #    print("neighbors for " + str(node_from) + ":")
    #    print(speed_data[node_from].keys())
    return commands

"""
def checkSegment(start, end, nodes, speed_data, segments):
    end_pos =  nodes.index(end)
    if start in speed_data:
        for neig in speed_data[start]:
            if neig in nodes:
                pos = nodes.index(neig)
                if pos > end_pos:
                    print("Potential edge from " + str(start) + " to " + str(neig))
                    print(nodes)
                    print(segments)
"""                           
    
def createEdgesForMissingWay(way_id, segments, oneway):  
    commands = ""
    for seg in segments:
        node_from = seg[0]
        node_to = seg[1]
        commands+= createCommandForSegment(node_from, node_to, way_id, -1, -1, -1, -1, empty_timestamp, -1, -1)
        commands+= createEdge(node_from, node_to, way_id, seg, False)
        if not oneway:
            commands+= createCommandForSegment(node_to, node_from, way_id, -1, -1, -1, -1, empty_timestamp, -1, -1)
            commands+= createEdge(node_to, node_from, way_id, seg[::-1], False)
    return commands

def createEdge(node_from, node_to, way_id, segment, has_data):
    global edge_id
    global nodes
    geometry = "ST_GeomFromText('LINESTRING("
    
    for node in segment:
        if node not in nodes:
            print("NODE NOT FOUND!!")
        
        lat = float(nodes[node][0]/10**7)
        lon = float(nodes[node][1]/10**7)
        geometry+= str(lon) + " " + str(lat)+ ","
    geometry = geometry[:-1]
    geometry+= ")')"
    command =  ("INSERT INTO edges_movement_osm VALUES (" + str(edge_id) + ", " + str(node_from) + "," + str(node_to) + "," 
                + str(way_id) + ",ST_Length(" + geometry + "::geography)," + geometry + "," + str(has_data) + ");\n")
    edge_id += 1 
    return command

    
def createTableEdge():
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
                geometry geometry(LineString) NOT NULL,
                has_data boolean NOT NULL
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
            
        
    
def createCommandForSegment(node_from, node_to, osm_way_id, year, month, day, hour, timestamp, speed, stddev):
    timestamp = "'" + str(timestamp) + "'"
    return  ("INSERT INTO speeds_movement_osm VALUES (" + str(node_from) + ", " + str(node_to) + "," + str(osm_way_id) + "," 
                + str(year) + "," + str(month) + "," + str(day) +  "," + str(hour) + "," + timestamp
                + "," + str(speed) + "," + str(stddev) +");\n")
    
    
def reverseSegments(segments):
    rev_segments = []
    for seg in reversed(segments):
        rev_segments.append(seg[::-1])
    return rev_segments


def retrieveSpeedDataForWay(way_id):
    """ Connect to the PostgreSQL database server """
    conn = None
    speed_data = {}
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        #print('Retrieving speed data for ' + str(way_id))
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        command = """
            SELECT  
            (SELECT osm_node_id
            FROM   junctions_movement
            where junction_id = start_junction_id
            ) AS from_node_id,
            (SELECT osm_node_id
            FROM   junctions_movement
            where junction_id = end_junction_id
            ) AS to_node_id,
            year,
            month,
            day, 
            hour,
            utc_timestamp,
            speed_mph_mean,
            speed_mph_stddev
    
            from speeds_movement sp
            where sp.segment_id = any(select segment_id from segments_movement where osm_way_id = 
        """ + str(way_id) + ");"
        
        # execute a statement
        cur.execute(command)
 
        # display the result
        row = cur.fetchone()
        
        while row is not None:
            (node_from, node_to, year, month, day, hour, timestamp, speed, dev) = row
            if node_from in speed_data:
                if node_to in speed_data[node_from]:
                    speed_data[node_from][node_to].append((year, month, day, hour, timestamp, speed, dev))
                else:
                    speed_data[node_from][node_to] = [(year, month, day, hour, timestamp, speed, dev)]
            else:
                speed_data[node_from] = {}
                speed_data[node_from][node_to] = [(year, month, day, hour, timestamp, speed, dev)]
            row = cur.fetchone()
            
        # close the communication with the PostgreSQL
        cur.close()
        #print("#movement ways: "+ str(len(ways)))
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')
    return speed_data
        
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
    count = {}
    
    global nodes_id
    
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Reading OSM ways...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        command = "SELECT * from planet_osm_ways;"
        
        # execute a statement
        cur.execute(command)
 
        # display the result
        row = cur.fetchone()
        
        while row is not None:
            (osm_way_id, nodes, tags) = row
            if osm_way_id in ways_id:
                #print(osm_way_id)
                oneway = False
                if tags and "oneway" in tags:
                    pos = tags.index("oneway")
                    pos+=1
                    if tags[pos] == "yes":
                        oneway = True
                ways_osm[osm_way_id] = (nodes, oneway)
                
                for v in nodes:
                    nodes_id.add(v)
                    if v in count:
                        count[v]+=1
                    else:
                        count[v]=1
                
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
    return ways_osm, count
    
def readMovementSpeeds():
    """ Connect to the PostgreSQL database server """
    conn = None
    global nodes
    
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Reading speeds...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        command = """
            SELECT  
            (SELECT osm_way_id
            FROM   segments_movement se
            where se.segment_id = sp.segment_id
            ) AS osm_way_id,
            (SELECT osm_node_id
            FROM   junctions_movement
            where junction_id = start_junction_id
            ) AS from_node_id,
            (SELECT osm_node_id
            FROM   junctions_movement
            where junction_id = end_junction_id
            ) AS to_node_id,
            year,
            month,
            day, 
            hour,
            speed_mph_mean,
            speed_mph_stddev
            FROM speeds_movement sp;
        """
        
        # execute a statement
        cur.execute(command)
 
        # display the result
        row = cur.fetchone()
        
        while row is not None:
            (osm_way_id) = row
            print(osm_way_id)
            
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

def readOsmNodes():
    """ Connect to the PostgreSQL database server """
    conn = None
    global nodes, nodes_id
    
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
        
        aux = 0;
        while row is not None:
            (node_id, lat, lon) = row
            
            
            #print(node_id)
            
            if node_id in nodes_id:
                nodes[node_id] = (lat, lon)
            
            aux+=1
            row = cur.fetchone()
            
        # close the communication with the PostgreSQL
        print(aux)
        print("#nodes: "+ str(len(nodes)))
        cur.close()
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')
            
def executeCommand(command):
    #print(command)
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        #print('Creating edges for way...')
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
        DROP TABLE IF EXISTS speeds_movement_osm CASCADE;
        """,
        """ CREATE TABLE speeds_movement_osm( 
                node_from            bigint NOT NULL,
                node_to              bigint NOT NULL,
                osm_way_id           bigint NOT NULL,
                year                 integer NOT NULL,
                month                integer NOT NULL,
                day                  integer NOT NULL,
                hour                 integer NOT NULL,
                utc_timestamp        timestamp NOT NULL,
                speed_mph_mean       double precision NOT NULL,
                speed_mph_stddev     double precision NOT NULL
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

createTable()
createTableEdge()
processWays()
#readOsmNodes()
#readMovementOSMWays()