'''
Created on Oct 8, 2019

@author: camila
'''

import psycopg2
from database.config import config

edge_slot = {}
total_slots = 0

def coverage():
    readEdges()
    coverageMovement()
    covered = countSlotsCovered()
    print("% of slots covered:" + str(covered))

def countSlotsCovered():
    count = 0;
    for node_from in edge_slot:
        for node_to in edge_slot[node_from]:
            count+= len(edge_slot[node_from][node_to])
            
    return float(count)/total_slots
            
    
def readEdges():
    global edge_slot
    global total_slots
    """ Connect to the PostgreSQL database server """
    conn = None
    
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Reading edges...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        command = "SELECT node_from, node_to from edges;"
        
        # execute a statement
        cur.execute(command)
 
        # display the result
        row = cur.fetchone()
        
        while row is not None:
            (node_from, node_to) = row
            #print(node_from, node_to)
            if node_from in edge_slot:
                edge_slot[node_from][node_to] = set()
            else:
                edge_slot[node_from] = {}
                edge_slot[node_from][node_to] = set()
            
            total_slots+=1
            row = cur.fetchone()
            
        print(total_slots)
        total_slots*=24
        print("total slots:" + str(total_slots))        
        # close the communication with the PostgreSQL
        cur.close()
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')

def readJunctions():
    """ Connect to the PostgreSQL database server """
    conn = None
    junctions = {}
    
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Reading junctions...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        command = "SELECT * from junctions_movement;"
        
        # execute a statement
        cur.execute(command)
 
        # display the result
        row = cur.fetchone()
        
        while row is not None:
            (junction_id, osm_node_id) = row
            junctions[junction_id] = osm_node_id
            
            row = cur.fetchone()
            
        # close the communication with the PostgreSQL
        cur.close()
        print(len(junctions))
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')
    return junctions

def coverageMovement():
    junctions = readJunctions()
    global edge_slot
    """ Connect to the PostgreSQL database server """
    conn = None
    
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Reading movement...')
        conn = psycopg2.connect(**params)
        
        print("ok1")
      
        # create a cursor
        cur = conn.cursor()
        
        print("ok2")
        
        command = "SELECT start_junction_id, end_junction_id, hour from speeds_movement where hour <= 12;"
        
        # execute a statement
        cur.execute(command)
        
        print("ok3")
 
        # display the result
        row = cur.fetchone()
        
        print("ok4")
        
        while row is not None:
            (node_from, node_to, hour) = row
            node_from = junctions[node_from]
            node_to = junctions[node_to]
            
            #print(node_from, node_to, hour)
            
            addTimeSlot(node_from, node_to, hour)
            
            row = cur.fetchone()
        
        command = "SELECT start_junction_id, end_junction_id, hour from speeds_movement where hour > 12;"
        
        # execute a statement
        cur.execute(command)
        
        print("ok3")
 
        # display the result
        row = cur.fetchone()
        
        print("ok4")
        
        while row is not None:
            (node_from, node_to, hour) = row
            node_from = junctions[node_from]
            node_to = junctions[node_to]
            
            #print(node_from, node_to, hour)
            
            addTimeSlot(node_from, node_to, hour)
            
            row = cur.fetchone()
            
        # close the communication with the PostgreSQL
        cur.close()
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')
    
def addTimeSlot(node_from, node_to, time):
    if node_from in edge_slot:
        if node_to in edge_slot[node_from]:
            edge_slot[node_from][node_to].add(time)
        else:
            print("There is no edge from " + str(node_from) + " to " + str(node_to))
    else: 
        print("Node from not found!")
        
coverage()
    