'''
Created on Oct 5, 2019

@author: camila
'''

import psycopg2
from database.config import config
from network.Graph import MultimodalNetwork

 
def buildGraph(day=0, month=0, year=0):
    g = MultimodalNetwork()
    #buildVertices(g)
    #print (g.getNumNodes())
    buildEdges(g, day, month, year)
    print (g.getNumNodes())
    print (g.getNumEdges())
    print("MultimodalNetwork imported")
    print(g.isConnected())
    g.getConnectedComponents()
    
    return g

def buildVertices(graph):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        command = "SELECT node_id, lon, lat from nodes;"
        
        # execute a statement
        print('Executing command...')
        cur.execute(command)
 
        # display the result
        row = cur.fetchone()
 
        while row is not None:
            (node_id, lon, lat) = row
            #print lon, lat
            graph.addNode(node_id, lat, lon)
            row = cur.fetchone()
       
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
            
            
def buildEdges(graph, day, month, year):
    """ Connect to the PostgreSQL database server """
    conn = None
    edges = {}
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        #command = " SELECT begin_node_id, end_node_id, datetime, travel_time from travel_times where EXTRACT(DAY FROM datetime) = " + str(day) + " and \
        #EXTRACT(MONTH FROM datetime) = " + str(month) + " and \
        #EXTRACT(YEAR FROM datetime) =  " + str(year) + ";"
        
        sql_edges = "SELECT distinct node_from, node_to from speeds_movement_osm;"
        
        # execute a statement
        print('Executing command...')
        cur.execute(sql_edges)
 
        # display the result
        row = cur.fetchone()
        while row is not None:
            (node_from, node_to) = row
            graph.addEdge(node_from, node_to)
            
            #if node_from in edges:
            #    if node_to in edges[node_from]:
            #       print("Repeated edge!")
            #    edges[node_from].add(node_to)
            #else:
            #   edges[node_from] = {node_to}
            #print(node_from, node_to)
            row = cur.fetchone()
           
        ''' for node in edges:
            neig = edges[node]
            for node_neig in neig:
                print (node, node_neig)
                sql_travel_time = " SELECT datetime, travel_time from travel_times where EXTRACT(DAY FROM datetime) = " + str(day) + " and \
                EXTRACT(MONTH FROM datetime) = " + str(month) + " and \
                EXTRACT(YEAR FROM datetime) =  " + str(year) + "and \
                begin_node_id = " + str(node) + "and end_node_id = " + str(node_neig)+ ";"
                
                print('Executing command...')
                cur.execute(sql_travel_time)
                
                row = cur.fetchone()
 
                while row is not None:
                    print (row)
                    row = cur.fetchone() ''' 
             
            
       
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
            
if __name__ == '__main__':
    buildGraph(5, 10, 2013)
