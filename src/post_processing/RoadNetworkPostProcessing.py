'''
Created on Nov 25, 2019

@author: camila
'''
import sys

import psycopg2

from database.PostGISConnection import PostGISConnection
from load.LoadRoadNetwork import LoadRoadNetwork
from network.MultimodalNetwork import MultimodalNetwork
import networkx as nx


def checkNetworkConnection(region, network_type):
    graph = loadNetork(region, network_type)
    if nx.is_connected(graph):
        print("Graph is connected!")
        return
    else:
        print("Graph is disconnected!")
        connected_components = nx.connected_components(graph)
        num_nodes = graph.number_of_nodes()
        count = 0
        
        sql_remove = """
            DELETE FROM roadnet_{0}
            WHERE id = {1};
        """
        
        connection = PostGISConnection()
        connection.connect()
        cursor = connection.getCursor()
        
        for c in sorted(connected_components, key=len, reverse=True):
            print(len(c))
            count += 1
            
            if len(c) > num_nodes * 0.01:
                print("Component is large:" + str(len(c)))
                continue
            else:
                for n1 in c:
                    #print(n1)
                    #print(graph_undirected[n1])
                    for n2 in c:
                        #print(n1, n2)
                        edge = None
                        if n1 in graph:
                            if n2 in graph[n1]:
                                edge = graph[n1][n2]
                        if edge:
                            original_edge = edge["original_edge_id"]
                            sql_remove_id = sql_remove.format(region, original_edge)
                            cursor.execute(sql_remove_id)
                            
                    #print("Commiting changes...")
                    #connection.conn.commit()
        print(count)
    

def loadNetork(region, network_type):
    print("Loading road network...")
    sql = """SELECT id, osm_source_id, osm_target_id, clazz, km, kmh, ST_X(source_location), ST_Y(source_location), 
        ST_X(target_location), ST_Y(target_location), reverse_cost
        FROM roadnet_{};
        """
    sql = sql.format(region)
    conn = None
    osm_mapping = {}
    graph = nx.Graph()
    node_id = 0
    try:
        conn = PostGISConnection()
        conn.connect()
        cursor = conn.conn.cursor()
        cursor.execute(sql)
        
        row = cursor.fetchone()
        
        while row is not None:
            (edge_id, osm_source_id, osm_target_id, clazz, length, speed, x_source, y_source, 
             x_target, y_target, reverse_cost) = row
            #print(id, osm_source_id, osm_target_id)
            
            if network_type == MultimodalNetwork.PEDESTRIAN:
                if clazz not in MultimodalNetwork.PEDESTRIAN_WAYS:
                    row = cursor.fetchone()
                    continue
            elif network_type == MultimodalNetwork.PRIVATE:
                if clazz not in MultimodalNetwork.CAR_WAYS:
                    row = cursor.fetchone()
                    continue
            
            if osm_source_id not in osm_mapping:
                graph.add_node(node_id, lat=x_source, lon=y_source)
                osm_mapping[osm_source_id] = node_id
                node_id += 1
                
            
            if osm_target_id not in osm_mapping:
                graph.add_node(node_id, lat=x_target, lon=y_target)
                osm_mapping[osm_target_id] = node_id
                node_id += 1
                
            node_from = osm_mapping[osm_source_id]
            node_to = osm_mapping[osm_target_id]
            
            graph.add_edge(node_from, node_to, cost=length, original_edge_id = edge_id)
                
            row = cursor.fetchone()
        return graph
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    except: 
        print("Unexpected error:", sys.exc_info()[0])
    finally:
        if conn is not None:
            conn.close()


def removeDisconnectedComponents(region):
    network = MultimodalNetwork()
    load = LoadRoadNetwork(region, network)
    load.load()
    
    print(network.getNumNodes())
    print(network.getNumEdges())
    
    print("Creating undirected graph...")
    graph_undirected = network.graph.to_undirected()
    network = None
    
    if nx.is_connected(graph_undirected):
        print("Graph is connected!")
        return
    else:
        print("Graph is disconnected!")
        connected_components = nx.connected_components(graph_undirected)
        num_nodes = graph_undirected.number_of_nodes()
        #largest_cc = max(nx.connected_components(graph_undirected), key=len)
        count = 0
        
        sql_remove = """
            DELETE FROM roadnet_{0}
            WHERE id = {1};
        """
        
        connection = PostGISConnection()
        connection.connect()
        cursor = connection.getCursor()
            
        for c in sorted(connected_components, key=len, reverse=True):
            print(len(c))
            count += 1
            if len(c) > num_nodes * 0.05:
                print("Component is large:" + str(len(c)))
                continue
            else:
                for n1 in c:
                    #print(n1)
                    #print(graph_undirected[n1])
                    for n2 in c:
                        #print(n1, n2)
                        edge = None
                        if n1 in graph_undirected:
                            if n2 in graph_undirected[n1]:
                                edge = graph_undirected[n1][n2]
                        if edge:
                            #print(edge)
                            original_edge = edge["original_edge_id"]
                            sql_remove_id = sql_remove.format(region, original_edge)
                            cursor.execute(sql_remove_id)
            
            
                #print("Commiting changes...")
                #connection.conn.commit()
        print(count)

#removeDisconnectedComponents("berlin")
checkNetworkConnection("berlin", MultimodalNetwork.PEDESTRIAN)
