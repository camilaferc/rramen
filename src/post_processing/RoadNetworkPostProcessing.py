'''
Created on Nov 25, 2019

@author: camila
'''
from database.PostGISConnection import PostGISConnection
from load.LoadRoadNetwork import LoadRoadNetwork
from network.MultimodalNetwork import MultimodalNetwork
import networkx as nx


def removeDisconnectedComponents(region):
    network = MultimodalNetwork()
    load = LoadRoadNetwork(region, network)
    load.load()
    
    print(network.getNumNodes())
    print(network.getNumEdges())
    
    print("Creating undirected graph...")
    graph_undirected = network.graph.to_undirected()
    osm_mapping = network.getOsmMapping()
    network = None
    
    if nx.is_connected(graph_undirected):
        print("Graph is connected!")
        return
    else:
        print("Graph is disconnected!")
        connected_components = nx.connected_components(graph_undirected)
        num_nodes = graph_undirected.number_of_nodes()
        largest_cc = max(nx.connected_components(graph_undirected), key=len)
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

removeDisconnectedComponents("berlin")
