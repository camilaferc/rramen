'''
Created on Oct 5, 2019

@author: camila
'''
import networkx as nx

class MultimodalNetwork:
    #modes
    PRIVATE = 1
    PUBLIC = 2
    PEDESTRIAN = 3
    
    #types
    ROAD = 1
    TRANSPORTATION = 2
    TRANSFER = 3
    SUPER_NODE = 4
    
    PEDESTRIAN_SPEED = 4 #km/h
    
    MIN_TRANSFER_TIME = 15
    
    def __init__(self):
        self.graph = nx.DiGraph()
        self.osmMapping = None
        
    def addNode(self, node_id, lat, lon, node_type, stop_id=None, route_id=None):
        if stop_id and route_id:
            self.graph.add_node(node_id, lat=lat, lon=lon, type=node_type, stop_id=stop_id, route_id=route_id)
        else:   
            self.graph.add_node(node_id, lat=lat, lon=lon, type=node_type)
    
    def addEdge(self, node_from, node_to, edge_type, modes, travel_time_functions, original_edge_id=None, edge_position=None):
        if not original_edge_id:
            self.graph.add_edge(node_from, node_to, type=edge_type, modes = modes, travel_time_functions = travel_time_functions)
        else:
            if edge_position:
                self.graph.add_edge(node_from, node_to, type=edge_type, modes = modes, travel_time_functions = travel_time_functions,
                                    original_edge_id=original_edge_id, edge_position=edge_position)
            else:
                self.graph.add_edge(node_from, node_to, type=edge_type, modes = modes, travel_time_functions = travel_time_functions,
                                    original_edge_id=original_edge_id)
        
    def getNeighbors(self, node_id):
        return list(self.graph.adj[node_id])
    
    def getTravelTimeToNeighbors(self, node_id, arrival_time, modes):
        neig_travel_times = {}
        #start = time.time()
        neigs = self.graph[node_id]
        for node_to in neigs:
            edge = neigs[node_to]
            for mode in edge["modes"]:
                #print(edge)
                if mode in modes:
                    if node_to in neig_travel_times:
                        cur_travel_time = neig_travel_times[node_to]
                        new_travel_time = edge['travel_time_functions'][mode].getTravelTime(arrival_time)
                        if new_travel_time < cur_travel_time:
                            neig_travel_times[node_to] = new_travel_time
                    else:
                        neig_travel_times[node_to] = edge['travel_time_functions'][mode].getTravelTime(arrival_time)
        #self.time_neighbors += (time.time()- start)    
        return neig_travel_times
    
    def setOsmMapping(self, osmMapping):
        self.osmMapping = osmMapping
        
    def getOsmMapping(self):
        return self.osmMapping
    
    '''
    def getTravelTimeToNeighbors(self, node_id, arrival_time, mode):
        neig_travel_times = {}
        #start = time.time()
        neigs = self.graph[node_id]
        for node_to in neigs:
            edge = neigs[node_to]
            if mode in edge["modes"]:
                #print(edge)
                neig_travel_times[node_to] = edge['travel_time_functions'][mode].getTravelTime(arrival_time)
        #self.time_neighbors += (time.time()- start)    
        return neig_travel_times
    '''
    
    def getEdge(self, node_from, node_to):
        if node_from in self.graph:
            if node_to in self.graph[node_from]:
                return self.graph[node_from][node_to]
        return None
    
    def getNode(self, node_id):
        return self.graph.nodes[node_id]
    
    def getNumNodes(self):
        return self.graph.number_of_nodes()
    
    def getNumEdges(self):
        return self.graph.number_of_edges()
    
    def getEdges(self):
        return self.graph.edges
    
    def getVertices(self):
        return self.graph.vertices
    
    def isConnected(self):
        visited = {}
        visited_rev = {}
        for n in self.graph.nodes:
            visited[n] = False
            visited_rev[n] = False
            v = n
        self.DFSUtil(v, visited)
        g_rev = self.getTranspose()
        g_rev.DFSUtil(v, visited_rev)
        
        for n in self.graph.nodes:
            if not visited[n] and not visited_rev[n]:
                #print(n)
                return False
        return True;
    
    def getConnectedComponents(self):
        components = []
        remaining_nodes = self.graph.nodes
        
        while remaining_nodes:
            visited = {}
            visited_rev = {}
            for n in remaining_nodes:
                visited[n] = False
                visited_rev[n] = False
                v = n
                
            #print("Forward search...")    
            self.DFSUtil(v, visited)
            g_rev = self.getTranspose()
            #print("Backward search...")  
            g_rev.DFSUtil(v, visited_rev)
    
            connected = set()
            not_connected = set()
        
            for n in remaining_nodes:
                if not visited[n] and not visited_rev[n]:
                    #print(n)
                    not_connected.add(n)
                else:
                    connected.add(n)
        
            print(len(connected))
            #if 103747443 in connected:
            #    print("103747443 is here!" )
            #if 760840186 in connected:
            #    print("760840186 is here!" )
            if len(connected) <= 10:
                print(connected)
            components.append(connected)
            remaining_nodes = not_connected
            
        return components
    
        
    # A function used by DFS 
    def DFSUtil(self,v,visited): 
        # Create a stack for DFS  
        stack = [] 
  
        # Push the current source node.  
        stack.append(v)  
  
        while (len(stack)):  
            # Pop a vertex from stack and print it  
            v = stack[-1]  
            stack.pop() 
  
            # Stack may contain same vertex twice. So  
            # we need to print the popped item only  
            # if it is not visited.  
            if (not visited[v]):  
                #print(v,end=' ') 
                visited[v] = True 
  
            # Get all adjacent vertices of the popped vertex s  
            # If a adjacent has not been visited, then push it  
            # to the stack.  
            for node in self.graph[v]:  
                #if node == 103747443:
                #    print("103747443 is expanded from:" + str(v))
                if node in visited:
                    if (not visited[node]):  
                        stack.append(node)  
                    
        '''            
        # Mark the current node as visited and print it 
        visited[v]= True
        print(v)
        #Recur for all the vertices adjacent to this vertex 
        for i in self.graph[v]: 
            if visited[i]==False: 
                self.DFSUtil(i,visited)
        '''        
    
    # Function that returns reverse (or transpose) of this graph 
    def getTranspose(self): 
        g = MultimodalNetwork()
        # Recur for all the vertices adjacent to this vertex 
        for i in self.graph: 
            for j in self.graph[i]: 
                g.addEdge(j,i) 
        return g 
    
