'''
Created on Oct 2, 2019

@author: camila

nodes is a dictionary containing pairs (node_id:node)
edge is a dictionary containing pairs (node_id:[list_of_neighbors])
'''

from travel_time_function.PiecewiseLinearFunction import PiecewiseLinearFunction

class GraphTest:
    def __init__(self, nodes={}, edges={}):
        self.nodes = nodes
        self.edges = edges
        
    def addNode(self, node):
        self.nodes[node.id] = node
    
    def addEdge(self, edge):
        if edge.node_from in self.edges:
            self.edges[edge.node_from].append(edge)
        else:
            self.edges[edge.node_from] = [edge]
        
    def getTravelTimeToNeighbors(self, node_id, arrival_time):
        neig = self.edges.get(node_id)
        neig_travel_times = {}
        for n in neig:
            neig_travel_times[n.node_to] = n.getTravelTime(arrival_time)
        return neig_travel_times
    
    def getNeighborEdges(self, node_id):
        return self.edges.get(node_id)
    
    def getNumNodes(self):
        return len(self.nodes)
    
    def getNumEdges(self):
        return len(self.edges)
    

#e1 = Edge(1, 1, 2, "Road", PiecewiseLinearFunction([(0, 5), (1, 5), (2, 4), (3, 5)]))
#e2 = Edge(2, 2, 1, "Road", PiecewiseLinearFunction([(0, 5), (1, 5), (2, 4), (3, 5)]))

#g1 = Graph()
#g1.addEdge(e1)
#g1.addEdge(e2)
#print g1.edges


#print g1.getTravelTimeToNeighbors(2, 1.5)
#print str(g1.getNeighborEdges(2))

