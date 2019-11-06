'''
Created on Oct 2, 2019

@author: camila
'''
from travel_time_function.PiecewiseLinearFunction import PiecewiseLinearFunction

class Edge:
    def __init__(self, edge_id, node_from, node_to, edge_type, travel_time_function):
        self.id = edge_id
        self.node_from = node_from
        self.node_to = node_to
        self.type = edge_type
        self.travel_time_function = travel_time_function
        
    def __str__(self):
        return "id:" + str(self.id) + " from:" + str(self.node_from) + " to:" + str(self.node_to) + " type:" + str(self.type)
    
    def __repr__(self):
        return str(self)

    def getTravelTime(self, arrival_time):
        return self.travel_time_function.getTravelTime(arrival_time)
    

e1 = Edge(1, 5, 6, "Road", PiecewiseLinearFunction([(0, 5), (1, 5), (2, 4), (3, 5)]))
print (e1.getTravelTime(1))
print (e1)
