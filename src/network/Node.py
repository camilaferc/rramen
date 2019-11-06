'''
Created on Oct 2, 2019

@author: camila
'''

class Node:
    def __init__(self, node_id, lat, lon):
        self.id = node_id
        self.lat = lat
        self.lon = lon
        
    def __str__(self):
        return "id:" + str(self.id) + " lat:" + str(self.lat) + " long:" + str(self.lon)
    

