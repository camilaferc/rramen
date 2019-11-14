'''
Created on Nov 7, 2019

@author: camila
'''

from geojson.geometry import LineString

class Path(object):
    def __init__(self, parent_tree):
        self.parent_tree = parent_tree
    
    
    def reconstructPathToNode(self, node_id):
        if node_id not in self.parent_tree:
            raise Exception('{} has not been expanded'.format(node_id))
        else:
            path = []
            path.append(node_id)
            parent = self.parent_tree[node_id]
            while parent != -1:
                #print(parent)
                path.append(parent)
                parent = self.parent_tree[parent]
            path.reverse()
            return path
    
    
    def getPathGeometry(self, graph, node_id, region): 
        path = self.reconstructPathToNode(node_id)
    
        print(len(path))
        
        path_geometry = [] 
        for i in range(len(path) -1):
            node_from_id = path[i]
            node_to_id = path[i+1]
           
            node_from = graph.getNode(node_from_id)
            node_to = graph.getNode(node_to_id)
           
            if node_from["type"] == graph.ROAD and node_to["type"] == graph.ROAD:
                #retrieve geometry from database
                edge = graph.getEdge(node_from_id, node_to_id)
                original_edge = edge["original_edge_id"]
                if edge["type"] == graph.ROAD:
                    #print("ROAD")
                    '''
                    geometry = dataManager.getRoadGeometry(original_edge, region)
                    line = loads(geometry)
                    coord = line.coordinates
                    if i < len(path) - 2:
                        coord = coord[:-1]
                    path_geometry.extend(coord)
                    '''
                    path_geometry.append([node_from['lon'], node_from['lat']])
                    if i == len(path) - 2:
                        path_geometry.append([node_to['lon'], node_to['lat']]) 
                elif edge["type"] == graph.TRANSFER:
                    #print("TRANSFER")
                    '''
                    geometry = dataManager.getLinkGeometry(original_edge, edge["edge_position"], region)
                    #print(original_edge)
                    if not geometry:
                        path_geometry.append([node_from['lon'], node_from['lat']])
                        if i == len(path) - 2:
                            path_geometry.append([node_to['lon'], node_to['lat']]) 
                    else:
                        line = loads(geometry)
                        coord = line.coordinates
                        if i < len(path) - 2:
                            coord = coord[:-1]
                        path_geometry.extend(coord)
                    '''
                    path_geometry.append([node_from['lon'], node_from['lat']])
                    if i == len(path) - 2:
                        path_geometry.append([node_to['lon'], node_to['lat']]) 
            else:
                #print("DIRECT")
                path_geometry.append([node_from['lon'], node_from['lat']])
                if i == len(path) - 2:
                    path_geometry.append([node_to['lon'], node_to['lat']]) 
         
        #print(path_geometry)
        return LineString(path_geometry)      