'''
Created on Oct 2, 2019

@author: camila
'''

from util.FibonacciHeap import FibonacciHeap
from datetime import timedelta
import time
import queue as Q


class Dijsktra:
    def __init__(self, graph):
        self.graph = graph
        self.parents = {}
    
    def shortestPathToSetPrivate(self, s, departure_time, targets, allowed_modes):
        q = Q.PriorityQueue()
        travel_times = {}
        closed_set = set()
        targets = set(targets)
        self.parents = {}
        
        q.put((0, s, departure_time, -1))
        
        time_neig = 0
        
        while not q.empty():
            v_min = q.get()
            
            #if not v_min:
            #    print(heap)
                
            travel_time = v_min[0]
            vid = v_min[1]
            arrival_time = v_min[2]
            
            #print(vid, travel_time)
            if vid not in closed_set:
                closed_set.add(vid)
                self.parents[vid] = v_min[3]
            
                if vid in targets:
                    print(str(vid) + ":" + str(travel_time) + " " + str(self.graph.getNode(vid)))
                    travel_times[vid] = travel_time
                    targets.remove(vid)
                
                    if not targets:
                        #print(heapNodes)
                        print(time_neig)
                        return travel_times
                
                #for mode in allowed_modes:
                start = time.time()
                travel_times_neighbors = self.graph.getTravelTimeToNeighbors(vid, arrival_time, allowed_modes)
                time_neig += (time.time()-start)
                #if mode == self.graph.PUBLIC:
                #    print(travel_times_neighbors)
                for to in travel_times_neighbors:
                    if to not in closed_set:
                        tt = travel_times_neighbors[to]
                        total_tt = travel_time + tt
                        arrival_time_to = arrival_time + timedelta(seconds=tt)
                        
                        q.put((total_tt, to, arrival_time_to, vid))
        print(time_neig)                        
        return travel_times
    
    def shortestPathToSetPublic(self, s, departure_time, targets, allowed_modes):
        q = Q.PriorityQueue()
        travel_times = {}
        closed_set = set()
        targets = set(targets)
        self.parents = {}
        
        q.put((0, s, departure_time, -1))
        
        time_neig = 0
        
        while not q.empty():
            v_min = q.get()
            
            #if not v_min:
            #    print(heap)
                
            travel_time = v_min[0]
            vid = v_min[1]
            arrival_time = v_min[2]
            
            #print(vid, travel_time)
            if vid not in closed_set:
                closed_set.add(vid)
                parent = v_min[3]
                self.parents[vid] = parent
            
                if vid in targets:
                    print(str(vid) + ":" + str(travel_time) + " " + str(self.graph.getNode(vid)))
                    travel_times[vid] = travel_time
                    targets.remove(vid)
                
                    if not targets:
                        #print(heapNodes)
                        print(time_neig)
                        return travel_times
                
                #checking if v_min is a super node to avoid re-expanding the transportation nodes
                out_of_station = False
                vid_type = self.graph.getNode(vid)['type']
                
                #if vid_type == self.graph.SUPER_NODE:
                #    print(v_min, self.graph.getNode(vid))
                    
                if vid_type == self.graph.SUPER_NODE:
                    if parent != -1:
                        parent_type = self.graph.getNode(parent)['type']
                        if parent_type == self.graph.TRANSPORTATION:
                            out_of_station = True
                #for mode in allowed_modes:
                start = time.time()
                travel_times_neighbors = self.graph.getTravelTimeToNeighbors(vid, arrival_time, allowed_modes)
                time_neig += (time.time()-start)
                
                #if mode == self.graph.PUBLIC:
                #    print(travel_times_neighbors)
                for to in travel_times_neighbors:
                    if to not in closed_set:
                        if out_of_station and self.graph.getNode(to)['type'] == self.graph.TRANSPORTATION:
                            continue
                        
                        tt = travel_times_neighbors[to]
                        total_tt = travel_time + tt
                        arrival_time_to = arrival_time + timedelta(seconds=tt)
                        
                        #edge = self.graph.getEdge(vid, to)
                        #if 'route_id' in edge and edge['route_id'] == "17449_700":
                        #    print(total_tt, to, arrival_time_to, vid)
                            
                        q.put((total_tt, to, arrival_time_to, vid))
                        
        print(time_neig)                        
        return travel_times
    
    def shortestPathToSet(self, s, departure_time, targets, allowed_modes):
        q = Q.PriorityQueue()
        travel_times = {}
        closed_set = set()
        targets = set(targets)
        self.parents = {}
        
        q.put((0, s, departure_time, -1))
        
        time_neig = 0
        
        while not q.empty():
            v_min = q.get()
            
            #if not v_min:
            #    print(heap)
                
            travel_time = v_min[0]
            vid = v_min[1]
            arrival_time = v_min[2]
            
            #print(vid, travel_time)
            if vid not in closed_set:
                closed_set.add(vid)
                self.parents[vid] = v_min[3]
            
                if vid in targets:
                    print(str(vid) + ":" + str(travel_time) + " " + str(self.graph.getNode(vid)))
                    travel_times[vid] = travel_time
                    targets.remove(vid)
                
                    if not targets:
                        #print(heapNodes)
                        print(time_neig)
                        return travel_times
                
                for mode in allowed_modes:
                    start = time.time()
                    travel_times_neighbors = self.graph.getTravelTimeToNeighbors(vid, arrival_time, mode)
                    time_neig += (time.time()-start)
                    #if mode == self.graph.PUBLIC:
                    #    print(travel_times_neighbors)
                    for to in travel_times_neighbors:
                        if to not in closed_set:
                            tt = travel_times_neighbors[to]
                            total_tt = travel_time + tt
                            arrival_time_to = arrival_time + timedelta(seconds=tt)
                            
                            q.put((total_tt, to, arrival_time_to, vid))
        print(time_neig)                        
        return travel_times
    
    def reconstructPathToNode(self, node_id):
        if node_id not in self.parents:
            raise Exception('{} has not been expanded'.format(node_id))
        else:
            path = []
            path.append(node_id)
            parent = self.parents[node_id]
            while parent != -1:
                #print(parent)
                path.append(parent)
                parent = self.parents[parent]
            path.reverse()
            return path
                
    
    def shortestPathToSetHeap(self, s, departure_time, targets, allowed_modes):
        heap = FibonacciHeap()
        heapNodes = {}
        travel_times = {}
        closed_set = set()
        
        time_neig = 0
        
        heap.insert(0, (s, departure_time))
        
        while heap.total_nodes:
            print(heap.total_nodes)
            v_min = heap.extract_min()
            
            #if not v_min:
            #    print(heap)
                
            travel_time = v_min.key
            vid = v_min.value[0]
            arrival_time = v_min.value[1]
            
            print(vid, travel_time)
            if vid not in closed_set:
                closed_set.add(vid)
            
                if vid in targets:
                    print(str(vid) + ":" + str(travel_time) + " " + str(self.graph.getNode(vid)))
                    travel_times[vid] = travel_time
                    targets.remove(vid)
                
                    if not targets:
                        #print(heapNodes)
                        print(self.graph.time_neighbors) 
                        print(time_neig)
                        return travel_times
                
                for mode in allowed_modes:
                    start = time.time()
                    travel_times_neighbors = self.graph.getTravelTimeToNeighbors(vid, arrival_time, mode)
                    time_neig += (time.time()-start)
                    for to in travel_times_neighbors:
                        if to not in closed_set:
                            tt = travel_times_neighbors[to]
                            total_tt = travel_time + tt
                            arrival_time_to = arrival_time + timedelta(seconds=tt)
                            if to in heapNodes:
                                if total_tt < heapNodes[to].key:
                                    #print("Updating distance")
                                    
                                    #print(heapNodes[to].key, heapNodes[to].value)
                                    heap.decrease_key(heapNodes[to], total_tt, (to, arrival_time_to))
                                    #print(heapNodes[to].key, heapNodes[to].value)
                                    
                                    
                            else:
                                node = heap.insert(total_tt, (to, arrival_time_to))
                                heapNodes[to] = node
                                
        print(self.graph.time_neighbors)        
        print(time_neig)                        
        return travel_times
            
