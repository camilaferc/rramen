'''
Created on Oct 2, 2019

@author: camila
'''

from datetime import timedelta
from multiprocessing import Manager
import multiprocessing
from threading import Thread
import time

import queue as Q
from util.FibonacciHeap import FibonacciHeap
from posix import remove


class Dijsktra:
    def __init__(self, graph):
        self.graph = graph
        self.timeout = 10

    def manyToManyPrivate(self, sources, targets, departure_time, removed_segments, travel_times, parents):
        print("Computing travel times by car...")
        if travel_times is None:
            travel_times = {}
        if parents is None:
            parents = {}
        for s in sources:
            # create another Thread
            manager = Manager()
            travel_times_s = manager.dict()
            parents_s = manager.dict()
            
            if not removed_segments:
                action_thread = multiprocessing.Process(target=self.shortestPathToSetPrivate, args=[s, departure_time, targets, travel_times_s, parents_s])
             
                # start the thread and we wait 'timeout' seconds before the code continues to execute.
                action_thread.start()
                action_thread.join(self.timeout)
                
             
                # If thread is still active
                if action_thread.is_alive():
                    print ("Private TIMED OUT")
                    # Terminate
                    action_thread.terminate()
                    action_thread.join()
        
                #travel_times_s, parents_s = self.shortestPathToSetPrivate(s, departure_time, targets)
                #print(travel_times_s)
            else:
                action_thread = multiprocessing.Process(target=self.shortestPathToSetPrivateSegmentsRemoved, args=[s, departure_time, targets, 
                                                                                                    removed_segments, travel_times_s, parents_s])
             
                # start the thread and we wait 'timeout' seconds before the code continues to execute.
                action_thread.start()
                action_thread.join(self.timeout)
                
             
                # If thread is still active
                if action_thread.is_alive():
                    print ("Private TIMED OUT")
                    # Terminate
                    action_thread.terminate()
                    action_thread.join()
        
            travel_times[s] = travel_times_s
            parents[s] = parents_s
        return travel_times, parents
    
    def manyToManyPublic(self, sources, targets, departure_time, removed_routes = None, removed_stops = None, travel_times = None, parents = None):
        print("Computing travel times by public transit...")
        if travel_times is None:
            travel_times = {}
        if parents is None:
            parents = {}
        for s in sources:
            manager = Manager()
            travel_times_s = manager.dict()
            parents_s = manager.dict()
            
            if not (removed_routes or removed_stops):
                action_thread = multiprocessing.Process(target=self.shortestPathToSetPublic, args=[s, departure_time, targets, {self.graph.PEDESTRIAN, self.graph.PUBLIC}, 
                                                                                   travel_times_s, parents_s])
         
                # start the thread and we wait 'timeout' seconds before the code continues to execute.
                action_thread.start()
                action_thread.join(self.timeout)
            
         
                # If thread is still active
                if action_thread.is_alive():
                    print ("Public TIMED OUT")
                    # Terminate
                    action_thread.terminate()
                    action_thread.join()
            
                #travel_times_s, parents_s = self.shortestPathToSetPublic(s, departure_time, targets, {self.graph.PEDESTRIAN, self.graph.PUBLIC})
            else:
                action_thread = multiprocessing.Process(target=self.shortestPathToSetPublicRoutesRemoved, args=[s, departure_time, targets, {self.graph.PEDESTRIAN, self.graph.PUBLIC}, 
                                                                                               removed_routes, removed_stops, travel_times_s, parents_s])
         
                # start the thread and we wait 'timeout' seconds before the code continues to execute.
                action_thread.start()
                action_thread.join(self.timeout)
            
         
                # If thread is still active
                if action_thread.is_alive():
                    print ("Public TIMED OUT")
                    # Terminate
                    action_thread.terminate()
                    action_thread.join()
                #travel_times_s, parents_s = self.shortestPathToSetPublicRoutesRemoved(s, departure_time, targets, {self.graph.PEDESTRIAN, self.graph.PUBLIC}, 
                #removed_routes, removed_stops)
                                                                               
            #print(travel_times_s)
            travel_times[s] = travel_times_s
            parents[s] = parents_s
        return travel_times, parents
    
    def shortestPathToSetPrivate(self, s, departure_time, targets, travel_times=None, parents=None):
        print("shortestPathToSetPrivate")
        if travel_times is None:
            travel_times = {}
        if parents is None:
            parents = {}
        q = Q.PriorityQueue()
        closed_set = set()
        targets = set(targets)
        
        q.put((0, s, departure_time, -1, []))
        
        time_neig = 0
        
        while not q.empty():
            
            v_min = q.get()
            
                
            travel_time = v_min[0]
            vid = v_min[1]
            arrival_time = v_min[2]
            
            #print(vid, travel_time)
            if vid not in closed_set:
                closed_set.add(vid)
                parents[vid] = v_min[3]
            
                if vid in targets:
                    #print(str(vid) + ":" + str(travel_time) + " " + str(self.graph.getNode(vid)))
                    travel_times[vid] = travel_time
                    targets.remove(vid)
                
                    if not targets:
                        #print("All targets found!")
                        return travel_times, parents
                
                #for mode in allowed_modes:
                start = time.time()
                modes = v_min[4]
                allowed_modes = self.getAllowedModesPrivate(modes)
                travel_times_neighbors = self.graph.getTravelTimeToNeighbors(vid, arrival_time, allowed_modes)
                time_neig += (time.time()-start)
                for to in travel_times_neighbors:
                    if to not in closed_set:
                        tt, mode = travel_times_neighbors[to]
                        total_tt = travel_time + tt
                        arrival_time_to = arrival_time + timedelta(seconds=tt)
                        modes_to = modes.copy()
                        if len(modes_to) == 0 or  modes_to[-1] != mode:
                            modes_to.append(mode)
                            
                        q.put((total_tt, to, arrival_time_to, vid, modes_to))
        print(time_neig)                        
        return travel_times, parents
    
    
    def shortestPathToSetPrivateSegmentsRemoved(self, s, departure_time, targets, removed_segments, travel_times=None, parents=None):
        print("shortestPathToSetPrivateSegmentsRemoved")
        if travel_times is None:
            travel_times = {}
        if parents is None:
            parents = {}
        q = Q.PriorityQueue()
        closed_set = set()
        targets = set(targets)
        
        q.put((0, s, departure_time, -1, []))
        
        time_neig = 0
        
        while not q.empty():
            
            v_min = q.get()
            
                
            travel_time = v_min[0]
            vid = v_min[1]
            arrival_time = v_min[2]
            
            #print(vid, travel_time)
            if vid not in closed_set:
                closed_set.add(vid)
                parents[vid] = v_min[3]
            
                if vid in targets:
                    #print(str(vid) + ":" + str(travel_time) + " " + str(self.graph.getNode(vid)))
                    travel_times[vid] = travel_time
                    targets.remove(vid)
                
                    if not targets:
                        #print("All targets found!")
                        return travel_times, parents
                
                #for mode in allowed_modes:
                start = time.time()
                modes = v_min[4]
                allowed_modes = self.getAllowedModesPrivate(modes)
                travel_times_neighbors = self.graph.getTravelTimeToNeighbors(vid, arrival_time, allowed_modes)
                time_neig += (time.time()-start)
                removed_targets = removed_segments.get(vid)
                for to in travel_times_neighbors:
                    if removed_targets and to in removed_targets:
                        print("Removed segments:", vid, to)
                        continue
                    if to not in closed_set:
                        tt, mode = travel_times_neighbors[to]
                        total_tt = travel_time + tt
                        arrival_time_to = arrival_time + timedelta(seconds=tt)
                        modes_to = modes.copy()
                        if len(modes_to) == 0 or  modes_to[-1] != mode:
                            modes_to.append(mode)
                            
                        q.put((total_tt, to, arrival_time_to, vid, modes_to))
        print(time_neig)                        
        return travel_times, parents
    
    def getAllowedModesPrivate(self, modes):
        if len(modes) < 2:
            allowed_modes = {self.graph.PEDESTRIAN, self.graph.PRIVATE}
        elif len(modes) == 2:
            last_mode = modes[-1]
            if last_mode == self.graph.PEDESTRIAN:
                allowed_modes = {self.graph.PEDESTRIAN}
            else:
                allowed_modes = {self.graph.PEDESTRIAN, self.graph.PRIVATE}
        else:
            allowed_modes = {self.graph.PEDESTRIAN}
        
        return allowed_modes
                
    def shortestPathToSetPublic(self, s, departure_time, targets, allowed_modes, travel_times=None, parents=None):
        print("shortestPathToSetPublic")
        if travel_times is None:
            travel_times = {}
        if parents is None:
            parents = {}
        q = Q.PriorityQueue()
        closed_set = set()
        targets = set(targets)
        
        q.put((0, s, departure_time, -1))
        
        time_neig = 0
        
        while not q.empty():
            
            v_min = q.get()
            
            travel_time = v_min[0]
            vid = v_min[1]
            arrival_time = v_min[2]
            
            #print(vid, travel_time)
            if vid not in closed_set:
                closed_set.add(vid)
                parent = v_min[3]
                parents[vid] = parent
            
                if vid in targets:
                    #print(str(vid) + ":" + str(travel_time) + " " + str(self.graph.getNode(vid)))
                    travel_times[vid] = travel_time
                    targets.remove(vid)
                
                    if not targets:
                        #print(time_neig)
                        print("All targets found!")
                        return travel_times, parents
                
                #checking if v_min is a super node to avoid re-expanding the transportation nodes
                out_of_station = False
                vid_type = self.graph.getNode(vid)['type']
                
                if vid_type == self.graph.SUPER_NODE:
                    #print("SUPER NODE:" + str(self.graph.getNode(vid)))
                    #print(v_min)
                    if parent != -1:
                        parent_type = self.graph.getNode(parent)['type']
                        if parent_type == self.graph.TRANSPORTATION:
                            out_of_station = True
                start = time.time()
                travel_times_neighbors = self.graph.getTravelTimeToNeighbors(vid, arrival_time, allowed_modes)
                time_neig += (time.time()-start)
                
                for to in travel_times_neighbors:
                    if to not in closed_set:
                        if out_of_station and self.graph.getNode(to)['type'] == self.graph.TRANSPORTATION:
                            continue
                        
                        tt, _ = travel_times_neighbors[to]
                        total_tt = travel_time + tt
                        arrival_time_to = arrival_time + timedelta(seconds=tt)
                        
                        q.put((total_tt, to, arrival_time_to, vid))
                        
        #print(time_neig)                        
        return travel_times, parents
    
    
    def shortestPathToSetPublicRoutesRemoved(self, s, departure_time, targets, allowed_modes, removed_routes, removed_stops, travel_times=None, parents=None):
        print("shortestPathToSetPublicRoutesRemoved")
        if travel_times is None:
            travel_times = {}
        if parents is None:
            parents = {}
        q = Q.PriorityQueue()
        closed_set = set()
        targets = set(targets)
        
        q.put((0, s, departure_time, -1))
        
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
                parents[vid] = parent
            
                if vid in targets:
                    #print(str(vid) + ":" + str(travel_time) + " " + str(self.graph.getNode(vid)))
                    travel_times[vid] = travel_time
                    targets.remove(vid)
                
                    if not targets:
                        #print(heapNodes)
                        return travel_times, parents
                
                #checking if v_min is a super node to avoid re-expanding the transportation nodes
                out_of_station = False
                v_node = self.graph.getNode(vid)
                vid_type = v_node['type']
                
                #if vid_type == self.graph.SUPER_NODE:
                #    print(v_min, self.graph.getNode(vid))
                    
                if vid_type == self.graph.SUPER_NODE:
                    #print("SUPER NODE:" + str(self.graph.getNode(vid)))
                    #print(v_min)
                    if parent != -1:
                        parent_type = self.graph.getNode(parent)['type']
                        if parent_type == self.graph.TRANSPORTATION:
                            out_of_station = True
                travel_times_neighbors = self.graph.getTravelTimeToNeighbors(vid, arrival_time, allowed_modes)
                
                only_routes = False
                if vid_type == self.graph.PUBLIC:
                    stop_id = v_node["stop_id"]
                    route_id = v_node["route_id"]
                    if removed_stops:
                        if stop_id in removed_stops and route_id in removed_stops[stop_id]:
                            print("Removed stop:" + str(stop_id))
                            only_routes = True
                
                for to in travel_times_neighbors:
                    if to not in closed_set:
                        
                        nodeTo = self.graph.getNode(to)
                        if only_routes and nodeTo["type"] == self.graph.SUPER_NODE:
                            #print("Path from " + str(vid) + " to super node pruned!")
                            continue;
                        if "route_id" in nodeTo:
                            if removed_routes:
                                if nodeTo["route_id"] in removed_routes:
                                    #print("Removed route:" + str(nodeTo["route_id"]))
                                    continue
                            
                        if out_of_station and nodeTo['type'] == self.graph.TRANSPORTATION:
                            continue
                        
                        tt, _ = travel_times_neighbors[to]
                        total_tt = travel_time + tt
                        arrival_time_to = arrival_time + timedelta(seconds=tt)
                        
                        q.put((total_tt, to, arrival_time_to, vid))
                        
        return travel_times, parents
    
    def shortestPathToSet(self, s, departure_time, targets, allowed_modes):
        q = Q.PriorityQueue()
        travel_times = {}
        closed_set = set()
        targets = set(targets)
        parents = {}
        
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
                parents[vid] = v_min[3]
            
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
    
    '''
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
    '''            
    
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
            
