'''
Created on Oct 2, 2019

@author: camila
'''

from datetime import timedelta
from threading import Thread, Event
import queue as Q


class Dijsktra:
    def __init__(self, graph):
        self.graph = graph
        self.timeout = 10

    def manyToManyPrivate(self, sources, targets, departure_time, removed_segments, travel_times, parents):
        if travel_times is None:
            travel_times = {}
        if parents is None:
            parents = {}
        for s in sources:
            # create another Thread
            travel_times_s = {}
            parents_s = {}
            stop_event = Event()
            
            if not removed_segments:
                action_thread = Thread(target=self.shortestPathToSetPrivate, args=[s, departure_time, targets, travel_times_s, parents_s, stop_event])
             
                # start the thread and we wait 'timeout' seconds before killing it.
                action_thread.start()
                action_thread.join(timeout=self.timeout)
                
             
                # Stop thread if still active
                stop_event.set()
        
            else:
                action_thread = Thread(target=self.shortestPathToSetPrivateSegmentsRemoved, args=[s, departure_time, targets, 
                                                                                                    removed_segments, travel_times_s, parents_s, stop_event])
                action_thread.start()
                action_thread.join(timeout=self.timeout)
                
                stop_event.set()
        
            travel_times[s] = travel_times_s
            parents[s] = parents_s
        return travel_times, parents
    
    def manyToManyPublic(self, sources, targets, departure_time, removed_routes = None, removed_stops = None, travel_times = None, parents = None):
        if travel_times is None:
            travel_times = {}
        if parents is None:
            parents = {}
        for s in sources:
            travel_times_s = {}
            parents_s = {}
            stop_event = Event()
            
            if not (removed_routes or removed_stops):
                action_thread = Thread(target=self.shortestPathToSetPublic, args=[s, departure_time, targets, {self.graph.PEDESTRIAN, self.graph.PUBLIC}, 
                                                                                   travel_times_s, parents_s, stop_event])
         
                action_thread.start()
                action_thread.join(self.timeout)
            
                stop_event.set()
            else:
                action_thread = Thread(target=self.shortestPathToSetPublicRoutesRemoved, args=[s, departure_time, targets, {self.graph.PEDESTRIAN, self.graph.PUBLIC}, 
                                                                                               removed_routes, removed_stops, travel_times_s, parents_s, stop_event])
                action_thread.start()
                action_thread.join(self.timeout)
            
                stop_event.set()
                                                                               
            travel_times[s] = travel_times_s
            parents[s] = parents_s
        return travel_times, parents
    
    def shortestPathToSetPrivate(self, s, departure_time, targets, travel_times=None, parents=None, stop_event = None):
        if travel_times is None:
            travel_times = {}
        if parents is None:
            parents = {}
        q = Q.PriorityQueue()
        closed_set = set()
        targets = set(targets)
        
        q.put((0, s, departure_time, -1, []))
        
        while not q.empty():
            if stop_event is not None and stop_event.is_set():
                break
            
            v_min = q.get()
            
            travel_time = v_min[0]
            vid = v_min[1]
            arrival_time = v_min[2]
            
            if vid not in closed_set:
                closed_set.add(vid)
                parents[vid] = v_min[3]
            
                if vid in targets:
                    travel_times[vid] = travel_time
                    targets.remove(vid)
                
                    if not targets:
                        return travel_times, parents
                
                #for mode in allowed_modes:
                modes = v_min[4]
                allowed_modes = self.getAllowedModesPrivate(modes)
                travel_times_neighbors = self.graph.getTravelTimeToNeighbors(vid, arrival_time, allowed_modes)
                for to in travel_times_neighbors:
                    if to not in closed_set:
                        tt, mode = travel_times_neighbors[to]
                        total_tt = travel_time + tt
                        arrival_time_to = arrival_time + timedelta(seconds=tt)
                        modes_to = modes.copy()
                        if len(modes_to) == 0 or  modes_to[-1] != mode:
                            modes_to.append(mode)
                            
                        q.put((total_tt, to, arrival_time_to, vid, modes_to))
        return travel_times, parents
    
    
    def shortestPathToSetPrivateSegmentsRemoved(self, s, departure_time, targets, removed_segments, travel_times=None, parents=None, stop_event = None):
        if travel_times is None:
            travel_times = {}
        if parents is None:
            parents = {}
        q = Q.PriorityQueue()
        closed_set = set()
        targets = set(targets)
        
        q.put((0, s, departure_time, -1, []))
        
        while not q.empty():
            if stop_event is not None and stop_event.is_set():
                break
            
            v_min = q.get()
            
                
            travel_time = v_min[0]
            vid = v_min[1]
            arrival_time = v_min[2]
            
            if vid not in closed_set:
                closed_set.add(vid)
                parents[vid] = v_min[3]
            
                if vid in targets:
                    travel_times[vid] = travel_time
                    targets.remove(vid)
                
                    if not targets:
                        return travel_times, parents
                
                #for mode in allowed_modes:
                modes = v_min[4]
                allowed_modes = self.getAllowedModesPrivate(modes)
                travel_times_neighbors = self.graph.getTravelTimeToNeighbors(vid, arrival_time, allowed_modes)
                removed_targets = removed_segments.get(vid)
                for to in travel_times_neighbors:
                    if removed_targets and to in removed_targets:
                        #segment was removed
                        continue
                    if to not in closed_set:
                        tt, mode = travel_times_neighbors[to]
                        total_tt = travel_time + tt
                        arrival_time_to = arrival_time + timedelta(seconds=tt)
                        modes_to = modes.copy()
                        if len(modes_to) == 0 or  modes_to[-1] != mode:
                            modes_to.append(mode)
                            
                        q.put((total_tt, to, arrival_time_to, vid, modes_to))
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
                
    def shortestPathToSetPublic(self, s, departure_time, targets, allowed_modes, travel_times=None, parents=None, stop_event = None):
        if travel_times is None:
            travel_times = {}
        if parents is None:
            parents = {}
        q = Q.PriorityQueue()
        closed_set = set()
        targets = set(targets)
        
        q.put((0, s, departure_time, -1))
        
        while not q.empty():
            if stop_event is not None and stop_event.is_set():
                break
            
            v_min = q.get()
            
            travel_time = v_min[0]
            vid = v_min[1]
            arrival_time = v_min[2]
            
            if vid not in closed_set:
                closed_set.add(vid)
                parent = v_min[3]
                parents[vid] = parent
            
                if vid in targets:
                    travel_times[vid] = travel_time
                    targets.remove(vid)
                
                    if not targets:
                        return travel_times, parents
                
                #checking if v_min is a super node to avoid re-expanding the transportation nodes
                out_of_station = False
                vid_type = self.graph.getNode(vid)['type']
                
                if vid_type == self.graph.SUPER_NODE:
                    if parent != -1:
                        parent_type = self.graph.getNode(parent)['type']
                        if parent_type == self.graph.TRANSPORTATION:
                            out_of_station = True
                travel_times_neighbors = self.graph.getTravelTimeToNeighbors(vid, arrival_time, allowed_modes)
                
                for to in travel_times_neighbors:
                    if to not in closed_set:
                        if out_of_station and self.graph.getNode(to)['type'] == self.graph.TRANSPORTATION:
                            continue
                        
                        tt, _ = travel_times_neighbors[to]
                        total_tt = travel_time + tt
                        arrival_time_to = arrival_time + timedelta(seconds=tt)
                        
                        q.put((total_tt, to, arrival_time_to, vid))
                        
        return travel_times, parents
    
    
    def shortestPathToSetPublicRoutesRemoved(self, s, departure_time, targets, allowed_modes, removed_routes, removed_stops, 
                                             travel_times=None, parents=None, stop_event = None):
        if travel_times is None:
            travel_times = {}
        if parents is None:
            parents = {}
        q = Q.PriorityQueue()
        closed_set = set()
        targets = set(targets)
        
        q.put((0, s, departure_time, -1))
        
        while not q.empty():
            if stop_event is not None and stop_event.is_set():
                break
            
            v_min = q.get()
            
            travel_time = v_min[0]
            vid = v_min[1]
            arrival_time = v_min[2]
            
            if vid not in closed_set:
                closed_set.add(vid)
                parent = v_min[3]
                parents[vid] = parent
            
                if vid in targets:
                    travel_times[vid] = travel_time
                    targets.remove(vid)
                
                    if not targets:
                        return travel_times, parents
                
                #checking if v_min is a super node to avoid re-expanding the transportation nodes
                out_of_station = False
                v_node = self.graph.getNode(vid)
                vid_type = v_node['type']
                
                    
                if vid_type == self.graph.SUPER_NODE:
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
                            #stop was removed
                            only_routes = True
                
                for to in travel_times_neighbors:
                    if to not in closed_set:
                        
                        nodeTo = self.graph.getNode(to)
                        if only_routes and nodeTo["type"] == self.graph.SUPER_NODE:
                            continue;
                        if "route_id" in nodeTo:
                            if removed_routes:
                                if nodeTo["route_id"] in removed_routes:
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
        
        while not q.empty():
            v_min = q.get()
            
                
            travel_time = v_min[0]
            vid = v_min[1]
            arrival_time = v_min[2]
            
            if vid not in closed_set:
                closed_set.add(vid)
                parents[vid] = v_min[3]
            
                if vid in targets:
                    travel_times[vid] = travel_time
                    targets.remove(vid)
                
                    if not targets:
                        return travel_times
                
                for mode in allowed_modes:
                    travel_times_neighbors = self.graph.getTravelTimeToNeighbors(vid, arrival_time, mode)
                    for to in travel_times_neighbors:
                        if to not in closed_set:
                            tt = travel_times_neighbors[to]
                            total_tt = travel_time + tt
                            arrival_time_to = arrival_time + timedelta(seconds=tt)
                            
                            q.put((total_tt, to, arrival_time_to, vid))
        return travel_times