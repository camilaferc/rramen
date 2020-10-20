'''
Created on Oct 20, 2019

@author: camila
'''
import sys
import psycopg2

from ..database.PostGISConnection import PostGISConnection
from ..network.MultimodalNetwork import MultimodalNetwork
from ..travel_time_function.ConstantFunction import ConstantFunction
from ..travel_time_function.TimeTable import TimeTable


class LoadTransportationNetwork:
    
    def __init__(self, region, graph):
        self.region = region
        self.graph = graph
        self.conn = PostGISConnection()
        
    def load(self):
        print("Loading public transportation network...")
        
        self.conn.connect()
        self.loadNodes()
        self.loadEdges()
        self.conn.close()
    
    
    def loadNodes(self):
        #print("Loading nodes...")
        sql = """SELECT *
        FROM transportation_nodes_{0};
        """
        sql = sql.format(self.region)
        
        try:
            cursor = self.conn.getCursor()
            cursor.execute(sql)
            
            row = cursor.fetchone()
            
            while row is not None:
                (node_id, lat, lon, node_type, stop_id, route_id) = row
                self.graph.addNode(node_id, lat, lon, node_type, stop_id, route_id)
                row = cursor.fetchone()
                
            cursor.close()
                
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
    
    def loadEdges(self):
        #print("Loading edges...")
        sql_transp = """SELECT e.source, e.target, e.type, e.modes,
                t.monday, t.tuesday, t.wednesday, t.thursday, t.friday, t.saturday, t.sunday
                FROM transportation_edges_{0} e, transportation_timetables_{0} t 
                WHERE e.timetable_id = t.id;
        """
        sql_transp = sql_transp.format(self.region)
        
        sql_transfer = """SELECT source, target, type, modes, cost, original_edge_id, edge_position
                FROM transportation_edges_{0} 
                WHERE timetable_id is null;
        """
        sql_transfer = sql_transfer.format(self.region)
        
        try:
            cursor = self.conn.getCursor()
            
            cursor.execute(sql_transp)
            row = cursor.fetchone()
            
            while row is not None:
                (source, target, edge_type, modes, t_monday, t_tuesday, t_wednesday, t_thursday, t_friday, t_saturday, t_sunday) = row
                timetable = {}
                if t_monday:
                    timetable[0] = t_monday
                if t_tuesday:
                    timetable[1] = t_tuesday
                if t_wednesday:
                    timetable[2] = t_wednesday
                if t_thursday:
                    timetable[3] = t_thursday
                if t_friday:
                    timetable[4] = t_friday
                if t_saturday:
                    timetable[5] = t_saturday
                if t_sunday:
                    timetable[6] = t_sunday
                    
                self.graph.addEdge(source, target, edge_type, modes, {MultimodalNetwork.PUBLIC:TimeTable(timetable)})
                row = cursor.fetchone()
                
                
            cursor.execute(sql_transfer)
            row = cursor.fetchone()
            while row is not None:
                (source, target, edge_type, modes, cost, original_edge_id, edge_position) = row
                self.graph.addEdge(source, target, type, modes, {MultimodalNetwork.PEDESTRIAN:ConstantFunction(cost)}, original_edge_id, edge_position)
                row = cursor.fetchone()
                
            cursor.close()
            
                
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        
