'''
Created on Oct 16, 2019

@author: camila
'''
import psycopg2
from psycopg2 import pool

from ..conf.config import config

class PostGISConnectionPool:
    def __init__(self):
        params = config()
        self.pool = pool.ThreadedConnectionPool(**params)
        
    def getConnection(self):
        return self.pool.getconn()
    
    def closeConnection(self, conn):
        self.pool.putconn(conn)
        

class PostGISConnection:
    def __init__(self):
        self.pool = PostGISConnectionPool()
        self.conn = self.pool.getConnection()
        
    def connect(self):
        """ Connect to the PostgreSQL database server """
        try:
            # read connection parameters
            #params = config()
 
            # connect to the PostgreSQL server
            #self.conn = psycopg2.connect(**params)
            self.conn = self.pool.getConnection()
            return self.conn
       
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
    def getCursor(self):
        return self.conn.cursor()
    
    def commit(self):
        self.conn.commit()
            
    def executeCommand(self, sql):
        """ Connect to the PostgreSQL database server """
        if not self.conn:
            raise Exception('Connection not established!')
        try:
            # executing command
            cur = self.conn.cursor()
        
            cur.execute(sql)
            cur.close()
            self.conn.commit()
       
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
    def close(self, conn = None):
        """ Closing connection to the PostgreSQL database server """
        if not self.conn:
            raise Exception('Connection not established!')
        try:
            # executing command
            if conn is None:
                self.pool.closeConnection(self.conn)
            else:
                self.pool.closeConnection(conn)
            #self.conn.close()
       
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
