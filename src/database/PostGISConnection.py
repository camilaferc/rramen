'''
Created on Oct 16, 2019

@author: camila
'''
import psycopg2
from conf.config import config

class PostGISConnection:
    def __init__(self):
        self.conn = None
        
    def connect(self):
        """ Connect to the PostgreSQL database server """
        try:
            # read connection parameters
            params = config()
 
            # connect to the PostgreSQL server
            self.conn = psycopg2.connect(**params)
       
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
            
    def close(self):
        """ Closing connection to the PostgreSQL database server """
        if not self.conn:
            raise Exception('Connection not established!')
        try:
            # executing command
            self.conn.close()
       
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
