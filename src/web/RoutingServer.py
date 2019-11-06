'''
Created on Oct 29, 2019

@author: camila
'''

from decorator import append
from flask import Flask, request, session, g, redirect, \
    url_for, abort, render_template, flash
from geojson import Feature, Point, dumps, GeometryCollection, FeatureCollection
import requests

from database.PostgisDataManager import PostgisDataManager
from load.LoadMultimodalNetwork import LoadMultimodalNetwork

class RoutingServer:
    def __init__(self):
        self.key = 'pk.eyJ1IjoiY2FtaWxhZmVyYyIsImEiOiJjazB3aGJ5emkwMzNqM29tbWxkZ2t3OWJwIn0.LYcGltgmo4yj5zqhDJGoEA'
        self.graph = None
        
        self.app = Flask(__name__)
        print(self.app)
        #self.app.config.from_object(__name__)
        self.app.add_url_rule('/rr_test', self.load())
        #self.app.add_url_rule()
        

    #@app.route('/rr_test')
    def load(self):
        #load = LoadMultimodalNetwork("berlin")
        #graph = load.load()
        return render_template('rr_test.html', 
            ACCESS_KEY=self.key
        )
        
    def run(self):
        self.app.run()
        
if __name__ == '__main__':
    # run!
    server = RoutingServer()
    server.run()

