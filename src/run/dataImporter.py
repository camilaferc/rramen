'''
Created on Jan 24, 2020

@author: camila
'''

from pathlib import Path
import sys

from ..gtfs.GTFSImporter import GTFSImporter
from ..neighborhood.ImportBoundariesFromGeoJSON import ImportBoundariesFromGeoJSON
from ..transit.CreateTransportationNetwork import CreateTransportationNetwork


if __name__ == '__main__':
    region = sys.argv[1]
    
    data_dir = str(Path(__file__).resolve().parents[2]) + "/data/"
    
    #import boundaries
    boundaries_dir =  data_dir + "osm_boundaries/"+region +"/"
    importBoundaries = ImportBoundariesFromGeoJSON(boundaries_dir, region)
    importBoundaries.parse()
    
    #import gtfs data 
    gtfs_dir = data_dir + "gtfs/"+region +"/"
    gtfs = GTFSImporter(gtfs_dir, region)
    gtfs.run()
    
    #create public transit network
    create = CreateTransportationNetwork(region)
    create.run()
    
