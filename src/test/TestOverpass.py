'''
Created on Nov 15, 2019

@author: camila
'''

from geopy.geocoders import Nominatim
import osmapi

geolocator = Nominatim(user_agent="test")
location = geolocator.reverse("52.521512, 13.411267")
place = geolocator.geocode({"osm_id": "62192860"})
print(place)
print(location.address)
print((location.latitude, location.longitude))
print(location.raw)

api = osmapi.OsmApi()
print(api.WayGet(62192860))
