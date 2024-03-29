'''
Created on Nov 24, 2019

@author: camila
'''

ROUTE_TYPE = {0: "Light Rail", 1: "Suburban Railway", 2: "Railway", 3:"Bus", 100: "Railway", 109: "Suburban Railway", 400:"Urban Railway", 700: "Bus", 900: "Tram"}

ROUTE_LEVEL = {0:4, 1:2, 2:1, 3:5, 100: 1, 109: 2, 400:3, 700: 5, 900: 4}

'''
* REQUIRED. One or more transit agencies that provide the data in this feed.
'''
AGENCY_FILE = "agency.txt"

'''
* CONDITIONAL. Dates for service IDs using a weekly schedule. Specify when service starts and ends, as well as days of the week where service is available.
'''
CALENDAR_FILE = "calendar.txt";

'''
* CONDITIONAL (REQUIRED IF "calendar.txt" IS NOT PROVIDED). Explicitly activate or disable service by date..
'''
CALENDAR_DATE_FILE = "calendar_dates.txt";

'''
* REQUIRED. Individual locations where vehicles pick up or drop off passengers.
'''
STOPS_FILE = "stops.txt";

'''
* REQUIRED. Transit routes. A route is a group of trips that are displayed to riders as a single service.
'''
ROUTES_FILE = "routes.txt";

'''
* REQUIRED. Trips for each route. A trip is a sequence of two or more stops that occurs at specific time.
'''
TRIPS_FILE = "trips.txt";

'''
* REQUIRED. Times that a vehicle arrives at and departs from individual stops for each trip.
'''
STOP_TIMES_FILE = "stop_times.txt";

'''
* OPTIONAL. Rules for making connections at transfer points between routes.
'''
TRANSFERS_FILE = "transfers.txt";

