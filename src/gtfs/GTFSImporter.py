#!/usr/bin/env python
'''
Created on Oct 16, 2019

@author: camila
'''
from database.PostGISConnection import PostGISConnection
import sys
import csv
import psycopg2
from psycopg2.extensions import AsIs
from network.MultimodalNetwork import MultimodalNetwork

class GTFSImporter:
    '''
    * REQUIRED. One or more transit agencies that provide the data in this feed.
    '''
    AGENCY_FILE = "agency.txt"
    
    '''
    * REQUIRED. Dates for service IDs using a weekly schedule. Specify when service starts and ends, as well as days of the week where service is available.
    '''
    CALENDAR_FILE = "calendar.txt";
    
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
    * OPTIONAL. Exceptions for the service IDs defined in the calendar.txt file. If calendar_dates.txt includes ALL dates of service, this file may be specified instead of calendar.txt.
    '''
    CALENDAR_DATES_FILE = "calendar_dates.txt"; 
    
    '''
    * OPTIONAL. Fare information for a transit organization's routes.
    '''
    FARE_ATTRIBUTES_FILE = "fare_attributes.txt";
    
    '''
    * OPTIONAL. Rules for applying fare information for a transit organization's routes.
    '''
    FARE_RULES_FILE = "fare_rules.txt";
    
    '''
    * OPTIONAL. Rules for drawing lines on a map to represent a transit organization's routes.
    '''
    SHAPES_FILE = "shapes.txt";
    
    '''
    * OPTIONAL. Headway (time between trips) for routes with variable frequency of service.
    '''
    FREQUENCIES_FILE = "frequencies.txt";
    
    '''
    * OPTIONAL. Rules for making connections at transfer points between routes.
    '''
    TRANSFERS_FILE = "transfers.txt";
    
    '''
    * OPTIONAL. Additional information about the feed itself, including publisher, version, and expiration information.    
    '''
    FEED_INFO_FILE = "feed_info.txt";
    
    def __init__(self, gtfs_dir, region):
        self.conn = PostGISConnection()
        self.gtfs_dir = gtfs_dir
        self.region = region
        
    def run(self):
        self.conn.connect()
        self.createTables()
        self.populateTables()
        self.conn.close()    
    
    def createTables(self):
        self.createTableAgency()
        self.createTableCalendar()
        self.createTableStops()
        self.createTableRoutes()
        self.createTableTrips()
        self.createTableStopTimes()
        self.createTableLinks()
        self.createTableTransfers()
        self.createTableRoutesGeometry()
        
    def populateTables(self):
        #self.populateTableAgency()
        #self.populateTableCalendar()
        #self.populateTableStops()
        #self.populateTableRoutes()
        #self.populateTableTrips()
        #self.populateTableStopTimes()
        #self.populateTableLinks()
        #self.populateTableTransfers()
        self.populateTableRoutesGeometry()
    
    
    def createTableAgency(self):
        sql = """
        CREATE TABLE IF NOT EXISTS agency_{} (
            agency_id integer NOT NULL,
            agency_name character varying NOT NULL,
            agency_url character varying NOT NULL,
            agency_timezone character varying NOT NULL,    
            CONSTRAINT agency_pkey PRIMARY KEY (agency_id)
        );
        """
        sql = sql.format(self.region)
        self.conn.executeCommand(sql)
        
    def populateTableAgency(self):
        fName = self.gtfs_dir + self.AGENCY_FILE
        sql = """INSERT INTO agency_{}(agency_id, agency_name, agency_url, agency_timezone) 
                 VALUES ({}, {}, {}, {});
            """
        commands = ""
        f = None
        try:
            with open(fName, 'r') as f:
                line = f.readline()
                line = f.readline()
                while line:
                    (agency_id, agency_name, agency_url, agency_timezone) = line.split(",")[0:4]
                    print(agency_id, agency_name, agency_url, agency_timezone)
                    commands += sql.format(self.region, agency_id, agency_name.replace('"', '\''), agency_url.replace('"', '\''), agency_timezone.replace('"', '\''))
                    print(agency_id)
                    line = f.readline()
            self.conn.executeCommand(commands)
   
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if f is not None:
                f.close()
   
        
    def createTableCalendar(self):
        sql = """
        CREATE TABLE IF NOT EXISTS calendar_{} (
            service_id integer NOT NULL,
            monday boolean NOT NULL,
            tuesday boolean NOT NULL,
            wednesday boolean NOT NULL,
            thursday boolean NOT NULL,
            friday boolean NOT NULL,
            saturday boolean NOT NULL,
            sunday boolean NOT NULL,
            start_date date NOT NULL,
            end_date date NOT NULL,    
            CONSTRAINT calendar_pkey PRIMARY KEY (service_id)
        );
        """
        sql = sql.format(self.region)
        self.conn.executeCommand(sql)
    
    def populateTableCalendar(self):
        fName = self.gtfs_dir + self.CALENDAR_FILE
        sql = """INSERT INTO calendar_{}(service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date) 
                 VALUES ({}, {}, {}, {}, {}, {}, {}, {}, to_date('{}','YYYYMMDD'), to_date('{}','YYYYMMDD'));
            """
        commands = ""
        f = None
        try:
            with open(fName, 'r') as f:
                line = f.readline()
                line = f.readline()
                while line:
                    print(line)
                    (service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date) = line.split(",")
                    print(service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date)
                    commands += sql.format(self.region, service_id, bool(monday), bool(tuesday), bool(wednesday), 
                                bool(thursday), bool(friday), bool(saturday), bool(sunday), str(start_date), str(end_date))
                    print(service_id)
                    line = f.readline()
            self.conn.executeCommand(commands)
   
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if f is not None:
                f.close()    
        
    def createTableStops(self):
        sql = """
        CREATE TABLE IF NOT EXISTS stops_{} (
            stop_id bigint NOT NULL,
            stop_name character varying NOT NULL,
            stop_lat double precision NOT NULL,
            stop_lon double precision NOT NULL,
            stop_location geometry NOT NULL,
            stop_type integer NULL,
            stop_parent bigint NULL,
            CONSTRAINT stops_pkey PRIMARY KEY (stop_id)
        );
        """
        sql = sql.format(self.region)
        self.conn.executeCommand(sql)
        
        
    def populateTableStops(self):
        fName = self.gtfs_dir + self.STOPS_FILE
        sql = """INSERT INTO stops_{0}(stop_id, stop_name, stop_lat, stop_lon, stop_location, stop_type, stop_parent) 
                 VALUES ({1}, '{2}', {3}, {4}, ST_SetSRID(ST_MakePoint({4},{3}),4326), {5}, {6});
            """
        commands = ""
        f = None
        try:
            with open(fName, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    parent = 'NULL'
                    if row['parent_station']:
                        parent = row['parent_station']
                    sql_final = sql.format(self.region, row['stop_id'], row['stop_name'], row['stop_lat'], row['stop_lon'], row['location_type'], parent)
                    #print(sql_final)
                    commands += sql_final
            self.conn.executeCommand(commands)
   
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if f is not None:
                f.close()    
        
    def createTableRoutes(self):
        sql = """
        CREATE TABLE IF NOT EXISTS routes_{0} (
            route_id varchar NOT NULL,
            agency_id integer NOT NULL,
            route_short_name varchar NOT NULL,
            route_long_name varchar NULL,
            route_type integer NOT NULL,    
            CONSTRAINT routes_pkey PRIMARY KEY (route_id),
            CONSTRAINT routes_agency_id_gtfsinfo_id_fkey FOREIGN KEY (agency_id)
            REFERENCES agency_{0} (agency_id) MATCH SIMPLE
            ON UPDATE NO ACTION ON DELETE NO ACTION
        );
        """
        sql = sql.format(self.region)
        self.conn.executeCommand(sql)
    
    '''
    def createTableRoutesGeometry(self):
        sql = """
        CREATE TABLE IF NOT EXISTS routes_geometry_{0} (
            route_id varchar NOT NULL,
            route_short_name varchar NOT NULL,
            stops bigint[] NOT NULL,
            route_geom geometry NOT NULL,
            CONSTRAINT routes_geometry_{0}_pkey PRIMARY KEY (route_id)
        );
        """
        sql = sql.format(self.region)
        self.conn.executeCommand(sql)
    '''  
        
    def createTableRoutesGeometry(self):
        sql = """
        CREATE TABLE IF NOT EXISTS routes_geometry_{0} (
            route_id varchar NOT NULL,
            trip_id bigint NOT NULL,
            route_short_name varchar NOT NULL,
            stops bigint[] NOT NULL,
            stop_names varchar[] NOT NULL,
            route_geom geometry NOT NULL,
            CONSTRAINT routes_geometry_{0}_pkey PRIMARY KEY (route_id, trip_id)
        );
        """
        sql = sql.format(self.region)
        self.conn.executeCommand(sql)
         
    def populateTableRoutes(self):
        fName = self.gtfs_dir + self.ROUTES_FILE
        sql = """INSERT INTO routes_{0}(route_id, route_short_name, route_long_name, route_type, agency_id) 
                 VALUES ('{1}', '{2}', {3}, {4}, {5});
            """
        commands = ""
        f = None
        try:
            with open(fName, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    longName = 'NULL'
                    if row['route_long_name']:
                        longName = "'"+ row['route_long_name']+"'"
                    print(self.region, row['route_id'], row['route_short_name'], row['route_long_name'], row['route_type'], row['agency_id'])
                    commands += sql.format(self.region, row['route_id'], row['route_short_name'], longName, row['route_type'], row['agency_id'])
            self.conn.executeCommand(commands)
   
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if f is not None:
                f.close()   
    
    def populateTableRoutesGeometry(self):
        sql_insert = """INSERT INTO routes_geometry_{0}(route_id, trip_id, route_short_name, stops, stop_names, route_geom) 
                 VALUES ('{1}', {2}, '{3}', '{4}', '{5}', {6});
            """
        try:
            sql_routes = """
                    SELECT route_id, route_short_name FROM routes_{0};
            """
            
            sql_geometry = """
                    SELECT ST_Intersects({1}, 
                    (SELECT polygon from neighborhoods_{0} where level = (SELECT min(level) FROM neighborhoods_{0})));
            """
            sql_routes = sql_routes.format(self.region)
            cursor = self.conn.getCursor()
            cursor.execute(sql_routes)
            routes = {}
            
            row = cursor.fetchone()
            
            while row is not None:
                (route_id, route_short_name) = row
                routes[route_id] = route_short_name
                row = cursor.fetchone()
            
            sql_stops = """select st.stop_id, trip_id, s.stop_lat, s.stop_lon, s.stop_parent, s.stop_name 
                        from stop_times_{0} st, stops_{0} s
                        where trip_id in
                        (select trip_id from trips_berlin where route_id = '{1}')
                        and st.stop_id = s.stop_id
                        ORDER BY trip_id, stop_sequence;
                        """
            for route in routes:
                print(route)
                sql_stops_route = sql_stops.format(self.region, route)
                cursor.execute(sql_stops_route)
                row = cursor.fetchone()
                trips_set = []
                trips_id = []
                route_stops = []
                stop_names = []
                
                trip_stops = {}
                trip_names = {}
                trip_geometry = {}
                
                previous_trip = -1
                
                geometry = "ST_GeomFromText('LINESTRING("
                while row is not None:
                    (stop_id, trip_id, lat, lon, parent, name) = row
                    if not parent:
                        print("Parent does not exist:" + str(stop_id))
                        parent = stop_id
                    if trip_id != previous_trip:
                        #print("different trip!")
                        print(trip_id)
                        if previous_trip != -1:
                            route_set = set(route_stops)
                            res = self.checkNewTrip(trips_set, route_set)
                            if res >= -1:
                                trips_set.append(set(route_set))
                                trips_id.append(previous_trip)
                                trip_stops[previous_trip] = route_stops
                                trip_names[previous_trip] = stop_names
                            if res >= 0:
                                print("removing:" + str(res))
                                del trips_set[res]
                                del trips_id[res]
                            
                            geometry = geometry[:-1]
                            geometry += ")', 4326)"
                            trip_geometry[previous_trip] = geometry
                            
                        geometry = "ST_GeomFromText('LINESTRING("
                        geometry += str(lon) + " " + str(lat) + ","
                        route_stops = [parent]
                        stop_names = ['"' + str(name) + '"']
                    else:
                        route_stops.append(parent)
                        stop_names.append('"' + str(name) + '"')
                        geometry += str(lon) + " " + str(lat) + ","
                    previous_trip = trip_id
                    row = cursor.fetchone()
                    
                route_set = set(route_stops)
                res = self.checkNewTrip(trips_set, route_set)
                if res >= -1:
                    trips_set.append(set(route_set))
                    trips_id.append(previous_trip)
                    trip_stops[previous_trip] = route_stops
                    trip_names[previous_trip] = stop_names
                if res >= 0:
                    print("removing:" + str(res))
                    del trips_set[res]
                    del trips_id[res]
                
                geometry = geometry[:-1]
                geometry += ")', 4326)"
                trip_geometry[previous_trip] = geometry
                
                for trip_id in trips_id:
                    print(trip_id)
                    geometry_trip = trip_geometry[trip_id]
                    stops = trip_stops[trip_id]
                    if(len(stops) <=1):
                        print("trip is too short! " + str(stops))
                    sql_geometry_id = sql_geometry.format(self.region, geometry_trip)
                    cursor.execute(sql_geometry_id)
                    (intersects, ) = cursor.fetchone()
                    if intersects:
                        str_stops = str(stops)
                        str_stops = str_stops[1:-1]
                        str_stops = "{" + str_stops + "}"
                        
                        seperator = ', '
                        str_names = trip_names[trip_id]
                        #str_names = str_names[1:-1]
                        str_names = seperator.join(str_names)
                        str_names = "{" + str_names + "}"
                        #print(str_names)
                        
                        sql_insert_route = sql_insert.format(self.region, route, trip_id, routes[route], str_stops, str_names, geometry_trip)
                        self.conn.executeCommand(sql_insert_route)
                        print(str(trip_id) + " INSERTED!")
                    else:
                        print(str(trip_id) + " does not intersect the region")
   
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0]) 
    
    def checkNewTrip(self, trips_set, new_trip):
        if not trips_set:
            print("adding new trip")
            return -1
        else:
            for i in range(len(trips_set)):
                trip = trips_set[i]
                if new_trip == trip:
                    print("trip already exists!")
                    return -2
                elif new_trip.issubset(trip):
                    print("trip is subset of existing trip!")
                    return -2 
                elif trip.issubset(new_trip): 
                    print("adding new trip")
                    print("existing trip should be removed!")
                    return i 
            print("adding new trip")
            return -1   
                    
        
    '''
    def populateTableRoutesGeometry(self):
        sql_insert = """INSERT INTO routes_geometry_{0}(route_id, route_short_name, stops, route_geom) 
                 VALUES ('{1}', '{2}', '{3}', {4});
            """
        try:
            sql_routes = """
                    SELECT route_id, route_short_name FROM routes_{0};
            """
            
            sql_geometry = """
                    SELECT ST_Intersects({1}, 
                    (SELECT polygon from neighborhoods_{0} where level = (SELECT min(level) FROM neighborhoods_{0})));
            """
            sql_routes = sql_routes.format(self.region)
            cursor = self.conn.getCursor()
            cursor.execute(sql_routes)
            routes = {}
            
            row = cursor.fetchone()
            
            while row is not None:
                (route_id, route_short_name) = row
                routes[route_id] = route_short_name
                row = cursor.fetchone()
            
                        
            sql_stops = """select st.stop_id, s.stop_lat, s.stop_lon from stop_times_{0} st, stops_{0} s
                            where trip_id = (select trip_id
                            from stop_times_{0} where trip_id in 
                            (select trip_id from trips_{0} where route_id = '{1}')
                            group by trip_id
                            order by count(*) DESC LIMIT 1) and
                            st.stop_id = s.stop_id
                            ORDER BY stop_sequence;
                        """
            for route in routes:
                print(route)
                stops = []
                sql_stops_route = sql_stops.format(self.region, route)
                cursor.execute(sql_stops_route)
                row = cursor.fetchone()
                
                geometry = "ST_GeomFromText('LINESTRING("
                while row is not None:
                    (stop_id, lat, lon) = row
                    stops.append(stop_id)
                    geometry += str(lon) + " " + str(lat) + ","
                    row = cursor.fetchone()
                geometry = geometry[:-1]
                geometry += ")', 4326)"
                
                sql_geometry_id = sql_geometry.format(self.region, geometry)
                cursor.execute(sql_geometry_id)
                (intersects, ) = cursor.fetchone()
                if intersects:
                    str_stops = str(stops)
                    str_stops = str_stops[1:-1]
                    str_stops = "{" + str_stops + "}"
                    
                    sql_insert_route = sql_insert.format(self.region, route, routes[route], str_stops, geometry)
                    self.conn.executeCommand(sql_insert_route)
                    print("INSERTED!")
                else:
                    print(str(route) + "does not intersect the region")
                
   
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
    '''
        
    def createTableTrips(self):
        sql = """
        CREATE TABLE IF NOT EXISTS trips_{0} (
            route_id varchar NOT NULL,
            trip_id bigint NOT NULL,
            service_id integer NOT NULL,    
            CONSTRAINT trips_pkey PRIMARY KEY (trip_id),
            CONSTRAINT trips_service_id_fkey FOREIGN KEY (service_id)
                REFERENCES calendar_{0} (service_id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION,
            CONSTRAINT trips_route_id_fkey FOREIGN KEY (route_id)
                REFERENCES routes_{0} (route_id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION
            );
        """
        sql = sql.format(self.region)
        self.conn.executeCommand(sql)
        
    
    def populateTableTrips(self):
        fName = self.gtfs_dir + self.TRIPS_FILE
        sql = """INSERT INTO trips_{0}(route_id, trip_id, service_id) 
                 VALUES ('{1}', {2}, {3});
            """
        commands = ""
        f = None
        try:
            with open(fName, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    #print(self.region, row['route_id'], row['trip_id'], row['service_id'])
                    commands += sql.format(self.region, row['route_id'], row['trip_id'], row['service_id'])
            self.conn.executeCommand(commands)
   
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if f is not None:
                f.close()   
        
    def createTableStopTimes(self):
        sql = """
        CREATE TABLE IF NOT EXISTS stop_times_{0} (
            trip_id bigint NOT NULL,
            arrival_time time NOT NULL,
            departure_time time NOT NULL,
            stop_id bigint NOT NULL,
            stop_sequence integer NOT NULL,    
            CONSTRAINT stop_times_stop_id_fkey FOREIGN KEY (stop_id)
                REFERENCES stops_{0} (stop_id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION,
            CONSTRAINT stop_times_trip_id_fkey FOREIGN KEY (trip_id)
                REFERENCES trips_{0} (trip_id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION
        );
        """
        sql = sql.format(self.region)
        self.conn.executeCommand(sql)
        
    def populateTableStopTimes(self):
        fName = self.gtfs_dir + self.STOP_TIMES_FILE
        sql = """INSERT INTO stop_times_%s(trip_id, arrival_time, departure_time, stop_id, stop_sequence) 
                 VALUES (%s, %s, %s, %s, %s);
            """
        records = []
        f = None
        try:
            with open(fName, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    #print(row['trip_id'])
                    #print(self.region, row['route_id'], row['trip_id'], row['service_id'])
                    hour_arrival, min_arrival, sec_arrival = row['arrival_time'].split(":")
                    if int(hour_arrival) > 23:
                        hour_arrival = int(hour_arrival)%24
                        row['arrival_time'] = str(hour_arrival) + ":" + min_arrival + ":" + sec_arrival 
                    hour_departure, min_departure, sec_departure = row['departure_time'].split(":")
                    if int(hour_departure) > 23:
                        hour_departure = int(hour_departure)%24
                        row['departure_time'] = str(hour_departure) + ":" + min_departure + ":" + sec_departure
                    
                    records.append((AsIs(self.region), row['trip_id'], row['arrival_time'], row['departure_time'],
                                row['stop_id'], row['stop_sequence']))
                    #commands += sql.format(self.region, row['trip_id'], row['arrival_time'], row['departure_time'],
                    #            row['stop_id'], row['stop_sequence'])
            #self.conn.executeCommand(commands)
            cursor = self.conn.conn.cursor()
            #cursor.execute(sql, ("berlin", '105286112', '6:18:00', '6:18:00', '790002155692', '0'))
            cursor.executemany(sql, records)
            self.conn.conn.commit()
   
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if f is not None:
                f.close()  
    
    def createTableTransfers(self):
        
        sql = """
        CREATE TABLE IF NOT EXISTS transfers_{0} (
            from_stop_id bigint NOT NULL,
            to_stop_id bigint NOT NULL,
            transfer_type integer NOT NULL,
            min_transfer_time integer NULL,
            from_route_id varchar NULL,
            to_route_id varchar NULL,
            from_trip_id bigint NULL,
            to_trip_id bigint NULL,
            CONSTRAINT trips_from_stop_fkey FOREIGN KEY (from_stop_id)
                REFERENCES stops_{0} (stop_id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION,
            CONSTRAINT trips_to_stop_fkey FOREIGN KEY (to_stop_id)
                REFERENCES stops_{0} (stop_id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION
        );
        """
        sql = sql.format(self.region)
        self.conn.executeCommand(sql)
        
    def populateTableTransfers(self):
        fName = self.gtfs_dir + self.TRANSFERS_FILE
        sql = """INSERT INTO transfers_%s(from_stop_id, to_stop_id, transfer_type, min_transfer_time, from_route_id, to_route_id, from_trip_id, to_trip_id) 
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
        records = []
        f = None
        try:
            with open(fName, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    min_transfer_time = AsIs('NULL')
                    if row['min_transfer_time']:
                        min_transfer_time = row['min_transfer_time']
                    from_route_id = AsIs('NULL')
                    if 'from_route_id' in row and row['from_route_id']:
                        from_route_id = row['from_route_id']
                    to_route_id = AsIs('NULL')
                    if 'to_route_id' in row and row['to_route_id']:
                        to_route_id = row['to_route_id']
                    from_trip_id = AsIs('NULL')
                    if 'from_trip_id' in row and row['from_trip_id']:
                        from_trip_id = row['from_trip_id']
                    to_trip_id = AsIs('NULL')
                    if 'to_trip_id' in row and row['to_trip_id']:
                        to_trip_id = row['to_trip_id']
                    
                    records.append((AsIs(self.region), row['from_stop_id'], row['to_stop_id'], row['transfer_type'],
                                min_transfer_time, from_route_id, to_route_id, from_trip_id, to_trip_id))
            cursor = self.conn.conn.cursor()
            cursor.executemany(sql, records)
            self.conn.conn.commit()
   
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if f is not None:
                f.close()  
        
    def createTableLinks(self):
        sql = """
        CREATE TABLE IF NOT EXISTS links_{0} (
            link_id bigint NOT NULL,
            stop_id bigint NOT NULL,
            edge_id bigint NOT NULL,
            osm_source bigint NOT NULL,
            osm_target bigint NOT NULL,
            edge_dist double precision NOT NULL,
            source_ratio double precision NOT NULL,
            edge_length double precision NOT NULL,
            point_location geometry NOT NULL,
            source_point_geom geometry NULL,   
            point_target_geom geometry NULL,   
            CONSTRAINT links_{0}_pkey PRIMARY KEY (link_id), 
            CONSTRAINT links_{0}_stop_id_fkey FOREIGN KEY (stop_id)
                REFERENCES stops_{0} (stop_id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION,
            CONSTRAINT link_{0}_edge_id_fkey FOREIGN KEY (edge_id)
                REFERENCES roadnet_{0} (id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION
        );
        """
        
        sql = sql.format(self.region)
        self.conn.executeCommand(sql)
        
    
    def getParentsIds(self):
        ids = []
        cur = self.conn.conn.cursor()
        
        sql = """SELECT stop_id from stops_berlin
                WHERE stop_type = 1
                ORDER BY stop_id;"""
        sql = sql.format(self.region)
        
        # execute a statement
        cur.execute(sql)
 
        # display the result
        row = cur.fetchone()
        
        while row is not None:
            (id,) = row
            ids.append(id)
            row = cur.fetchone()
        return ids
    
    def populateTableLinks(self):
        stop_ids = self.getParentsIds()
        sql = """INSERT INTO links_%s(link_id, stop_id, edge_id, osm_source, osm_target, edge_dist, source_ratio, edge_length, point_location, 
                source_point_geom, point_target_geom) 
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
        records = []
        link_id = 0
        try:
            cursor = self.conn.conn.cursor()
            sql_select = """SELECT stop_id, edge_id, osm_source, osm_target,
                    ST_Distance(stop_location::geography, geom_way::geography)/1000
                    AS edge_dist, 
                    source_ratio,
                    km as edge_length,  
                    ST_LineInterpolatePoint(geom_way, source_ratio) as point_location, 
                    ST_GeometryN(ST_Split(ST_Snap(geom_way, ST_ClosestPoint(geom_way, stop_location), 0.00001), ST_ClosestPoint(geom_way, stop_location)), 1) as source_point_geom,
                    ST_GeometryN(ST_Split(ST_Snap(geom_way, ST_ClosestPoint(geom_way, stop_location), 0.00001), ST_ClosestPoint(geom_way, stop_location)), 2) as point_target_geom
                    FROM 
                    (SELECT stop_id, roadnet_{0}.id AS edge_id, roadnet_{0}.osm_source_id AS osm_source, roadnet_{0}.osm_target_id AS osm_target,
                    stops_{0}.stop_location as stop_location, roadnet_{0}.geom_way as geom_way, roadnet_{0}.km as km,
                    ST_LineLocatePoint(geom_way, stop_location) as source_ratio
                    FROM roadnet_{0}, stops_{0} 
                    WHERE stops_{0}.stop_id = {1} and roadnet_{0}.clazz= ANY('{2}'::int[])
                    ORDER BY ST_Distance(stops_{0}.stop_location::geography, roadnet_{0}.geom_way::geography) LIMIT 1) as r;
                    """
            for stop in stop_ids:
                sql_select_stop = sql_select.format(self.region, stop, MultimodalNetwork.PEDESTRIAN_WAYS)
                cursor.execute(sql_select_stop)
                (stop_id, edge_id, osm_source, osm_target, edge_dist, source_ratio, edge_length, point_location, 
                 source_point_geom, point_target_geom) = cursor.fetchone()
                print(stop_id, edge_id)
                records.append((AsIs(self.region), link_id, stop_id, edge_id, osm_source, osm_target, edge_dist, source_ratio, edge_length, 
                                         point_location, source_point_geom, point_target_geom))
                link_id += 1
                
                if link_id%100 == 0:
                    print("Committing...")
                    cursor.executemany(sql, records)
                    self.conn.conn.commit()
                    records = []
                    
            cursor.executemany(sql, records)
            self.conn.conn.commit()   
                
   
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except: 
            print("Unexpected error:", sys.exc_info()[0])
        
    
gtfs = GTFSImporter("/home/camila/gtfs-schema-master/GTFS_Berlin/" , "berlin")
gtfs.run()
        