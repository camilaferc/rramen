'''
Created on Oct 16, 2019

@author: camila
'''
from _datetime import datetime
import csv
import os
import sys

import psycopg2
from psycopg2._psycopg import AsIs
from psycopg2.sql import SQL, Identifier

from ..database.PostGISConnection import PostGISConnection
from . import GTFS
from ..network.MultimodalNetwork import MultimodalNetwork


class GTFSImporter:
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
        self.createTableRoutesGeometry()

    def populateTables(self):
        self.populateTableAgency()
        self.populateTableCalendar()
        self.populateTableStops()
        self.populateTableRoutes()
        self.populateTableTrips()
        self.populateTableStopTimes()
        self.populateTableLinks()
        self.populateTableTransfers()
        self.populateTableRoutesGeometry()


    def createTableAgency(self):
        sql = """
        CREATE TABLE IF NOT EXISTS {} (
            agency_id character varying NOT NULL,
            agency_name character varying NOT NULL,
            agency_url character varying NOT NULL,
            agency_timezone character varying NOT NULL,
            CONSTRAINT {} PRIMARY KEY (agency_id)
        );
        """
        sql = SQL(sql).format(Identifier("agency_"+str(self.region)),
                              Identifier("agency_"+str(self.region)+"_pkey"))
        self.conn.executeCommand(sql)

    def populateTableAgency(self):
        print("Creating table agency...")
        fName = self.gtfs_dir + GTFS.AGENCY_FILE
        sql = """INSERT INTO {}(agency_id, agency_name, agency_url, agency_timezone)
                 VALUES (%s, %s, %s, %s);
            """
        sql = SQL(sql).format(Identifier("agency_"+str(self.region)))
        f = None
        try:
            cursor = self.conn.getCursor()
            with open(fName, 'r') as f:
                reader = csv.DictReader(f) # read rows into a dictionary format
                for row in reader: # read a row as {column1: value1, column2: value2,...}
                    cursor.execute(sql, (row.get('agency_id'), row.get('agency_name'), row.get('agency_url'), row.get('agency_timezone')))
                self.conn.commit()
                cursor.close()
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except:
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if f is not None:
                f.close()


    def createTableCalendar(self):
        sql = """
        CREATE TABLE IF NOT EXISTS {} (
            service_id character varying NOT NULL,
            monday boolean NOT NULL,
            tuesday boolean NOT NULL,
            wednesday boolean NOT NULL,
            thursday boolean NOT NULL,
            friday boolean NOT NULL,
            saturday boolean NOT NULL,
            sunday boolean NOT NULL,
            CONSTRAINT {} PRIMARY KEY (service_id)
        );
        """
        sql = SQL(sql).format(Identifier("calendar_"+str(self.region)),
                              Identifier("calendar_"+str(self.region)+"_pkey"))
        self.conn.executeCommand(sql)

    def populateTableCalendar(self):
        print("Creating table calendar...")
        fName = self.gtfs_dir + GTFS.CALENDAR_FILE
        sql = """INSERT INTO {}(service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                 ON CONFLICT DO NOTHING;
            """
        sql = SQL(sql).format(Identifier("calendar_"+str(self.region)))
        f = None
        calendar = {}
        try:
            if os.path.isfile(fName):
                with open(fName, 'r') as f:
                    line = f.readline()
                    line = f.readline()
                    while line:
                        (service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, _ ,_) = line.split(",")
                        calendar_service = {}
                        if monday:
                            calendar_service[0] = True
                        if tuesday:
                            calendar_service[1] = True
                        if wednesday:
                            calendar_service[2] = True
                        if thursday:
                            calendar_service[3] = True
                        if friday:
                            calendar_service[4] = True
                        if saturday:
                            calendar_service[5] = True
                        if sunday:
                            calendar_service[6] = True
                        calendar[service_id] = calendar_service
                        line = f.readline()
            fName = self.gtfs_dir + GTFS.CALENDAR_DATE_FILE
            if os.path.isfile(fName):
                with open(fName, 'r') as f:
                    line = f.readline()
                    line = f.readline()
                    while line:
                        (service_id, date, exception_type) = line.split(",")
                        if int(exception_type) == 1:
                            day = datetime.strptime(date, '%Y%m%d').weekday()
                            if service_id in calendar:
                                calendar[service_id][day] = True
                            else:
                                calendar_service = {}
                                calendar_service[day] = True
                                calendar[service_id] = calendar_service

                        line = f.readline()
            #else:
            #    raise SystemExit("No calendar file provided!")
            cursor = self.conn.getCursor()
            for service_id in calendar:
                service = calendar[service_id]
                cursor.execute(sql, (service_id, service.get(0, False), service.get(1, False), service.get(2, False), service.get(3, False),
                                     service.get(4, False), service.get(5, False), service.get(6, False)))
            self.conn.commit()
            cursor.close()

        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except:
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if f is not None:
                f.close()

    def createTableStops(self):
        sql = """
        CREATE TABLE IF NOT EXISTS {0} (
            stop_id character varying NOT NULL,
            stop_name character varying NOT NULL,
            stop_lat double precision NOT NULL,
            stop_lon double precision NOT NULL,
            stop_location geometry NOT NULL,
            stop_type integer NULL,
            stop_parent character varying NULL,
            CONSTRAINT {1} PRIMARY KEY (stop_id)
        );

        CREATE INDEX IF NOT EXISTS {2}
        ON {0}
        USING gist
        (stop_location);
        """
        sql = SQL(sql).format(Identifier("stops_"+str(self.region)),
                              Identifier("stops_"+str(self.region)+"_pkey"),
                              Identifier("stops_"+str(self.region)+"_geom_idx"))
        self.conn.executeCommand(sql)


    def populateTableStops(self):
        print("Creating table stops...")
        fName = self.gtfs_dir + GTFS.STOPS_FILE
        sql = """INSERT INTO {}(stop_id, stop_name, stop_lat, stop_lon, stop_location, stop_type, stop_parent)
                 VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s),4326), %s, %s);
            """
        sql = SQL(sql).format(Identifier("stops_"+str(self.region)))
        f = None
        try:
            cursor = self.conn.getCursor()
            with open(fName, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    for i in row:
                        if row[i] == '':
                            row[i] = None
                    cursor.execute(sql, (row.get('stop_id'), row.get('stop_name'), row.get('stop_lat'), row.get('stop_lon'),
                                         row.get('stop_lon'), row.get('stop_lat'),
                                         row.get('location_type'), row.get('parent_station')))
            self.conn.commit()
            cursor.close()

        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except:
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if f is not None:
                f.close()

    def createTableRoutes(self):
        sql = """
        CREATE TABLE IF NOT EXISTS {} (
            route_id varchar NOT NULL,
            agency_id varchar NOT NULL,
            route_short_name varchar NOT NULL,
            route_long_name varchar NULL,
            route_type integer NOT NULL,
            CONSTRAINT {} PRIMARY KEY (route_id),
            CONSTRAINT {} FOREIGN KEY (agency_id)
            REFERENCES {} (agency_id) MATCH SIMPLE
            ON UPDATE NO ACTION ON DELETE NO ACTION
        );
        """
        sql = SQL(sql).format(Identifier("routes_"+str(self.region)),
                              Identifier("routes_"+str(self.region)+"_pkey"),
                              Identifier("routes_"+str(self.region)+"_agency_id_gtfsinfo_id_fkey"),
                              Identifier("agency_"+str(self.region)))
        self.conn.executeCommand(sql)

    def createTableRoutesGeometry(self):
        sql = """
        CREATE TABLE IF NOT EXISTS {} (
            route_id varchar NOT NULL,
            trip_id varchar NOT NULL,
            route_short_name varchar NOT NULL,
            stops varchar[] NOT NULL,
            stop_names varchar[] NOT NULL,
            route_geom geometry NOT NULL,
            CONSTRAINT {} PRIMARY KEY (route_id, trip_id)
        );
        """
        sql = SQL(sql).format(Identifier("routes_geometry_"+str(self.region)),
                              Identifier("routes_geometry_"+str(self.region)+"_pkey"))
        self.conn.executeCommand(sql)

    def populateTableRoutes(self):
        print("Creating table routes...")
        fName = self.gtfs_dir + GTFS.ROUTES_FILE
        sql = """INSERT INTO {}(route_id, route_short_name, route_long_name, route_type, agency_id)
                 VALUES (%s, %s, %s, %s, %s);
            """
        sql = SQL(sql).format(Identifier("routes_"+str(self.region)))
        f = None
        try:
            cursor = self.conn.getCursor()
            with open(fName, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    for i in row:
                        if row[i] == '':
                            row[i] = None
                    cursor.execute(sql, (row.get('route_id'), row.get('route_short_name'), row.get('route_long_name'),
                                         row.get('route_type'), row.get('agency_id')))
            self.conn.commit()

        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except:
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if f is not None:
                f.close()

    def populateTableRoutesGeometry(self):
        print("Creating table routes geometry...")
        sql_insert = """INSERT INTO {}(route_id, trip_id, route_short_name, stops, stop_names, route_geom)
                 VALUES (%s, %s, %s, %s, %s, ST_GeomFromText('LINESTRING(%s)', 4326));
            """
        sql_insert = SQL(sql_insert).format(Identifier("routes_geometry_"+str(self.region)))

        sql_routes = """
                    SELECT route_id, route_short_name FROM {};
            """
        sql_routes = SQL(sql_routes).format(Identifier("routes_"+str(self.region)))

        sql_geometry = """
                SELECT ST_Intersects(ST_GeomFromText('LINESTRING(%s)', 4326),
                (SELECT polygon from {0} where level = (SELECT min(level) FROM {0})));
        """
        sql_geometry = SQL(sql_geometry).format(Identifier("neighborhoods_"+str(self.region)))

        try:

            cursor = self.conn.getCursor()
            cursor.execute(sql_routes)
            routes = {}

            row = cursor.fetchone()

            while row is not None:
                (route_id, route_short_name) = row
                routes[route_id] = route_short_name
                row = cursor.fetchone()

            sql_stops = """select st.stop_id, trip_id, s.stop_lat, s.stop_lon, s.stop_parent, s.stop_name
                        from {} st, {} s
                        where trip_id in
                        (select trip_id from {} where route_id = %s)
                        and st.stop_id = s.stop_id
                        ORDER BY trip_id, stop_sequence;
                        """
            sql_stops = SQL(sql_stops).format(Identifier("stop_times_"+str(self.region)),
                                              Identifier("stops_"+str(self.region)),
                                              Identifier("trips_"+str(self.region)))
            for route in routes:
                cursor.execute(sql_stops, (route, ))

                row = cursor.fetchone()
                trips_set = []
                trips_id = []
                route_stops = []
                stop_names = []

                trip_stops = {}
                trip_names = {}
                trip_geometry = {}

                previous_trip = -1

                geometry = ""
                while row is not None:
                    (stop_id, trip_id, lat, lon, parent, name) = row
                    if not parent:
                        #stop does not have a parent
                        parent = stop_id
                    if trip_id != previous_trip:
                        if previous_trip != -1:
                            route_set = set(route_stops)
                            res = self.checkNewTrip(trips_set, route_set)
                            if res >= -1:
                                trips_set.append(set(route_set))
                                trips_id.append(previous_trip)
                                trip_stops[previous_trip] = route_stops
                                trip_names[previous_trip] = stop_names
                            if res >= 0:
                                del trips_set[res]
                                del trips_id[res]

                            geometry = geometry[:-1]
                            trip_geometry[previous_trip] = geometry

                        geometry = ""
                        geometry += str(lon) + " " + str(lat) + ","
                        route_stops = [parent]
                        stop_names = [name]
                    else:
                        route_stops.append(parent)
                        stop_names.append(name)
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
                    del trips_set[res]
                    del trips_id[res]

                geometry = geometry[:-1]
                trip_geometry[previous_trip] = geometry

                for trip_id in trips_id:
                    geometry_trip = AsIs(trip_geometry[trip_id])
                    stops = trip_stops[trip_id]

                    cursor.execute(sql_geometry, (geometry_trip, ))
                    (intersects, ) = cursor.fetchone()

                    if intersects:
                        names = trip_names[trip_id]
                        cursor.execute(sql_insert, (route, trip_id, routes[route], stops, names, geometry_trip))
                        self.conn.commit()
            cursor.close()
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except:
            print("Unexpected error:", sys.exc_info()[0])

    def checkNewTrip(self, trips_set, new_trip):
        if not new_trip:
            return -2
        if not trips_set:
            return -1
        else:
            for i in range(len(trips_set)):
                trip = trips_set[i]
                if new_trip == trip:
                    return -2
                elif new_trip.issubset(trip):
                    return -2
                elif trip.issubset(new_trip):
                    return i
            return -1


    def createTableTrips(self):
        sql = """
        CREATE TABLE IF NOT EXISTS {0} (
            route_id varchar NOT NULL,
            trip_id character varying NOT NULL,
            service_id character varying NOT NULL,
            CONSTRAINT {1} PRIMARY KEY (trip_id),
            CONSTRAINT {2} FOREIGN KEY (service_id)
                REFERENCES {3} (service_id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION,
            CONSTRAINT {4} FOREIGN KEY (route_id)
                REFERENCES {5} (route_id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION
            );

        CREATE INDEX IF NOT EXISTS {6}
        ON {0}
        USING btree (route_id);
        """
        sql = SQL(sql).format(Identifier("trips_"+str(self.region)),
                              Identifier("trips_"+str(self.region)+"_pkey"),
                              Identifier("trips_"+str(self.region)+"_service_id_fkey"),
                              Identifier("calendar_"+str(self.region)),
                              Identifier("trips_"+str(self.region)+"_route_id_fkey"),
                              Identifier("routes_"+str(self.region)),
                              Identifier("route_trip_"+str(self.region)+"_idx"))
        self.conn.executeCommand(sql)


    def populateTableTrips(self):
        print("Creating table trips...")
        fName = self.gtfs_dir + GTFS.TRIPS_FILE
        sql = """INSERT INTO {}(route_id, trip_id, service_id)
                 VALUES (%s, %s, %s);
            """
        sql = SQL(sql).format(Identifier("trips_"+str(self.region)))
        f = None
        try:
            cursor = self.conn.getCursor()
            with open(fName, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cursor.execute(sql, (row['route_id'], row['trip_id'], row['service_id']))
            self.conn.commit()
            cursor.close()

        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except:
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            if f is not None:
                f.close()

    def createTableStopTimes(self):
        sql = """
        CREATE TABLE IF NOT EXISTS {0} (
            trip_id character varying  NOT NULL,
            arrival_time time NOT NULL,
            departure_time time NOT NULL,
            stop_id character varying NOT NULL,
            stop_sequence integer NOT NULL,
            CONSTRAINT {1} FOREIGN KEY (stop_id)
                REFERENCES {2} (stop_id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION,
            CONSTRAINT {3} FOREIGN KEY (trip_id)
                REFERENCES {4} (trip_id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION
        );

        """
        sql = SQL(sql).format(Identifier("stop_times_"+str(self.region)),
                              Identifier("stop_times_"+str(self.region)+"_stop_id_fkey"),
                              Identifier("stops_"+str(self.region)),
                              Identifier("stop_times_"+str(self.region)+"_trip_id_fkey"),
                              Identifier("trips_"+str(self.region)))
        self.conn.executeCommand(sql)

    def populateTableStopTimes(self):
        print("Creating table stop times...")
        fName = self.gtfs_dir + GTFS.STOP_TIMES_FILE
        sql = """INSERT INTO {}(trip_id, arrival_time, departure_time, stop_id, stop_sequence)
                 VALUES (%s, %s, %s, %s, %s);
            """
        sql = SQL(sql).format(Identifier("stop_times_"+str(self.region)))
        records = []
        f = None
        try:
            cursor = self.conn.conn.cursor()
            count = 0
            with open(fName, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    hour_arrival, min_arrival, sec_arrival = row['arrival_time'].split(":")
                    if int(hour_arrival) > 23:
                        hour_arrival = int(hour_arrival)%24
                        row['arrival_time'] = str(hour_arrival) + ":" + min_arrival + ":" + sec_arrival
                    hour_departure, min_departure, sec_departure = row['departure_time'].split(":")
                    if int(hour_departure) > 23:
                        hour_departure = int(hour_departure)%24
                        row['departure_time'] = str(hour_departure) + ":" + min_departure + ":" + sec_departure

                    records.append((row['trip_id'], row['arrival_time'], row['departure_time'],
                                row['stop_id'], row['stop_sequence']))
                    count += 1
                    if count%10000 == 0:
                        cursor.executemany(sql, records)
                        self.conn.commit()
                        records = []

            cursor.executemany(sql, records)

            sql_index = """CREATE INDEX IF NOT EXISTS {0}
                        ON {1}
                        USING btree
                        (trip_id);"""
            sql_index = SQL(sql_index).format(Identifier("stop_trip_"+str(self.region)+"_idx"),
                              Identifier("stop_times_"+str(self.region)))

            cursor.execute(sql_index)
            self.conn.commit()
            cursor.close()
            records = None
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
        CREATE TABLE IF NOT EXISTS {0} (
            from_stop_id character varying NOT NULL,
            to_stop_id character varying NOT NULL,
            transfer_type integer NOT NULL,
            min_transfer_time integer NULL,
            from_route_id varchar NULL,
            to_route_id varchar NULL,
            from_trip_id varchar NULL,
            to_trip_id varchar NULL,
            CONSTRAINT {1} FOREIGN KEY (from_stop_id)
                REFERENCES {2} (stop_id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION,
            CONSTRAINT {3} FOREIGN KEY (to_stop_id)
                REFERENCES {2} (stop_id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION
        );
        """
        sql = SQL(sql).format(Identifier("transfers_"+str(self.region)),
                              Identifier("trips_from_stop_"+str(self.region)+"_fkey"),
                              Identifier("stops_"+str(self.region)),
                              Identifier("trips_to_stop_"+str(self.region)+"_fkey"))
        self.conn.executeCommand(sql)

    def populateTableTransfers(self):
        fName = self.gtfs_dir + GTFS.TRANSFERS_FILE

        if not os.path.isfile(fName):
            print("Transfer file not provided. OK")
            return

        print("Creating table transfers...")

        self.createTableTransfers()

        sql = """INSERT INTO {0}(from_stop_id, to_stop_id, transfer_type, min_transfer_time, from_route_id, to_route_id, from_trip_id, to_trip_id)
                 SELECT %s, %s, %s, %s, %s, %s, %s, %s
                 WHERE EXISTS (SELECT * FROM {1} WHERE stop_id = %s) and EXISTS (SELECT * FROM {1} WHERE stop_id = %s);
            """
        sql = SQL(sql).format(Identifier("transfers_"+str(self.region)),
                              Identifier("stops_"+str(self.region)))
        records = []
        f = None
        try:
            with open(fName, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    for i in row:
                        if row[i] == '':
                            row[i] = None
                    records.append((row.get('from_stop_id'), row.get('to_stop_id'), row.get('transfer_type'),
                                row.get('min_transfer_time'), row.get('from_route_id'), row.get('to_route_id'),
                                row.get('from_trip_id'), row.get('to_trip_id'),
                                row.get('from_stop_id'), row.get('to_stop_id')))
            cursor = self.conn.getCursor()
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
        CREATE TABLE IF NOT EXISTS {0} (
            link_id bigint NOT NULL,
            stop_id character varying NOT NULL,
            edge_id bigint NOT NULL,
            source bigint NOT NULL,
            target bigint NOT NULL,
            edge_dist double precision NOT NULL,
            source_ratio double precision NOT NULL,
            edge_length double precision NOT NULL,
            point_location geometry NOT NULL,
            source_point_geom geometry NULL,
            point_target_geom geometry NULL,
            CONSTRAINT {1} PRIMARY KEY (link_id),
            CONSTRAINT {2} FOREIGN KEY (stop_id)
                REFERENCES {3} (stop_id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION,
            CONSTRAINT {4} FOREIGN KEY (edge_id)
                REFERENCES {5} (id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION
        );
        """

        sql = SQL(sql).format(Identifier("links_"+str(self.region)),
                              Identifier("links_"+str(self.region)+"_pkey"),
                              Identifier("links_"+str(self.region)+"_stop_id_fkey"),
                              Identifier("stops_"+str(self.region)),
                              Identifier("link_"+str(self.region)+"_edge_id_fkey"),
                              Identifier("roadnet_"+str(self.region)))
        self.conn.executeCommand(sql)


    def getParentsIds(self):
        ids = []

        sql = """SELECT stop_id from {}
                WHERE stop_type = 1
                ORDER BY stop_id;"""
        sql = SQL(sql).format(Identifier("stops_"+str(self.region)))

        try:
            cursor = self.conn.getCursor()
            # execute a statement
            cursor.execute(sql)

            # display the result
            row = cursor.fetchone()

            while row is not None:
                (parent_id,) = row
                ids.append(parent_id)
                row = cursor.fetchone()
            cursor.close()
            return ids
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except:
            print("Unexpected error:", sys.exc_info()[0])

    def getStopsParent(self):
        stop_parent = {}

        sql = """SELECT stop_id, stop_parent
                FROM {}
                ORDER BY stop_id;"""
        sql = SQL(sql).format(Identifier("stops_"+str(self.region)))

        try:
            cursor = self.conn.getCursor()
            # execute a statement
            cursor.execute(sql)

            # display the result
            row = cursor.fetchone()

            while row is not None:
                (stop_id, stop_parent_id) = row
                stop_parent[stop_id] = stop_parent_id
                row = cursor.fetchone()
            cursor.close()
            return stop_parent

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except:
            print("Unexpected error:", sys.exc_info()[0])

    def populateTableLinks(self):
        print("Creating table links...")
        stop_parent = self.getStopsParent()

        sql = """INSERT INTO {}(link_id, stop_id, edge_id, source, target, edge_dist, source_ratio, edge_length, point_location,
                source_point_geom, point_target_geom)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
        sql = SQL(sql).format(Identifier("links_"+str(self.region)))
        records = []
        link_id = 0
        list_pedestrian_ways = list(MultimodalNetwork.PEDESTRIAN_WAYS)
        try:
            cursor = self.conn.getCursor()
            sql_select = """
            WITH closest_candidates AS (
              SELECT
                id AS edge_id, source, target,
                geom_way as geom_way, km as km
              FROM
                {0}
              WHERE clazz= ANY(%s)
              ORDER BY
                geom_way <-> (select stop_location from {1} where stop_id = %s)
              LIMIT 100
            )

            SELECT edge_id, source, target,
            ST_Distance(stop_location::geography, geom_way::geography)/1000 AS edge_dist,
            source_ratio,
            km as edge_length,
            ST_LineInterpolatePoint(geom_way, source_ratio) as point_location,
            ST_GeometryN(ST_Split(ST_Snap(geom_way, ST_ClosestPoint(geom_way, stop_location), 0.00001), ST_ClosestPoint(geom_way, stop_location)), 1) as source_point_geom,
            ST_GeometryN(ST_Split(ST_Snap(geom_way, ST_ClosestPoint(geom_way, stop_location), 0.00001), ST_ClosestPoint(geom_way, stop_location)), 2) as point_target_geom
            FROM
            (SELECT geom_way, edge_id, source, target, km, stop_location as stop_location, ST_LineLocatePoint(geom_way, stop_location) as source_ratio
                FROM {1}, closest_candidates
                WHERE stop_id = %s
                ORDER BY
                ST_Distance(geom_way, stop_location)
                LIMIT 1) as r
            ;
            """
            sql_select = SQL(sql_select).format(Identifier("roadnet_"+str(self.region)),
                                                Identifier("stops_"+str(self.region)))

            for stop in stop_parent:
                parent = stop_parent[stop]
                if parent:
                    continue
                else:
                    cursor.execute(sql_select, (list_pedestrian_ways, stop, stop))
                    (edge_id, source, target, edge_dist, source_ratio, edge_length, point_location,
                     source_point_geom, point_target_geom) = cursor.fetchone()
                    records.append((link_id, stop, edge_id, source, target, edge_dist, source_ratio, edge_length,
                                             point_location, source_point_geom, point_target_geom))
                    link_id += 1

                    if link_id%100 == 0:
                        cursor.executemany(sql, records)
                        self.conn.commit()
                        records = []

            cursor.executemany(sql, records)
            self.conn.commit()
            cursor.close()
            stop_parent = None


        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        except:
            print("Unexpected error:", sys.exc_info()[0])
