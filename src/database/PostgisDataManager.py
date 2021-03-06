'''
Created on Oct 31, 2019

@author: camila
'''
import json

from geojson import Feature, FeatureCollection
from geojson import loads
from geojson.geometry import LineString
import psycopg2

from .PostGISConnection import PostGISConnection
from ..gtfs import GTFS

class PostgisDataManager:

    def __init__(self):
        self.connection = PostGISConnection()

    def getNetworkMBR(self, region):
        sql = """SELECT ST_Extent(polygon) from neighborhoods_{0} WHERE level =
                (SELECT min(level) FROM neighborhoods_{0});
            """
        sql = sql.format(region)

        try:
            self.connection.connect()

            cursor = self.connection.getCursor()

            cursor.execute(sql)
            (mbr, ) = cursor.fetchone()
            mbr = mbr.replace("BOX", "").replace("(", "").replace(")", "").replace(",", " ")
            mbr_li = [float(i) for i in list(mbr.split(" "))]
            self.connection.close()

            return mbr_li

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getNetworkCenter(self, region):
        mbr = self.getNetworkMBR(region)
        lat_center = float((mbr[1] + mbr[3])/2)
        long_center = float((mbr[0] + mbr[2])/2)
        return [long_center, lat_center]

    def getNumNodes(self, region):
        sql = """SELECT max(GREATEST(source, target)) FROM roadnet_{};
            """
        sql = sql.format(region)

        try:
            self.connection.connect()

            cursor = self.connection.getCursor()

            cursor.execute(sql)
            (max_id, ) = cursor.fetchone()
            self.connection.close()

            return max_id

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getChildrenStops(self, region, parent_id):
        sql = """SELECT stop_id
                FROM stops_{0}
                WHERE stop_parent = %s;
            ;
            """
        sql = sql.format(region)

        try:
            stops = []
            self.connection.connect()

            cursor = self.connection.getCursor()

            cursor.execute(sql, (parent_id, ))
            row = cursor.fetchone()

            while row is not None:
                (stop_id,) = row
                stops.append(stop_id)
                row = cursor.fetchone()

            self.connection.close()
            if not stops:
                stops.append(parent_id)
            return stops

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getNeighborhoodsPolygons(self, region):
        sql = """SELECT id, name, level, parent, ST_AsGeoJSON(polygon) as polygon
                FROM neighborhoods_{};
            ;
            """
        sql = sql.format(region)

        try:
            features = []
            self.connection.connect()

            cursor = self.connection.getCursor()

            cursor.execute(sql)
            row = cursor.fetchone()

            while row is not None:
                (nid, name, level, parent, polygon) = row
                properties = {'name': name, 'level':level, 'parent': parent}
                geometry = json.loads(polygon)
                feature = Feature(geometry = geometry, properties = properties, id = nid)
                features.append(feature)

                row = cursor.fetchone()

            self.connection.close()
            return FeatureCollection(features)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getStopsLocation(self, region, stops_level):
        sql = """select stop_id, stop_name, ST_AsGeoJSON(stop_location)
                from stops_{0}
                where stop_parent is null and
                ST_Within(stop_location, (select polygon from neighborhoods_{0} where level = (
                select min(level) from neighborhoods_{0}) ))
            ;
            """
        sql = sql.format(region)

        try:
            features = {}
            self.connection.connect()

            cursor = self.connection.getCursor()

            cursor.execute(sql)
            row = cursor.fetchone()

            while row is not None:
                (stop_id, name, location) = row
                if stop_id in stops_level:
                    stop_level = stops_level[stop_id]
                    properties = {'name': name}
                    geometry = json.loads(location)
                    feature = Feature(geometry = geometry, properties = properties, id = stop_id)

                    if stop_level in features:
                        features[stop_level].append(feature)
                    else:
                        features[stop_level] = [feature]
                row = cursor.fetchone()

            for level in features:
                features[level] = FeatureCollection(features[level])

            self.connection.close()
            stops_level = None
            return features

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getRoutes(self, region):
        try:
            sql_stops = """ select r.route_id, r.route_short_name, stops, stop_names, r.route_type
                            from routes_geometry_{0} rg, routes_{0} r
                            where rg.route_id = r.route_id;
                    """
            sql_stops = sql_stops.format(region)

            routes = {}
            route_stops = {}
            stop_routes = {}
            stop_level = {}
            self.connection.connect()

            cursor = self.connection.getCursor()

            cursor.execute(sql_stops)
            row = cursor.fetchone()

            while row is not None:
                (route_id, route_name, stop_ids, stop_names, route_type ) = row
                if route_type not in GTFS.ROUTE_LEVEL:
                    row = cursor.fetchone()
                    continue
                route_level = GTFS.ROUTE_LEVEL[route_type]
                list_stop_names = []
                for i in range(0,len(stop_ids)):
                    stop_id = stop_ids[i]
                    list_stop_names.append([stop_id,stop_names[i]])
                    stop_route_id = route_name + "_" + str(route_type)
                    if stop_id in stop_routes:
                        if route_level in stop_routes[stop_id]:
                            if stop_route_id not in stop_routes[stop_id][route_level]:
                                stop_routes[stop_id][route_level].append(stop_route_id)
                        else:
                            stop_routes[stop_id][route_level] = [stop_route_id]
                    else:
                        stop_routes[stop_id]={route_level : [stop_route_id]}

                    if route_type in GTFS.ROUTE_LEVEL:
                        if stop_id in stop_level:
                            if stop_level[stop_id] > route_level:
                                stop_level[stop_id] = route_level
                        else:
                            stop_level[stop_id] = route_level

                if route_type in GTFS.ROUTE_TYPE:
                    route_stop_name = route_name + "_" + str(route_type)
                    if route_type in routes:
                        list_routes = routes[route_type]
                        if route_name in list_routes:
                            list_routes[route_name].append(route_id)
                            list_stops = route_stops[route_stop_name]
                            for stop in list_stop_names:
                                if stop not in list_stops:
                                    list_stops.append(stop)
                        else:
                            list_routes[route_name] = [route_id]
                            route_stops[route_stop_name] = list_stop_names
                    else:
                        routes[route_type] = {route_name: [route_id]}
                        route_stops[route_stop_name] = list_stop_names

                row = cursor.fetchone()

            self.connection.close()
            return routes, route_stops, stop_routes, stop_level

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getPointsWithinPolygon(self, region, coordinates):
        sql = """SELECT source as id FROM roadnet_{0} as p,
                (SELECT {1}  as polygon) as pol
                 WHERE ST_Within(source_location, polygon)
                UNION
                SELECT target as id FROM roadnet_{0} as p,
                (SELECT {1} as polygon) as pol
                WHERE ST_Within(target_location, polygon)
            ;
            """
        sql_polygon = "ST_Polygon('LINESTRING("

        for c in coordinates[0]:
            sql_polygon += str(c[0]) + " " + str(c[1]) + ","

        sql_polygon = sql_polygon[:-1]

        sql_polygon += ")'::geometry, 4326)"
        sql = sql.format(region, sql_polygon)

        try:
            ids = set()
            self.connection.connect()

            cursor = self.connection.getCursor()

            cursor.execute(sql)
            row = cursor.fetchone()

            while row is not None:
                (node_id,) = row
                ids.add(node_id)
                row = cursor.fetchone()

            self.connection.close()
            return ids

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


    def getPointsWithinNeighborhoods(self, region, selected_neig):
        sql = """SELECT nodes from neighborhood_nodes_{0} where id = %s;
            """.format(region)
        points = []
        try:
            self.connection.connect()

            cursor = self.connection.getCursor()

            for n in selected_neig:
                cursor.execute(sql, (n,))
                (nodes, ) = cursor.fetchone()
                points.extend(nodes)


            self.connection.close()
            return points

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


    def getClosestEdgeRatio(self, lat, lon, region):
        sql = """SELECT id, source, target,
                ST_LineLocatePoint(geom_way, point) as source_ratio,
                ST_X(ST_LineInterpolatePoint(geom_way, ST_LineLocatePoint(geom_way, point))) as lon,
                ST_Y(ST_LineInterpolatePoint(geom_way, ST_LineLocatePoint(geom_way, point))) as lat
                from roadnet_{},
                (SELECT ST_SetSRID(ST_MakePoint(%s, %s),4326) as point) as p
                ORDER BY point <-> geom_way
                LIMIT 1;
            """.format(region)

        try:
            self.connection.connect()

            cursor = self.connection.getCursor()

            cursor.execute(sql, (lon, lat))
            (edge_id, source, target, source_ratio, node_lon, node_lat) = cursor.fetchone()

            self.connection.close()
            return (edge_id, source, target, source_ratio, node_lon, node_lat)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getClosestVertex(self, lat, lon,region):
        sql_source = """SELECT ST_Distance(point, source_location), source
                FROM roadnet_{}, (SELECT ST_SetSRID(ST_MakePoint(%s, %s),4326) as point) as p
                ORDER BY ST_Distance(point, source_location)
                LIMIT 1;
            """.format(region)
        sql_target = """SELECT ST_Distance(point, target_location), target
                    FROM roadnet_{}, (SELECT ST_SetSRID(ST_MakePoint(%s, %s),4326) as point) as p
                    ORDER BY ST_Distance(point, target_location)
                    LIMIT 1;
            """.format(region)
        try:
            conn = self.connection.connect()
            with self.connection.getCursor() as cursor:
                #cursor = self.connection.getCursor()
                cursor.execute(sql_source, (lon, lat))
                (source_dist, source_id) = cursor.fetchone()
                cursor.execute(sql_target, (lon, lat))
                (target_dist, target_id) = cursor.fetchone()
                self.connection.close(conn)
                if source_dist <= target_dist:
                    return source_id
                else:
                    return target_id

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getClosestVertex_CLASSDEPENDANT(self, lat, lon,region, class_list):
        sql_source = """SELECT ST_Distance(point, source_location), source
                FROM roadnet_{}, (SELECT ST_SetSRID(ST_MakePoint(%s, %s),4326) as point) as p
                WHERE clazz = ANY(%s::int[])
                ORDER BY ST_Distance(point, source_location)

                LIMIT 1;
            """.format(region)
        sql_target = """SELECT ST_Distance(point, target_location), target
                    FROM roadnet_{}, (SELECT ST_SetSRID(ST_MakePoint(%s, %s),4326) as point) as p
                    WHERE clazz = ANY(%s::int[])
                    ORDER BY ST_Distance(point, target_location)
                    LIMIT 1;
            """.format(region)
        try:
            conn = self.connection.connect()
            cursor = self.connection.getCursor()
            cursor.execute(sql_source, (lon, lat, list(class_list)))
            (source_dist, source_id) = cursor.fetchone()
            cursor.execute(sql_target, (lon, lat, list(class_list)))
            (target_dist, target_id) = cursor.fetchone()
            self.connection.close(conn)
            if source_dist <= target_dist:
                return source_id
            else:
                return target_id

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getClosestEdgeGeometry(self, lat, lon, region):
        sql = """SELECT id, source, target, ST_AsGeoJSON(geom_way)
                from roadnet_{},
                (SELECT ST_SetSRID(ST_MakePoint(%s, %s),4326) as point) as p
                ORDER BY point <-> geom_way
                LIMIT 1;
            """
        sql = sql.format(region)

        try:
            self.connection.connect();

            cursor = self.connection.getCursor()

            cursor.execute(sql, (lon, lat))
            (edge_id, source, target, geometry) = cursor.fetchone()

            self.connection.close()
            return edge_id, source, target, json.loads(geometry)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getClosestEdgeByClass(self, lat, lon, region, class_list):
        sql = """SELECT id, source, target,
                ST_LineLocatePoint(geom_way, point) as source_ratio,
                ST_X(ST_LineInterpolatePoint(geom_way, ST_LineLocatePoint(geom_way, point))) as lon,
                ST_Y(ST_LineInterpolatePoint(geom_way, ST_LineLocatePoint(geom_way, point))) as lat
                from roadnet_{},
                (SELECT ST_SetSRID(ST_MakePoint(%s, %s),4326) as point) as p
                WHERE clazz = ANY(%s::int[])
                ORDER BY point <-> geom_way
                LIMIT 1;
            """
        sql = sql.format(region)

        try:
            self.connection.connect();

            cursor = self.connection.getCursor()

            cursor.execute(sql, (lon, lat, list(class_list)))
            (edge_id, source, target, source_ratio, node_lon, node_lat) = cursor.fetchone()

            self.connection.close()
            return (edge_id, source, target, source_ratio, node_lon, node_lat)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getRoadGeometry(self, edge_id, region):
        sql = """SELECT ST_AsGeoJSON(geom_way) from roadnet_{}
            WHERE id = %s ;
            """
        sql = sql.format(region)

        try:
            self.connection.connect();

            cursor = self.connection.getCursor()

            cursor.execute(sql, (edge_id, ))
            (geometry, ) = cursor.fetchone()

            self.connection.close()
            return geometry

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getRouteGeometry(self, route_name, transp_id, region):
        sql = """SELECT ST_AsGeoJSON(route_geom) from routes_geometry_{0}
            WHERE route_id in
            (SELECT route_id from routes_{0} where route_short_name = %s and route_type = %s);
            """
        sql = sql.format(region)

        try:
            self.connection.connect();
            geometries = []

            cursor = self.connection.getCursor()

            cursor.execute(sql, (route_name, transp_id))
            row = cursor.fetchone()
            while row is not None:
                (geometry, ) = row
                line = loads(geometry)
                lineString = LineString(line.coordinates)
                feature = Feature(geometry = lineString)
                geometries.append(feature)
                row = cursor.fetchone()


            self.connection.close()
            return geometries

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getLinkGeometry(self, edge_id, edge_pos, region):
        if edge_pos == 1:
            sql = """SELECT ST_AsGeoJSON(source_point_geom) from links_{}
                WHERE link_id = %s ;
                """

        if edge_pos == 2:
            sql = """SELECT ST_AsGeoJSON(point_target_geom) from links_{}
                WHERE link_id = %s ;
                """

        sql = sql.format(region)

        try:
            self.connection.connect();

            cursor = self.connection.getCursor()

            cursor.execute(sql, (edge_id,))
            (geometry, ) = cursor.fetchone()

            self.connection.close()
            return geometry

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
