#!/bin/bash

if [ -z "$1" ]; then
	echo "Error: Region not defined!"
	exit 1
fi

region=$1

cur_dir=$PWD
SCRIPT=$(dirname `realpath $0`)
cd $SCRIPT
#dir_root=$(dirname $(dirname $(dirname "$SCRIPT")))
dir=../../data/osm/$region/

for file in "$dir"*; do
            if [ -f "$file" ]; then
		osm_file=$file
                break 1
            fi
done

echo $osm_file
if [ -z "$osm_file" ]; then
	echo "Error: OSM file not found!"
	exit 2
fi
#echo $osm_file

source <(grep = ../../config.ini)

pg_host=$host
pg_port=$port
pg_db=$database
pg_user=$user
pg_password=$password

if [ -z "$pg_host" ] || [ -z "$pg_port" ] || [ -z "$pg_db" ] || [ -z "$pg_user" ] || [ -z "$pg_password" ]; then
	echo "Error: Database configuration not defined!"
	exit 3
fi

root_reg_name="roadnet_${region}"
#echo $root_reg_name

#
echo "BEGIN;" > temp.sql
java -jar ./osm2po/osm2po-core-5.1.0-signed.jar workDir=./temp cmd=c prefix="$root_reg_name" $osm_file
cat "./temp/"$root_reg_name"_2po_4pgr.sql" >> temp.sql
echo "ALTER TABLE "$root_reg_name"_2po_4pgr RENAME TO $root_reg_name;" >> temp.sql
echo "ALTER TABLE "$root_reg_name" ADD COLUMN source_location GEOMETRY;" >> temp.sql
echo "ALTER TABLE "$root_reg_name" ADD COLUMN target_location GEOMETRY;" >> temp.sql
echo "CREATE INDEX "$root_reg_name"_source_idx ON "$root_reg_name" USING gist (source_location);" >> temp.sql
echo "CREATE INDEX "$root_reg_name"_target_idx ON "$root_reg_name" USING gist (target_location);" >> temp.sql
echo "CREATE INDEX "$root_reg_name"_clazz_idx ON "$root_reg_name" USING btree (clazz);" >> temp.sql
echo "CREATE UNIQUE INDEX "$root_reg_name"_edge_idx ON "$root_reg_name" USING btree (id);" >> temp.sql
echo "CREATE INDEX "$root_reg_name"_geom_way_idx ON "$root_reg_name" USING gist (geom_way);" >> temp.sql

echo "UPDATE "$root_reg_name" SET source_location=ST_SetSRID(ST_MakePoint(x1,y1), 4326), target_location=ST_SetSRID(ST_MakePoint(x2,y2), 4326);" >> temp.sql
echo "ALTER TABLE "$root_reg_name" DROP COLUMN x1;" >> temp.sql
echo "ALTER TABLE "$root_reg_name" DROP COLUMN y1;" >> temp.sql
echo "ALTER TABLE "$root_reg_name" DROP COLUMN x2;" >> temp.sql
echo "ALTER TABLE "$root_reg_name" DROP COLUMN y2;" >> temp.sql
#echo "ALTER TABLE "$root_reg_name" DROP COLUMN source;" >> temp.sql
#echo "ALTER TABLE "$root_reg_name" DROP COLUMN target;" >> temp.sql
echo "END;" >> temp.sql
PGPASSWORD="$pg_password" psql -h "$pg_host" -d "$pg_db" -U "$pg_user" -p "$pg_port" -f temp.sql
rm -rf ./temp
rm temp.sql

cd $cur_dir
