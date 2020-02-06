#!/bin/bash
$region=$0
echo $region

printf "Enter OSM file: "
read osm_file
if ! test -e "$osm_file"; then 
	echo "OSM file does not exist"
	exit 1
fi
printf "Enter name for region (default: root_region): "
read root_reg_name
if ["$root_reg_name" == ''];
	then root_reg_name="root_region"
fi
printf "Enter PostgreSQL hostname and press ENTER (default: localhost): "
read pg_host
if ["$pg_host" == ''];
	then pg_host="localhost";
fi
printf "Enter PostgreSQL port and press ENTER (default: 5432): "
read pg_port
if ["$pg_port" == ''];
	then pg_port="5432"
fi
printf "Enter PostgreSQL database name and press ENTER (default: motris): "
read pg_db
if ["$pg_db" == ''];
	then pg_db="motris"
fi
printf "Enter PostgreSQL username and press ENTER (default: postgres): "
read pg_user
if ["$pg_user" == ''];
	then pg_user="postgres"
fi
printf "Enter PostgreSQL password and press ENTER (default: postgres): "
read pg_password
if ["$pg_password" == ''];
	then pg_password="postgres"
fi
#
echo "BEGIN;" > temp.sql
java -jar ./osm2po/osm2po-core-5.1.0-signed.jar workDir=./temp cmd=c prefix="$root_reg_name" $osm_file
cat "./temp/"$root_reg_name"_2po_4pgr.sql" >> temp.sql
echo "ALTER TABLE "$root_reg_name"_2po_4pgr RENAME TO $root_reg_name;" >> temp.sql
echo "ALTER TABLE "$root_reg_name" ADD COLUMN source_location GEOMETRY;" >> temp.sql
echo "ALTER TABLE "$root_reg_name" ADD COLUMN target_location GEOMETRY;" >> temp.sql
echo "CREATE INDEX "$root_reg_name"_source_idx ON "$root_reg_name" USING gist (source_location);" >> temp.sql
echo "CREATE INDEX "$root_reg_name"_target_idx ON "$root_reg_name" USING gist (target_location);" >> temp.sql
echo "UPDATE "$root_reg_name" SET source_location=ST_MakePoint(x1,y1), target_location=ST_MakePoint(x2,y2);" >> temp.sql
echo "ALTER TABLE "$root_reg_name" DROP COLUMN x1;" >> temp.sql
echo "ALTER TABLE "$root_reg_name" DROP COLUMN y1;" >> temp.sql
echo "ALTER TABLE "$root_reg_name" DROP COLUMN x2;" >> temp.sql
echo "ALTER TABLE "$root_reg_name" DROP COLUMN y2;" >> temp.sql
echo "ALTER TABLE "$root_reg_name" DROP COLUMN source;" >> temp.sql
echo "ALTER TABLE "$root_reg_name" DROP COLUMN target;" >> temp.sql
echo "END;" >> temp.sql
PGPASSWORD="$pg_password" psql -h "$pg_host" -d "$pg_db" -U "$pg_user" -p "$pg_port" -f temp.sql
rm -rf ./temp
rm temp.sql

