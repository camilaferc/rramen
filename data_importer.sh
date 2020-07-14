#!/bin/bash

if [ -z "$1" ]; then
	echo "Error: Region not defined!"
	exit 1
fi

region=$1

./scripts/import_osm/import_osm.sh $region

cd src/
python -m run.dataImporter $region
