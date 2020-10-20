#!/bin/bash

region=$(awk -F "=" '/region/ {print $2}' config.ini)

echo $region

./scripts/import_osm/import_osm.sh $region

python3 -m src.run.dataImporter $region
