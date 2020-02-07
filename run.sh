#!/bin/bash

if [ -z "$1" ]; then
	echo "Error: Region not defined!"
	exit 1
fi

region=$1

#export FLASK_DEBUG=1
#export APP_CONFIG_FILE=settings.py
#echo $APP_CONFIG_FILE
cd src/
python -m web.server_rr_test $region
