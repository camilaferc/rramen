#!/bin/bash

if [ -z "$1" ]; then
	echo "Error: Region not defined!"
	exit 1
fi

region=$1

cd src/
python -m web.server_rramen $region
