#!/bin/bash

cores=$(nproc --all)
(( workers = $cores*2 + 1 ))
gunicorn -w $workers -t 120 --preload src.wsgi.wsgi:app
