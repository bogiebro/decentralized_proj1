#!/bin/sh
rm *.log
kill -9 $(sudo lsof -i:8000-8010 -i:9000-9010 -i:7000-7010 | awk 'NR > 1 {print $2}')
