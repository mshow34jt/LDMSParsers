#!/bin/bash

FILES=$1/*
for f in $FILES
do
	echo "Loading $f"
#	cat $f | ./parse_torque
	./parse_torque < $f > /dev/null
done
echo "Torque accounting file loading complete."
