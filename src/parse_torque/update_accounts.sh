#!/bin/bash

FILES=/mnt/torque/201[56][0-9][0-9][0-9][0-9]
for f in $FILES
do
	echo "Loading $f"
#	cat $f | ./parse_torque
	./update_account < $f > /dev/null
done
echo "Torque account update complete."
