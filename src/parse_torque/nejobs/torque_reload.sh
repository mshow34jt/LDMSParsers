#!/bin/bash


	cur_time=`date +%s`
	today=`date -d @$cur_time +"%Y%m%d"`
	yest_time=`expr $cur_time - 86400`
	day_before=`date -d @$yest_time +"%Y%m%d"`
	timediff=`expr $cur_time - $last_check`
	last_check=$cur_time

	/opt/isc/bin/parse_torque < /mnt/torque/$today
	
	#jobstart=`mysql -u bulkload -e "SELECT start FROM jobs WHERE jobid=$c" --silent --skip-column-names isc`;
	#		jobdate=`date -d @$jobstart +"%Y%m%d"`;
	#		jobstart2=`expr $jobstart + 86400`;
	#		jobdate2=`date -d @$jobstart2 +"%Y%m%d"`;

#			grep ";$c" /mnt/torque/$jobdate | /opt/isc/bin/parse_torque
#			grep ";$c" /mnt/torque/$jobdate2 | /opt/isc/bin/parse_torque
#			grep ";$c" /mnt/torque/$day_before | /opt/isc/bin/parse_torque
#			grep ";$c" /mnt/torque/$today | /opt/isc/bin/parse_torque

