#!/bin/bash

last_restart=0
last_check=0
while read a b c ; do

	cur_time=`date +%s`
	today=`date -d @$cur_time +"%Y%m%d"`
	yest_time=`expr $cur_time - 86400`
	day_before=`date -d @$yest_time +"%Y%m%d"`
	timediff=`expr $cur_time - $last_check`
	last_check=$cur_time
	#echo "[$today $day_before $last_check] A=$a B=$b C=$c ($timediff sec)"
#	case "$b" in
#		*)
			#echo "$c bang"
#			echo "got an o"
#jobid=$c
			#echo "$cur_time - handling o for $c" >> /tmp/jcc_log_$today
			#echo "$cur_time - handling o for $c"
			jobstart=`mysql -u bulkload -e "SELECT start FROM jobs WHERE jobid=$c" --silent --skip-column-names isc`;
			jobdate=`date -d @$jobstart +"%Y%m%d"`;
			jobstart2=`expr $jobstart + 86400`;
			jobdate2=`date -d @$jobstart2 +"%Y%m%d"`;

#echo "boo";
			printf "Working on: $c : $jobdate : $jobdate2\n"

			grep ";$c" /mnt/torque/$jobdate | /opt/isc/bin/parse_torque
			grep ";$c" /mnt/torque/$jobdate2 | /opt/isc/bin/parse_torque
#			grep ";$c" /mnt/torque/$day_before | /opt/isc/bin/parse_torque
#			grep ";$c" /mnt/torque/$today | /opt/isc/bin/parse_torque

#			mysql -u bulkload -e "INSERT INTO job_orphans (jobid, ocount, utime) values($c, 1, $cur_time) ON DUPLICATE KEY UPDATE ocount=ocount+1, utime=$cur_time" isc
			#echo "Job time = $jobtime"
			#mysql -u bulkload -e "UPDATE jobs SET status='Suspect' WHERE jobid IN (SELECT jobid FROM job_orphans WHERE ocount>=3)" isc
			#mysql -u bulkload -e "DELETE FROM job_orphans WHERE ocount>=3" isc

#			;;
#		m)
#			echo "got a m"
#			;;
#                q)
#                        echo "got a q"
			#echo "$cur_time - handling q for $c" >> /tmp/jcc_log_$today
			#mysql -u bulkload -e "INSERT INTO ztmp values($c)" isc

#			grep ";$c" /mnt/torque/$day_before | /opt/isc/bin/parse_torque
#			grep ";$c" /mnt/torque/$today | /opt/isc/bin/parse_torque

			# clean up instances where the job got re-queued after an end time has been set (i.e. after job has an end record)
#                        mysql -u bulkload -e "UPDATE jobs SET status='Completed' WHERE jobid=$c AND end IS NOT NULL" --silent isc

#                       ;;
#                r)
#                        echo "got a r"
#                        ;;
#		*)
#			echo "got nada"
#			;;
#	esac

done;

