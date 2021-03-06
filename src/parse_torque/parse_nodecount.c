/*
** cluser peak analysis
** version 0.1a
*/

#include "mysql.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <math.h>


#define MAXBUFSIZE 1049394 /* max number of bytes we can get at once */
#define MAXLINESIZE 8395152
//#define MAXLINESIZE 4197576
//#define MAXLINESIZE 2098788

int main(){
    MYSQL mysql;
    MYSQL mysql_remote;
    MYSQL_RES *rs_var, *rs, *rs2;
    MYSQL_ROW var_row, row, row2;
    char buf[MAXBUFSIZE];
    char buf2[128];
    int nDelay, nHosts, nProcs, nCollected, nHostTries, MAXCollectTime, StoreHistory, HistoryLength, iPID, availcolors, i, j, k;
    pid_t pID, ppID;
    //time_t unixtime;
    //tm unixtime;

    //unixtime = time(NULL);
    //unixtime = new tm;

//================================
//    float Vals[MAXSIZE], maxDif, segDif, sumDif, meanDif, dev, sumsqr, stdev, sumVals, sdFactor;
    int count, maxIdx, newIdx, n, curClisterSize, curClusterCount, lastClusters ;

//================================
    time_t rawtime;
    struct tm * timeinfo;
    int year, month, day, hour, min, sec;
    int jobid;
    char *pch;
    char *rectype;
    char *data;
    char *pair;
    char *val;
    char var[256];
    char linein[MAXLINESIZE];
    char sqlbuf[MAXBUFSIZE+256];

    char status[16];
    char user[64];
    char group[32];
    char jobname[256];
    char queue[64];
    char ctime[16], qtime[16], etime[16], start[16], end[16];
    char owner[128];
    char RLneednodes[16], RLnodect[16], RLnodes[16];
    char RLwalltime[32];
    char session[16], exitstatus[16];
    char RUcput[32];
    char RUmem[32];
    char RUvmem[32];
    char RUwalltime[32];
    FILE *qstat;
    char qstatline[512];
    char *puser;
    char curhost[8];
    char lasthost[8];
    //char procnum[8];
    int  hostprocs;
    int  jobnodes;
    char *phost;
    char *pproc;
    char *psave1;
    char *psave2;


    mysql_init(&mysql);
    if(!mysql_real_connect(&mysql, "localhost", "isc", "clumon", "isc", 0, NULL, 0)){
        printf("ERROR connecting to database\n\nExiting\n");
        exit(3);
    }

//mysql_query(&mysql, "TRUNCATE sched_acct_log");
//mysql_query(&mysql, "TRUNCATE jobs");


while(fgets(linein, sizeof linein, stdin) != NULL){
        //puts(linein);

        if (linein != NULL){
        if (strlen(linein)>0){
//x printf("RECV: ===%s===\n", linein);
        
      	month = atoi(strtok(linein,"/")); 
//printf("CHK A\n");
	day = atoi(strtok(NULL, "/"));
//printf("CHK A1\n");
	year = atoi(strtok(NULL, " "));        
//printf("CHK B\n");
	hour = atoi(strtok(NULL, ":"));
	min = atoi(strtok(NULL, ":"));
	sec = atoi(strtok(NULL, ";"));
//printf("CHK 1\n");
	rectype = strtok(NULL, ";");
	jobid = atoi(strtok(NULL, ".["));
	strtok(NULL, ";");
	data = strtok(NULL, "\n");	

//	sprintf(sqlbuf, "INSERT INTO sched_acct_log SELECT UNIX_TIMESTAMP('%d-%d-%d %d:%d:%d'), %d, '%s', '', '', '%s'\n", year,month,day,hour,min,sec, jobid, rectype, data);
//printf("LOG INSERT='%s'\n", sqlbuf);
//        i = mysql_query(&mysql, sqlbuf);
	}


//	printf("===================\n");
//	printf("Found a %s record for job: %d.\n", rectype, jobid);	

	//clear variables - to be safe
	sprintf(user, "");
        sprintf(group,"");
        sprintf(jobname, "");
	sprintf(queue, "");
        sprintf(ctime, "");
        sprintf(qtime, "");
        sprintf(etime, "");
        sprintf(start, "");
        sprintf(end, "");
        sprintf(owner, "");
        sprintf(RLneednodes, "");
        sprintf(RLnodect, "");
        sprintf(RLnodes, "");
        sprintf(RLwalltime, "");
        sprintf(session, "");
        sprintf(exitstatus, "");
        sprintf(RUcput, "");
        sprintf(RUmem, "");
        sprintf(RUvmem, "");
        sprintf(RUwalltime, "");
	//do the rest here	



	pair = strtok_r(data, " ", &psave1);
	while (pair !=NULL) {
//		printf("Pair: %s\n", pair);
		val = strpbrk (pair, "=");
		if (val != NULL){		
		//printf("     val='%s'\n", val+1);
		strncpy(var, pair, val-pair);
		var[val-pair] = '\0';
		//printf("     var='%s'\n", var); 
//		printf("     '%s'='%s'\n", var, val+1);

		// map variables
		if (strcmp(var,"user")==0) { sprintf(user, "%s", val+1); }
		if (strcmp(var,"group")==0) { sprintf(group, "%s", val+1); }
		if (strcmp(var,"jobname")==0) { sprintf(jobname, "%s", val+1); }
                if (strcmp(var,"queue")==0) { sprintf(queue, "%s", val+1); }
		if (strcmp(var,"ctime")==0) { sprintf(ctime, "%s", val+1); }
		if (strcmp(var,"qtime")==0) { sprintf(qtime, "%s", val+1); }
		if (strcmp(var,"etime")==0) { sprintf(etime, "%s", val+1); }
		if (strcmp(var,"start")==0) { sprintf(start, "%s", val+1); }
		if (strcmp(var,"end")==0) { sprintf(end, "%s", val+1); }
		if (strcmp(var,"owner")==0) { sprintf(owner, "%s", val+1); }
		if (strcmp(var,"Resource_List.neednodes")==0) { sprintf(RLneednodes, "%s", val+1); }
		if (strcmp(var,"Resource_List.nodect")==0) { sprintf(RLnodect, "%s", val+1); }
		if (strcmp(var,"Resource_List.nodes")==0) { sprintf(RLnodes, "%s", val+1); }
		if (strcmp(var,"Resource_List.walltime")==0) { sprintf(RLwalltime, "%s", val+1); }
		if (strcmp(var,"session")==0) { sprintf(session, "%s", val+1); }
		if (strcmp(var,"Exit_status")==0) { sprintf(exitstatus, "%s", val+1); }
		if (strcmp(var,"resources_used.cput")==0) { sprintf(RUcput, "%s", val+1); }
		if (strcmp(var,"resources_used.mem")==0) { sprintf(RUmem, "%s", val+1); }
		if (strcmp(var,"resources_used.vmem")==0) { sprintf(RUvmem, "%s", val+1); }
		if (strcmp(var,"resources_used.walltime")==0) { sprintf(RUwalltime, "%s", val+1); }

//printf("PAIR:%s | %s\n", var, val+1);

		if (strcmp(var,"exec_host")==0 && strcmp(rectype, "S")==0) {
			//printf("JOBID=%d\nEXECHOSTS=%s\n", jobid, val+1);
			jobnodes=0;
			hostprocs=0;
			phost = strtok_r(val+1, "/ ", &psave2);
			//sprintf(curhost,"%s", phost);
			//x sprintf(lasthost,"", phost);
			//x sprintf(sqlbuf, "INSERT INTO job_hosts VALUES(%d, %s)", jobid, phost);
			//x i = mysql_query(&mysql, sqlbuf);
			do{
				
				//printf("Host:%s\n", phost);
				sprintf(curhost, "%s", phost);
				if (strcmp(phost,lasthost)!=0){
					//printf("Host:%s\n", phost);
					sprintf(lasthost, "%s", phost);
					jobnodes = jobnodes + 1;
					//x sprintf(sqlbuf, "INSERT INTO job_hosts VALUES(%d, %s)", jobid, phost);
					//x i = mysql_query(&mysql, sqlbuf);
				}
				pproc = strtok_r(NULL,"+ ", &psave2);
				hostprocs = hostprocs + 1;
			}while(phost = strtok_r(NULL,"/ ", &psave2));
			
			sprintf(lasthost, "");
			//printf("HOSTS=%d\nPROCS=%d\n", jobnodes, hostprocs);
			sprintf(sqlbuf, "UPDATE jobs SET node_count=%d WHERE jobid=%d", jobnodes, jobid);
			printf("%s\n", sqlbuf);
			i = mysql_query(&mysql, sqlbuf);

		}


		if (strlen(RLnodect) == 0) {sprintf(RLnodect, "-1"); }
		}
		pair = strtok_r(NULL, " ", &psave1);
	}

	sprintf(sqlbuf, "");
	//printf("U/Q = '%s'-'%s'\n", user, queue);


/*
	if (strcmp(rectype, "Q") == 0) {
//printf("Q\n");
		sprintf(sqlbuf, "INSERT INTO jobs (jobid, status, queue)  VALUES (%d, 'Queued', '%s')", jobid, queue); 
		i = mysql_query(&mysql, sqlbuf);
		//i = mysql_query(&mysql_remote, sqlbuf);
		//printf ("Query: '%s'\n", sqlbuf);
	}

	if (strcmp(rectype, "R") == 0) {
		sprintf(sqlbuf, "UPDATE jobs SET status='Queued' WHERE jobid=%d", jobid);
                i = mysql_query(&mysql, sqlbuf);
		//i = mysql_query(&mysql_remote, sqlbuf);
	}

	if (strcmp(rectype, "S") == 0) {
//printf("S\n");
		//sprintf(sqlbuf, "INSERT INTO jobs(jobid, status, user, user_group, jobname, queue, ctime, qtime, etime, start, owner, RLneednodes, RLnodect, RLnodes, RLwalltime, tc) VALUES(%d, 'Running', '%s', '%s', '%s', '%s', %s, %s, %s, %s, '%s', '%s', '%s', '%s', '%s', 0) ON DUPLICATE KEY UPDATE status='Running', user='%s', user_group='%s', jobname='%s', queue='%s', ctime=%s, qtime=%s, etime=%s, start=%s, owner='%s', RLneednodes='%s', RLnodect='%s', RLnodes='%s', RLwalltime='%s'", jobid, user, group, jobname, queue, ctime, qtime, etime, start, owner, RLneednodes, RLnodect, RLnodes, RLwalltime, user, group, jobname, queue, ctime, qtime, etime, start, owner, RLneednodes, RLnodect, RLnodes, RLwalltime);
//              sprintf(sqlbuf, "INSERT INTO jobs(jobid, status, user, user_group, jobname, queue, ctime, qtime, etime, start, owner, RLneednodes, RLnodect, RLnodes, RLwalltime, tc) VALUES(%s, 'Running', '%s', '%s', '%s', '%s', %s, %s, %s, %s, '%s', 0)", jobid, user, group, jobname, queue, ctime, qtime, etime, start, owner);
//              sprintf(sqlbuf, "INSERT INTO jobs(jobid, status, user, user_group, jobname, queue, ctime, qtime, etime, start, owner, RLneednodes, RLnodect, RLnodes, RLwalltime, tc) VALUES(%s, 'Running', 0)", jobid );


//printf ("Query: '%s'\n", sqlbuf);

		//sprintf(sqlbuf, "UPDATE jobs set status='Running', user='%s', user_group='%s', jobname='%s', queue='%s', ctime=%s, qtime=%s, etime=%s, start=%s, owner='%s', RLneednodes='%s', RLnodect=%s, RLnodes='%s', RLwalltime='%s' WHERE jobid=%d", user, group, jobname, queue, ctime, qtime, etime, start, owner, RLneednodes, RLnodect, RLnodes, RLwalltime, jobid);

		//i = mysql_query(&mysql, sqlbuf);
		//i = mysql_query(&mysql_remote, sqlbuf);
		//printf ("Query: '%s'\n", sqlbuf);
	}
	
	if (strcmp(rectype, "E") == 0) {
                sprintf(sqlbuf, "UPDATE jobs set status='Completed', user='%s', user_group='%s', jobname='%s', queue='%s', ctime=%s, qtime=%s, etime=%s, start=%s, owner='%s', RLneednodes='%s', RLnodect='%s', RLnodes='%s', RLwalltime='%s', session=%s, end=%s, exit_status=%s, RUcput='%s', RUmem='%s', RUvmem='%s', RUwalltime='%s' WHERE jobid=%d", user, group, jobname, queue, ctime, qtime, etime, start, owner, RLneednodes, RLnodect, RLnodes, RLwalltime, session, end, exitstatus, RUcput, RUmem, RUvmem, RUwalltime, jobid);
                i = mysql_query(&mysql, sqlbuf);
		//i = mysql_query(&mysql_remote, sqlbuf);
//                printf ("Query: '%s'\n", sqlbuf);
        }

	if (strcmp(rectype, "A") == 0) {
                sprintf(sqlbuf, "UPDATE jobs set status='Aborted' WHERE jobid=%d", jobid);
                i = mysql_query(&mysql, sqlbuf);
		//i = mysql_query(&mysql_remote, sqlbuf);
//              printf ("Query: '%s'\n", sqlbuf);
	}

	if (strcmp(rectype, "D") == 0) {
                sprintf(sqlbuf, "UPDATE jobs set status='Deleted' WHERE jobid=%d", jobid);
                i = mysql_query(&mysql, sqlbuf);
		//i = mysql_query(&mysql_remote, sqlbuf);
//              printf ("Query: '%s'\n", sqlbuf);
	}

        if (strcmp(rectype, "H") == 0) {
                sprintf(sqlbuf, "UPDATE jobs set status='On Hold' WHERE jobid=%d", jobid);
                i = mysql_query(&mysql, sqlbuf);
		//i = mysql_query(&mysql_remote, sqlbuf);
//              printf ("Query: '%s'\n", sqlbuf);
	}

	
//printf("DUPLICATE=%s\n", sqlbuf);
//	i = mysql_query(&mysql, sqlbuf);
	//i = mysql_query(&mysql_remote, sqlbuf);


//	printf("===================\n");
	sprintf(sqlbuf, "UPDATE isc_feeds_last_data SET last_data=UNIX_TIMESTAMP() WHERE feed='torque'");
	i = mysql_query(&mysql, sqlbuf);
*/
}



}	



/*
    while(cptr = fgets(linein,MAXBUFSIZE,stdin)){

	if (cptr != NULL){
		printf("RECV: ===%s===\n", cptr);
	}



    }
*/

//printf("Program complete\n");
return 0;


}
