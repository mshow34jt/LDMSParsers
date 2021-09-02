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
		char account[32];
    char jobname[256];
    char queue[64];
    char ctime[16], qtime[16], etime[16], start[16], end[16];
    char owner[128];
    char RLneednodes[16], RLnodect[16], RLnodes[16];
    char RLwalltime[32];
    char RLminwclimit[32];
    char RLflags[512];
    char login_node[32];
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
//printf("RECV: ===%s===\n", linein);
        
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
        sprintf(account,"");
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
        sprintf(RLminwclimit, "");
        sprintf(RLflags, "");
        sprintf(login_node, "");
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
    if (strcmp(var,"account")==0) { sprintf(account, "%s", val+1); }
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
    if (strcmp(var,"Resource_List.minwclimit")==0) { sprintf(RLminwclimit, "%s", val+1); }
    if (strcmp(var,"Resource_List.flags")==0) { sprintf(RLflags, "%s", val+1); }
    if (strcmp(var,"login_node")==0) { sprintf(login_node, "%s", val+1); }
		if (strcmp(var,"session")==0) { sprintf(session, "%s", val+1); }
		if (strcmp(var,"Exit_status")==0) { sprintf(exitstatus, "%s", val+1); }
		if (strcmp(var,"resources_used.cput")==0) { sprintf(RUcput, "%s", val+1); }
		if (strcmp(var,"resources_used.mem")==0) { sprintf(RUmem, "%s", val+1); }
		if (strcmp(var,"resources_used.vmem")==0) { sprintf(RUvmem, "%s", val+1); }
		if (strcmp(var,"resources_used.walltime")==0) { sprintf(RUwalltime, "%s", val+1); }

//printf("PAIR:%s | %s\n", var, val+1);

		}

		pair = strtok_r(NULL, " ", &psave1);
	}

	sprintf(sqlbuf, "");
	//printf("U/Q = '%s'-'%s'\n", user, queue);

	if (strcmp(rectype, "S") == 0) {
//printf("S\n");
			sprintf(sqlbuf, "UPDATE jobs set account='%s' WHERE jobid=%d", account, jobid);


		i = mysql_query(&mysql, sqlbuf);
		//printf ("Query: '%s'\n", sqlbuf);
	}


}
}	




//printf("Program complete\n");
return 0;


}
