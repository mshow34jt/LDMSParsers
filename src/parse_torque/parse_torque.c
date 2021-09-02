/*
// torque parser
*/

#include "mysql.h"
#include <sys/resource.h>
#include <stdio.h>
#include <stdlib.h>
//#include <errno.h>
#include <string.h>
//#include <unistd.h>
//#include <math.h>


#define MAXBUFSIZE 1049394 
#define MAXLINESIZE 8395152 /* max number of bytes we can get at once */

int main(){
    MYSQL mysql;
    int i;

    int year, month, day, hour, min, sec;
    int jobid;
    char *rectype;
    char *data;
    char *pair;
    char *val;
    char var[256];
//    char * linein;
//    char * sqlbuf;
    char linein[MAXBUFSIZE];
    char sqlbuf[MAXBUFSIZE+256];
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
    char curhost[8];
    char lasthost[8];
    int  hostprocs;
    int  jobnodes;
    char *phost;
    char *pproc;
    char *psave1;
    char *psave2;
 
//    linein=malloc(MAXLINESIZE);	
//    sqlbuf=malloc(MAXBUFSIZE+256);
    mysql_init(&mysql);
    if(!mysql_real_connect(&mysql, "127.0.0.1", NULL, NULL, "ISC", 15306, NULL, 0)){
        printf("ERROR connecting to database\n\nExiting\n");
        exit(3);
    }
    else 
	printf("Connected to database\n");
   
while(fgets(linein, sizeof linein, stdin) != NULL){
        //puts(linein);

        if (linein != NULL){
        if (strlen(linein)>0){
printf("RECV: ===%s===\n", linein);
        
      	month = atoi(strtok(linein,"/")); 
	day = atoi(strtok(NULL, "/"));
	year = atoi(strtok(NULL, " "));        
	hour = atoi(strtok(NULL, ":"));
	min = atoi(strtok(NULL, ":"));
	sec = atoi(strtok(NULL, ";"));
	rectype = strtok(NULL, ";");
	jobid = atoi(strtok(NULL, ".["));
	strtok(NULL, ";");
	data = strtok(NULL, "\n");	

	}

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


	pair = strtok_r(data, " ", &psave1);
	while (pair !=NULL) {
//		printf("Pair: %s\n", pair);
		val = strpbrk (pair, "=");
		if (val != NULL){		
		strncpy(var, pair, val-pair);
		var[val-pair] = '\0';

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


		if (strcmp(var,"exec_host")==0 && strcmp(rectype, "S")==0) {
			//printf("JOBID=%d\nEXECHOSTS=%s\n", jobid, val+1);
			jobnodes=0;
			hostprocs=0;
			phost = strtok_r(val+1, "/ ", &psave2);
			//sprintf(curhost,"%s", phost);
			sprintf(lasthost,"", phost);
			sprintf(sqlbuf, "INSERT INTO job_hosts (jobid,nid) VALUES(%d, %s)", jobid, phost);
			i = mysql_query(&mysql, sqlbuf);
			if(i) printf("Error is %s\nQuery was:%s",mysql_error(&mysql),sqlbuf);
			do{
				
				//printf("Host:%s\n", phost);
				sprintf(curhost, "%s", phost);
				if (strcmp(phost,lasthost)!=0){
					//printf("Host:%s\n", phost);
					sprintf(lasthost, "%s", phost);
					jobnodes = jobnodes + 1;
					sprintf(sqlbuf, "INSERT INTO job_hosts (jobid,nid) VALUES(%d, %s)", jobid, phost);
					i = mysql_query(&mysql, sqlbuf);
					if(i) printf("Error is %s\n",mysql_error(&mysql));
				}
				pproc = strtok_r(NULL,"+ ", &psave2);
				hostprocs = hostprocs + 1;
			}while(phost = strtok_r(NULL,"/ ", &psave2));
			
			sprintf(lasthost, "");
			//printf("HOSTS=%d\nPROCS=%d\n", jobnodes, hostprocs);
			sprintf(sqlbuf, "UPDATE jobs SET node_count=%d WHERE jobid=%d", jobnodes, jobid);
			i = mysql_query(&mysql, sqlbuf);
			if(i) printf("Error is %s\n",mysql_error(&mysql));

		}


		if (strlen(RLnodect) == 0) {sprintf(RLnodect, "-1"); }
		}
		pair = strtok_r(NULL, " ", &psave1);
	}

	sprintf(sqlbuf, "");

	if (strcmp(rectype, "Q") == 0) {
		sprintf(sqlbuf, "INSERT INTO jobs (jobid, status, queue)  VALUES (%d, 'Queued', '%s') ON DUPLICATE KEY UPDATE status='Queued', queue='%s'", jobid, queue, queue); 
		i = mysql_query(&mysql, sqlbuf);
		if(i) printf("Error is %s\n",mysql_error(&mysql));
	}

	if (strcmp(rectype, "R") == 0) {
		sprintf(sqlbuf, "UPDATE jobs SET status='Queued' WHERE jobid=%d", jobid);
                i = mysql_query(&mysql, sqlbuf);
		if(i) printf("Error is %s\n",mysql_error(&mysql));
	}

	if (strcmp(rectype, "S") == 0) {
		sprintf(sqlbuf, "INSERT INTO jobs(jobid, status, user, user_group, account, jobname, queue, ctime, qtime, etime, start, owner, RLneednodes, RLnodect, RLnodes, RLwalltime, RLminwclimit, RLflags, login_node, tc) VALUES(%d, 'Running', '%s', '%s', '%s', '%s', '%s', %s, %s, %s, %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', 0) ON DUPLICATE KEY UPDATE status='Running', user='%s', user_group='%s', account='%s', jobname='%s', queue='%s', ctime=%s, qtime=%s, etime=%s, start=%s, owner='%s', RLneednodes='%s', RLnodect='%s', RLnodes='%s', RLwalltime='%s', RLminwclimit='%s', RLflags='%s', login_node='%s'", jobid, user, group, account, jobname, queue, ctime, qtime, etime, start, owner, RLneednodes, RLnodect, RLnodes, RLwalltime, RLminwclimit, RLflags, login_node, user, group, account, jobname, queue, ctime, qtime, etime, start, owner, RLneednodes, RLnodect, RLnodes, RLwalltime, RLminwclimit, RLflags, login_node);

		i = mysql_query(&mysql, sqlbuf);
		if(i) printf("Error is %s\n",mysql_error(&mysql));
		//printf ("Query: '%s'\n", sqlbuf);
	}
	
	if (strcmp(rectype, "E") == 0) {
                sprintf(sqlbuf, "UPDATE jobs set status='Completed', user='%s', user_group='%s', account='%s', jobname='%s', queue='%s', ctime=%s, qtime=%s, etime=%s, start=%s, owner='%s', RLneednodes='%s', RLnodect='%s', RLnodes='%s', RLwalltime='%s', session=%s, `end`=%s, exit_status=%s, RUcput='%s', RUmem='%s', RUvmem='%s', RUwalltime='%s', RLminwclimit='%s', RLflags='%s', login_node='%s' WHERE jobid=%d", user, group, account, jobname, queue, ctime, qtime, etime, start, owner, RLneednodes, RLnodect, RLnodes, RLwalltime, session, end, exitstatus, RUcput, RUmem, RUvmem, RUwalltime, RLminwclimit, RLflags, login_node, jobid);
                i = mysql_query(&mysql, sqlbuf);
		if(i) printf("Error is %s\n",mysql_error(&mysql));
                printf ("Query: '%s'\n", sqlbuf);
        }

	if (strcmp(rectype, "A") == 0) {
                sprintf(sqlbuf, "UPDATE jobs set status='Aborted' WHERE jobid=%d", jobid);
                i = mysql_query(&mysql, sqlbuf);
		if(i) printf("Error is %s\n",mysql_error(&mysql));
//              printf ("Query: '%s'\n", sqlbuf);
	}

	if (strcmp(rectype, "D") == 0) {
                sprintf(sqlbuf, "UPDATE jobs set status='Deleted' WHERE jobid=%d", jobid);
                i = mysql_query(&mysql, sqlbuf);
		if(i) printf("Error is %s\n",mysql_error(&mysql));
//              printf ("Query: '%s'\n", sqlbuf);
	}

        if (strcmp(rectype, "H") == 0) {
                sprintf(sqlbuf, "UPDATE jobs set status='On Hold' WHERE jobid=%d", jobid);
                i = mysql_query(&mysql, sqlbuf);
		if(i) printf("Error is %s\n",mysql_error(&mysql));
//              printf ("Query: '%s'\n", sqlbuf);
	}

//	sprintf(sqlbuf, "UPDATE isc_feeds_last_data SET last_data=UNIX_TIMESTAMP() WHERE feed='torque'");
//	i = mysql_query(&mysql, sqlbuf);
}
}	


//printf("Program complete\n");
mysql_close(&mysql);
//if(linein) free(linein);
//if(sqlbuf) free(sqlbuf);
return 0;
}

