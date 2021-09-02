#include "mysql.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <math.h>


//#define MAXBUFSIZE 1049394 /* max number of bytes we can get at once */
#define MAXBUFSIZE 2048
#define MAXSAFESIZE 4097
#define MAXLINESIZE 4197576

#define JOBID 1
#define USER 96
#define GROUP 27
#define ACCOUNT 2
#define JOBNAME
#define CTIME 73
#define ELIGIBLE 23
#define START 71
#define END 24
#define STATE 72
#define NODELIST 51
#define     delim  "|"
#define EXPECTFIELDS 101

struct jobs {
    char jobid[128];
    char user[64];
    char group[32];
    char account[32];
    char jobname[256];
    char ctime[16];  
    char eligible[16]; 
    char start[16]; 
    char end[16];
    char state[256];
    char nodelist[16384];
};


int main(){
    MYSQL mysql;
    char * linein;
    char * linebuf;
    char HEADER[256][32];
    int nFieldCount=0;
    char sqlbuf[MAXBUFSIZE+256];
    char * token, *dashToken,*underscore;
    char * end;
    char * hostBegin,*hostEnd;
    int firstHost,lastHost;
    int index=0;
    int repeat=0;
    int i,j;
    int sqlReturn;
    struct jobs job;  
//for labeling fields
    int jobidIdx,userIdx,groupIdx,accountIdx,jobnameIdx,ctimeIdx,eligibleIdx,startIdx,endIdx,stateIdx,nodelistIdx;

    MYSQL_RES *res;
    MYSQL_ROW row;


    mysql_init(&mysql);
    if(!mysql_real_connect(&mysql,  "127.0.0.1", NULL, NULL, "ISC", 15306, NULL, 0)){
        printf("ERROR connecting to database\n\nExiting\n");
        exit(3);
    }
	linein=malloc(MAXLINESIZE);
	linebuf=malloc(MAXLINESIZE);

// check for header first
if (fgets(linein, MAXLINESIZE, stdin) != NULL)
{
	index=0;
	sprintf(linebuf, "%s", linein);
	token=strtok(linebuf,delim);
	if (token!=NULL)
	{
		if(!strcmp(token,"Cluster"))
		{
	//		printf("This is the header\n");
			strcpy(HEADER[0],token);
			index++;
			token = strtok(NULL,delim);
			while(token!=NULL)
                        {
          //                    printf("Index %d:%s\n",index,token);
/*			      switch(str2inti2(token))
				{
					case (str2int2("JobID"))

						jobidIdx=index;
						break;
                                        case str2int2("User"):
                                                userIdx=index;
                                                break;                                        
					case str2int2("Group"):
                                                groupIdx=index;
                                                break;                                        
					case str2int2("Account"):
                                                acountIdx=index;
                                                break;
                                        case str2int(2"JobName"):
                                                jobnameIdx=index;
                                                break;
                                        case str2int2("Submit"):
                                                ctimeIdx=index;
                                                break;
                                        case str2int2("Eligible"):
                                                eligibleIdx=index;
                                                break;
                                        case str2int2("Start"):
                                                startIdx=index;
                                                break;
                                        case str2int2("End"):
                                                endIdx=index;
                                                break;
                                        case str2int2("State"):
                                                stateIdx=index;
                                                break;
                                        case str2int2("NodeList"):
                                                nodelistIdx=index;
                                                break;

				}
*/
				        if(!strcmp(token,"JobID"))
                                                jobidIdx=index;
                                        if(!strcmp(token,"User"))
                                                userIdx=index;
                                        if(!strcmp(token,"Group"))
                                                groupIdx=index;
                                        if(!strcmp(token,"Account"))
                                                accountIdx=index;
                                        if(!strcmp(token,"JobName"))
                                                jobnameIdx=index;
                                        if(!strcmp(token,"Submit"))
                                                ctimeIdx=index;
                                        if(!strcmp(token,"Eligible"))
                                               eligibleIdx=index;
                                        if(!strcmp(token,"Start"))
                                                startIdx=index;
                                        if(!strcmp(token,"End"))
                                                endIdx=index;
                                        if(!strcmp(token,"State"))
                                                stateIdx=index;
                                        if(!strcmp(token,"NodeList"))
                                               nodelistIdx=index;
                              token = strtok(NULL,delim);
                              index+=1;
                        }	
			nFieldCount=index;
		}
		else
		{
			nFieldCount=EXPECTFIELDS;
		}
	}
}

//process the rest of the input as data
	while(fgets(linein, MAXLINESIZE, stdin) != NULL){
        //puts(linein);
		//job.jobid=0;
		sprintf(job.jobid,"");
		sprintf(job.user,"");
    		sprintf(job.group,"");
    		sprintf(job.account,"");
    		sprintf(job.jobname,"");
    		sprintf(job.ctime,"");
    		sprintf(job.eligible,"");
    		sprintf(job.start,"");
    		sprintf(job.end,"");
    		sprintf(job.state,"");
    		sprintf(job.nodelist,"");
		sprintf(linebuf, "%s", linein);
		index=0;
		token=end=linebuf;
        	if (linein != NULL){
        		if (strlen(linein)>0){	
				int print=1;	
				while ((token=strsep(&end,delim)) != NULL)
				{
					//printf("Index %d:%s\n",index,token);
					switch(index)
					{
						case JOBID:
						if(token)
						{
							//underscore=strchr(token,'_');
							//if(underscore)
							//{
							//	*underscore='.';
						//		printf("Job array %s\n",token);
							//}
							//job.jobid=strtod(token,NULL);
							sprintf(job.jobid,"%s",token);
						}
						break;
					
						case ACCOUNT :
                                                if(token)
                                                        sprintf(job.account,"%s",token);
                                                break;
                                                case END :
                                                if(token)
                                                        sprintf(job.end,"%s",token);
				//		if(print)
				//				printf("Index:%d End=%s\n",index,token);
                                                break;
                                                case GROUP :
                                                if(token)
                                                        sprintf(job.group,"%s",token);
                                                break;
                                                case NODELIST :
                                                if(token)
                                                        sprintf(job.nodelist,"%s",token);
                                                break;
						case START :
                                                if(token)
                                                        sprintf(job.start,"%s",token);
                                                break;
						case STATE :
                                                if(token)
                                                        sprintf(job.state,"%s",token);
                                                break;
						case CTIME :
                                                if(token)
                                                        sprintf(job.ctime,"%s",token);
                                                break;
                                                case USER :
                                                if(token)
                                                        sprintf(job.user,"%s",token);
						break;
					}
//					if (print)
//						printf("Index:%d data %s\n",index,token);
					index+=1;
				}
			} //end non zero lenght string
			if(index!=nFieldCount)
				printf( "line with different number of fields %d instead of %d\n",index,nFieldCount);
			else
			{

	//			printf("Job:%d account:%s user:%s start:%s end:%s status:%s\n\n",job.jobid,job.account,job.user,job.start,job.end,job.state);
				if(!strcmp(job.state,"PENDING"))
				{
				//	printf("Pending job %d %s %s %s %s %s %s\n",job.jobid,job.ctime,job.start,job.end,job.user,job.group,job.account);
 					sprintf(sqlbuf, "INSERT INTO jobs (jobid, status, ctime,user,account,user_group)  VALUES ('%s', 'Queued', '%s','%s','%s','%s') ON DUPLICATE KEY UPDATE status='Queued', ctime='%s'", job.jobid, job.ctime,job.user,job.account,job.group,job.ctime );
					 sqlReturn = mysql_query(&mysql, sqlbuf);
                			 if(sqlReturn) 
						{
							printf("Error is %s\n",mysql_error(&mysql));
							printf("sqlbuf=%s\n",sqlbuf);
						}
				}
				else if(!strcmp(job.state,"RUNNING"))
				{
				//	printf("%d,%s,%s,%s,%s,%s,%s\n", job.jobid, job.user, job.group, job.account, job.ctime, job.start);
					sprintf(sqlbuf, "INSERT INTO jobs(jobid, status, user, user_group, account,ctime, start) VALUES('%s', 'Running', '%s', '%s', '%s', '%s', '%s') ON DUPLICATE KEY UPDATE status='Running', user='%s', user_group='%s', account='%s',  ctime=%s, start=%s", job.jobid, job.user, job.group, job.account, job.ctime, job.start,job.user, job.group, job.account, job.ctime, job.start);
					//printf("sqlbuf %s\n",sqlbuf);
					sqlReturn = mysql_query(&mysql, sqlbuf);
                                        if(sqlReturn) printf("Error is %s\n",mysql_error(&mysql));
					//printf("Job Running:'%s'---nodelist:%s\n",job.jobid,job.nodelist);
					
					//update the host data
					hostBegin=strchr(job.nodelist,'[');
					

					sprintf(sqlbuf, "select nid from job_hosts where jobid='%s' limit 1",job.jobid);
					sqlReturn = mysql_query(&mysql, sqlbuf);
					res = mysql_store_result(&mysql);
  					row = mysql_fetch_row(res);
					if(row) // if there are entries in the job+_hosts table then dont readd them
						repeat=1;
					else 
						repeat=0;	
					

					if(hostBegin&&(!repeat))
					{
						hostBegin++;
						hostEnd = strchr(hostBegin,']');
						if(hostEnd)
							sprintf(hostEnd,NULL);
						else
							printf("no ending\n");
						token=strtok(hostBegin,",");
						if(!token)
						{
							//not a comma separated list
							printf("list is: %s\n",hostBegin);	
						}
						while(token != NULL)
						{
							dashToken=strchr(token,'-');
							if (!dashToken)
							{
								//not a range
							//	printf("Insert %d,%d\n",job.jobid,atoi(token));
								 sprintf(sqlbuf, "INSERT INTO job_hosts(jobid,nid) values('%s',%d)",job.jobid,atoi(token));
                                                                 sqlReturn = mysql_query(&mysql, sqlbuf);
                                                                 //if(sqlReturn) printf("Error is %s\n",mysql_error(&mysql));
							}	
							else
							{
								//node range
							//	printf("Range %s\n",token);
								lastHost=atoi(dashToken+1);
								*dashToken=0;	
								firstHost=atoi(token);
								//printf("first: %d last: %d\n",firstHost,lastHost);	
								for(i=firstHost;i<=lastHost;i++)
								{
									sprintf(sqlbuf, "INSERT INTO job_hosts(jobid,nid) values('%s',%d)",job.jobid,i); 	
									sqlReturn = mysql_query(&mysql, sqlbuf);
                                        			//	if(sqlReturn) printf("Error is %s\n",mysql_error(&mysql));
								}
							}
							token=strtok(NULL,",");
						}					
						
					}	
					else if(!repeat)
					{
						//single node. strip off the prefix		
						char *s=job.nodelist;
						if(!isdigit(*s))
						{

							while(*s && !isdigit(*++s))
							;
						}
						//printf("single node %s\n",s);
						sprintf(sqlbuf, "INSERT INTO job_hosts(jobid,nid) values('%s',%d)",job.jobid,atoi(s));
                                                sqlReturn = mysql_query(&mysql, sqlbuf);
                                                //if(sqlReturn) printf("Error is %s\n",mysql_error(&mysql));
					}

				}
				else if(!strcmp(job.state,"COMPLETED"))
				{
				//	printf("%d,%s,%s,%s,%s,%s,%s\n", job.jobid, job.user, job.group, job.account, job.ctime, job.start,job.end);
					sprintf(sqlbuf, "UPDATE jobs set status='Completed', user='%s', user_group='%s', account='%s', ctime='%s',  start='%s',`end`='%s'  WHERE jobid='%s'", job.user, job.group, job.account,  job.ctime,  job.start, job.end, job.jobid);	
					sqlReturn = mysql_query(&mysql, sqlbuf);
                                        if(sqlReturn) // Handle this better
					        {
                                                        printf("Error is %s\n",mysql_error(&mysql));
                                                        printf("sqlbuf=%s\n",sqlbuf);
							sprintf(sqlbuf, "INSERT INTO jobs(jobid, status, user, user_group, account,ctime, start,`end`) VALUES('%s', 'Completed', '%s', '%s', '%s', %s, %s, %s)" , job.jobid, job.user, job.group, job.account, job.ctime, job.start,job.end);
                                        		sqlReturn = mysql_query(&mysql, sqlbuf);
							if(sqlReturn) 
							 {
                                                        printf("Error is %s\n",mysql_error(&mysql));
                                                        printf("sqlbuf=%s\n",sqlbuf);
							}
                                                }	
				}
				else if(!strncmp(job.state,"CANCELLED",9))
				{
                                       // printf("Got CANCELLED:%s-%s %s\n",job.start,job.end,linein);
					sprintf(sqlbuf, "UPDATE jobs set status='Cancelled', user='%s', user_group='%s', account='%s', ctime='%s',  start='%s',`end`='%s'  WHERE jobid='%s'", job.user, job.group, job.account,  job.ctime,  job.start, job.end, job.jobid);
                                        sqlReturn = mysql_query(&mysql, sqlbuf);
                                        if(sqlReturn)  printf("Error is %s\n",mysql_error(&mysql));
				}
				else if(!strcmp(job.state,"TIMEOUT"))
                                {
                                        //printf("Got TIMEOUT:%s-%s %s\n",job.start,job.end,linein);
					sprintf(sqlbuf, "UPDATE jobs set status='Timeout', user='%s', user_group='%s', account='%s', ctime='%s',  start='%s',`end`='%s'  WHERE jobid='%s'", job.user, job.group, job.account,  job.ctime,  job.start, job.end, job.jobid);
                                        sqlReturn = mysql_query(&mysql, sqlbuf);
                                        if(sqlReturn)  printf("Error is %s\n",mysql_error(&mysql));
                                }
				else if(!strcmp(job.state,"FAILED"))
                                {
                                        //printf("Got FAILED:%s\n",linein);
					sprintf(sqlbuf, "UPDATE jobs set status='Failed', user='%s', user_group='%s', account='%s', ctime='%s',  start='%s',`end`='%s'  WHERE jobid='%s'", job.user, job.group, job.account,  job.ctime,  job.start, job.end, job.jobid);
                                        sqlReturn = mysql_query(&mysql, sqlbuf);
                                        if(sqlReturn)  printf("Error is %s\n",mysql_error(&mysql));
                                }
				else if(!strcmp(job.state,"OUT_OF_MEMORY"))
                                {
                                        //printf("Got FAILED:%s\n",linein);
                                        sprintf(sqlbuf, "UPDATE jobs set status='Out_of_Memory', user='%s', user_group='%s', account='%s', ctime='%s',  start='%s',`end`='%s'  WHERE jobid='%s'", job.user, job.group, job.account,  job.ctime,  job.start, job.end, job.jobid);
                                        sqlReturn = mysql_query(&mysql, sqlbuf);
                                        if(sqlReturn)  printf("Error is %s\n",mysql_error(&mysql));
                                }
				else if(!strcmp(job.state,"RESIZING"))
                                {
                                        //printf("Got FAILED:%s\n",linein);
                                        sprintf(sqlbuf, "UPDATE jobs set status='Resizing', user='%s', user_group='%s', account='%s', ctime='%s',  start='%s',`end`='%s'  WHERE jobid='%s'", job.user, job.group, job.account,  job.ctime,  job.start, job.end, job.jobid);
                                        sqlReturn = mysql_query(&mysql, sqlbuf);
                                        if(sqlReturn)  printf("Error is %s\n",mysql_error(&mysql));
                                }
			        else if(!strcmp(job.state,"NODE_FAIL"))
                                {
                                        //printf("Got FAILED:%s\n",linein);
                                        sprintf(sqlbuf, "UPDATE jobs set status='NodeFail', user='%s', user_group='%s', account='%s', ctime='%s',  start='%s',`end`='%s'  WHERE jobid='%s'", job.user, job.group, job.account,  job.ctime,  job.start, job.end, job.jobid);
                                        sqlReturn = mysql_query(&mysql, sqlbuf);
                                        if(sqlReturn)  printf("Error is %s\n",mysql_error(&mysql));
                                }

				else
				{
					printf("unhandled State=%s:%s\n",job.state,linein);
				}

			} //end good line else
		} //non-null line end
	} //end fgts loop

mysql_close(&mysql);
free(linein);
free(linebuf);
return(0);
}
