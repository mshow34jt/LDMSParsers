#include "mysql.h"

#include <stdio.h>
#include <stdlib.h>
//#include <errno.h>
#include <string.h>
//#include <unistd.h>
//#include <math.h>


#define MAXBUFSIZE 1049394
#define MAXLINESIZE 8395152 /* max number of bytes we can get at once */

int main(){
    MYSQL * mysql;
    int i;

    int year, month, day, hour, min, sec;
    int jobid;
    char *rectype;
    char *data;
    char *pair;
    char *val;
    char var[256];
    char linein[MAXLINESIZE];
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


    mysql=mysql_init(NULL);
/*     if(!mysql_real_connect(mysql, "localhost", "isc", "clumon", "isc", 0, NULL, 0)){
        printf("ERROR connecting to databasennExiting\n");
        exit(3);
    }
    else
        printf("Connected to database\n");
*/
    mysql_close(mysql);
    return 0;


}
