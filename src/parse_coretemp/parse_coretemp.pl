#!/usr/bin/perl

use strict;
use warnings;
use POSIX;
use threads;
use List::Util qw(max);
use List::Util qw(sum);

use Thread::Queue;
use DBI;
my $nthreads = 4;
my $thread_q = Thread::Queue -> new();
my $pending_t=0;

my $numLines=4000;
#my @queryArray;
my $queryHeader="insert ignore into coretemp(cTime,cTime_usec,ProducerName,CompId,jobid,apid,maxTemp,avgTemp) values ";

my $lineCounter=0;
my $valCounter=0;
my $valCount=12;
my @fields;
my @temps;
my $maxTemp;
my $avgTemp;
my $values;
my $dsn= "DBI:mysql:ISC:host=127.0.0.1:port=15306";
	



sub worker
{
  #NB - this will sit a loop indefinitely, until you close the queue.
  #  #using $process_q -> end
  #    #we do this once we've queued all the things we want to process
  #      #and the sub completes and exits neatly.
  #        #however if you _don't_ end it, this will sit waiting forever.
            while ( my $query = $thread_q -> dequeue() )
              {
		  my $dbcon = DBI->connect($dsn)||
    			print STDERR "FATAL: Could not connect to database.\n$DBI::errstr\n";
                  chomp ( $query );
#		  print $query;
                  #print threads -> self() -> tid(). ": thread running\n";
                  $dbcon->do($query);
                  #print threads -> self() -> tid(). ":  done \n";
		  $dbcon->disconnect();
              }
}


#$thread_q -> limit = $nthreads;
for ( 1..$nthreads )
{
  threads -> create ( \&worker );
}


#open (my $FH, '<', $filename) or die "Can't open '$filename' for read: $!";
while (my $line = <STDIN>) {
    chomp $line;
    $lineCounter++;
#    print "reading line $lineCounter\n";
    if($lineCounter<$numLines)
    {
	@fields = split(',',$line);
        @temps=($fields[10],$fields[17],$fields[24],$fields[31],$fields[38],$fields[45],$fields[52],$fields[59],$fields[66],$fields[73],$fields[80],$fields[87],$fields[94],$fields[101],$fields[108],$fields[115],$fields[122],$fields[129],$fields[136],$fields[143],$fields[150],$fields[157],$fields[164],$fields[171],$fields[178],$fields[185],$fields[192],$fields[199],$fields[206],$fields[213],$fields[220],$fields[227],$fields[234],$fields[241]);
        $avgTemp=(sum(@temps))/34;
        $maxTemp=max(@temps);
#        print("The max is ".$maxTemp."\n");
#        print("The avg is ".$avgTemp."\n");
        $values.="(".floor($fields[0]).',';
	$values.="$fields[1],\'$fields[2]\',$fields[3],$fields[4],$fields[5],$maxTemp,$avgTemp";	
	$values.="),\n";
#	print "values=$values\n";
    }
    else
    {

	@fields = split(',',$line);
        @temps=($fields[10],$fields[17],$fields[24],$fields[31],$fields[38],$fields[45],$fields[52],$fields[59],$fields[66],$fields[73],$fields[80],$fields[87],$fields[94],$fields[101],$fields[108],$fields[115],$fields[122],$fields[129],$fields[136],$fields[143],$fields[150],$fields[157],$fields[164],$fields[171],$fields[178],$fields[185],$fields[192],$fields[199],$fields[206],$fields[213],$fields[220],$fields[227],$fields[234],$fields[241]);
        $avgTemp=(sum(@temps))/34;
        $maxTemp=max(@temps);
        print("The max is ".$maxTemp."\n");
        print("The avg is ".$avgTemp."\n");
        $values.="(".floor($fields[0]).',';
        $values.="$fields[1],\'$fields[2]\',$fields[3],$fields[4],$fields[5],$maxTemp,$avgTemp";
        $values.=")\n";
	
	if($thread_q->pending())
	{
		while($thread_q->pending() == $nthreads)
		{
#			print "waiting";
			sleep(1);
			$pending_t= $thread_q->pending();
		}
	}
	$thread_q -> enqueue ( $queryHeader.$values);
	$lineCounter=0;
	$valCounter=0;
	$values="";
    }
}
if($lineCounter>0)
{
	#remove the tailing comma and newline if it was not a complete set
	chop($values);
	chop($values);
	 $thread_q -> enqueue ( $queryHeader.$values);
}
#close $FH or die "Cannot close $filename: $!";

$thread_q->end();

#Wait for threads to all finish processing.
foreach my $thr ( threads -> list() )
{
  $thr -> join();
}
