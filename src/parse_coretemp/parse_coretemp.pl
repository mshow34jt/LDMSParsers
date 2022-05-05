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
	@temps=($fields[9],$fields[16],$fields[23],$fields[30],$fields[37],$fields[44],$fields[51],$fields[58],$fields[65],$fields[72],$fields[79],$fields[86],$fields[93],$fields[100],$fields[107],$fields[114],$fields[121],$fields[128],$fields[135],$fields[142],$fields[149],$fields[156],$fields[163],$fields[170],$fields[177],$fields[184],$fields[191],$fields[198],$fields[205],$fields[212],$fields[219],$fields[226],$fields[233],$fields[240]);
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
	@temps=($fields[9],$fields[16],$fields[23],$fields[30],$fields[37],$fields[44],$fields[51],$fields[58],$fields[65],$fields[72],$fields[79],$fields[86],$fields[93],$fields[100],$fields[107],$fields[114],$fields[121],$fields[128],$fields[135],$fields[142],$fields[149],$fields[156],$fields[163],$fields[170],$fields[177],$fields[184],$fields[191],$fields[198],$fields[205],$fields[212],$fields[219],$fields[226],$fields[233],$fields[240]);
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
