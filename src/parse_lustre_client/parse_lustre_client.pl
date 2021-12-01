#!/usr/bin/perl
use strict;
use warnings;
use POSIX;
use threads;

use Thread::Queue;
use DBI;
my $nthreads = 4;
my $thread_q = Thread::Queue -> new();
my $pending_t=0;

my $numLines=2000;
#my @queryArray;
my $queryHeader="insert ignore into lustre_client(cTime,cTime_usec,DT,DT_usec,ProducerName,CompId,jobid,fs_name,read_bytes_rate,write_bytes_rate,open_rate,close_rate,seek_rate,getattr_rate,create_rate) values ";

#my $filename=$ARGV[0];
my $lineCounter=0;
my $valCounter=0;
my $valCount=24;
my $f1;
my $f2;
my $f3;
my $f4;
my $f5;
my $f6;
my $f7;
my $f8;
my $f9;
my $f10;
my $f11;
my $f12;
my $f13;
my $f14;
my $f15;
my $f16;
my $f17;
my $f18;
my $f19;
my $f20;
my $f21;
my $f22;
my $f23;
my $f24;
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
#                 print $query;
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
        ($f1,$f2,$f3,$f4,$f5,$f6,$f7,$f8,$f9,$f10,$f11,$f12,$f13,$f14,$f15,$f16,$f17,$f18,$f19,$f20,$f21,$f22,$f23,$f24) = split(',',$line,$valCount);
        $values.="(".floor($f1).',';
        $values.="$f2,$f3,$f4,\'$f5\',$f6,$f7,\'$f8\',$f10,$f12,$f14,$f16,$f18,$f20,$f22";
        $values.="),\n";
#       print "values=$values\n";
    }
    else
    {

        ($f1,$f2,$f3,$f4,$f5,$f6,$f7,$f8,$f9,$f10,$f11,$f12,$f13,$f14,$f15,$f16,$f17,$f18,$f19,$f20,$f21,$f22,$f23,$f24) = split(',',$line,$valCount);
        $values.="(".floor($f1).',';
        $values.="$f2,$f3,$f4,\'$f5\',$f6,$f7,\'$f8\',$f10,$f12,$f14,$f16,$f18,$f20,$f22";
        $values.=")";
#       push (@queryArray, $queryHeader.$values."\n");
        if($thread_q->pending())
        {
                while($thread_q->pending() == $nthreads)
                {
#                       print "waiting";
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
#       push (@queryArray, $queryHeader.$values);
         $thread_q -> enqueue ( $queryHeader.$values);
}
#close $FH or die "Cannot close $filename: $!";

$thread_q->end();

#Wait for threads to all finish processing.
foreach my $thr ( threads -> list() )
{
  $thr -> join();
}
