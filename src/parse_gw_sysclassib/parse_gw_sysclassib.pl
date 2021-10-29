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

my $numLines=4000;
#my @queryArray;
my $queryHeader="insert ignore into gw_sysclassib(cTime,cTime_usec,DT,DT_usec,ProducerName,CompId,jobid,mlx5_symbol_error_rate,mlx5_port_rcv_error_rate,mlx5_port_rcv_remote_physical_error_rate,mlx5_port_rcv_switch_relay_error_rate,mlx5_port_xmit_discards_rate,mlx5_port_xmit_constraint_error_rate,mlx5_port_rcv_constraint_error_rate,mlx5_local_link_integrity_error_rate,mlx5_VL15_dropped_rate,mlx5_port_xmit_data_rate,mlx5_port_rcv_data_rate,mlx5_port_xmit_packets_rate,mlx5_port_rcv_packets_rate,mlx5_port_unicast_xmit_packets_rate,mlx5_port_unicast_rcv_packets_rate,mlx5_port_multicast_xmit_packets_rate,mlx5_port_multicast_rcv_packets_rate) values ";

#my $filename=$ARGV[0];
my $lineCounter=0;
my $valCounter=0;
my $valCount=42;
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
my $f25;
my $f26;
my $f27;
my $f28;
my $f29;
my $f30;
my $f31;
my $f32;
my $f33;
my $f34;
my $f35;
my $f36;
my $f37;
my $f38;
my $f39;
my $f40;
my $f41;
my $f42;
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
	($f1,$f2,$f3,$f4,$f5,$f6,$f7,$f8,$f9,$f10,$f11,$f12,$f13,$f14,$f15,$f16,$f17,$f18,$f19,$f20,$f21,$f22,$f23,$f24,$f25,$f26,$f27,$f28,$f29,$f30,$f31,$f32,$f33,$f34,$f35,$f36,$f37,$f38,$f39,$f40,$f41,$f42) = split(',',$line,$valCount);
        $values.="(".floor($f1).',';
	$values.="$f2,$f3,$f4,\'$f5\',$f6,$f7,$f8,$f10,$f12,$f14,$f16,$f18,$f20,$f22,$f24,$f26,$f28,$f30,$f32,$f34,$f36,$f38,$f40";	
	$values.="),\n";
#	print "values=$values\n";
    }
    else
    {

	($f1,$f2,$f3,$f4,$f5,$f6,$f7,$f8,$f9,$f10,$f11,$f12,$f13,$f14,$f15,$f16,$f17,$f18,$f19,$f20,$f21,$f22,$f23,$f24,$f25,$f26,$f27,$f28,$f29,$f30,$f31,$f32,$f33,$f34,$f35,$f36,$f37,$f38,$f39,$f40,$f41,$f42,$f25,$f26,$f27,$f28,$f29,$f30,$f31,$f32,$f33,$f34,$f35,$f36,$f37,$f38,$f39,$f40,$f41,$f42) = split(',',$line,$valCount);
        $values.="(".floor($f1).',';
        $values.="$f2,$f3,$f4,\'$f5\',$f6,$f7,$f8,$f10,$f12,$f14,$f16,$f18,$f20,$f22,$f24,$f26,$f28,$f30,$f32,$f34,$f36,$f38,$f40";
	$values.=")";
#	push (@queryArray, $queryHeader.$values."\n");
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
#	push (@queryArray, $queryHeader.$values);
	 $thread_q -> enqueue ( $queryHeader.$values);
}
#close $FH or die "Cannot close $filename: $!";

$thread_q->end();

#Wait for threads to all finish processing.
foreach my $thr ( threads -> list() )
{
  $thr -> join();
}
