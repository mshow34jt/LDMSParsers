#!/usr/bin/perl

use strict;
use warnings;

use threads;

use Thread::Queue;
use DBI;
my $nthreads = 4;
my $thread_q = Thread::Queue -> new();
my $pending_t=0;

my $numLines=8000;
#my @queryArray;
my $queryHeader="insert into ovis_metrics(cTime,cTime_usec,DT,DT_usec,CompId,nettopo_mesh_coord_X,nettopo_mesh_coord_Y,nettopo_mesh_coord_Z,Xp_SAMPLE_GEMINI_LINK_BW,Xn_SAMPLE_GEMINI_LINK_BW,Yp_SAMPLE_GEMINI_LINK_BW,Yn_SAMPLE_GEMINI_LINK_BW,Zp_SAMPLE_GEMINI_LINK_BW,Zn_SAMPLE_GEMINI_LINK_BW,Xp_SAMPLE_GEMINI_LINK_USED_BW,Xn_SAMPLE_GEMINI_LINK_USED_BW,Yp_SAMPLE_GEMINI_LINK_USED_BW,Yn_SAMPLE_GEMINI_LINK_USED_BW,Zp_SAMPLE_GEMINI_LINK_USED_BW,Zn_SAMPLE_GEMINI_LINK_USED_BW,Xp_SAMPLE_GEMINI_LINK_PACKETSIZE_AVE,Xn_SAMPLE_GEMINI_LINK_PACKETSIZE_AVE,Yp_SAMPLE_GEMINI_LINK_PACKETSIZE_AVE,Yn_SAMPLE_GEMINI_LINK_PACKETSIZE_AVE,Zp_SAMPLE_GEMINI_LINK_PACKETSIZE_AVE,Zn_SAMPLE_GEMINI_LINK_PACKETSIZE_AVE,Xp_SAMPLE_GEMINI_LINK_INQ_STALL,Xn_SAMPLE_GEMINI_LINK_INQ_STALL,Yp_SAMPLE_GEMINI_LINK_INQ_STALL,Yn_SAMPLE_GEMINI_LINK_INQ_STALL,Zp_SAMPLE_GEMINI_LINK_INQ_STALL,Zn_SAMPLE_GEMINI_LINK_INQ_STALL,Xp_SAMPLE_GEMINI_LINK_CREDIT_STALL,Xn_SAMPLE_GEMINI_LINK_CREDIT_STALL,Yp_SAMPLE_GEMINI_LINK_CREDIT_STALL,Yn_SAMPLE_GEMINI_LINK_CREDIT_STALL,Zp_SAMPLE_GEMINI_LINK_CREDIT_STALL,Zn_SAMPLE_GEMINI_LINK_CREDIT_STALL,SAMPLE_totaloutput_optA,SAMPLE_totalinput,SAMPLE_fmaout,SAMPLE_bteout_optA,SAMPLE_bteout_optB,SAMPLE_totaloutput_optB,Rate_read_bytes_stats_snx11001,Rate_write_bytes_stats_snx11001,Rate_open_stats_snx11001,Rate_close_stats_snx11001,Rate_seek_stats_snx11001,Rate_read_bytes_stats_snx11002,Rate_write_bytes_stats_snx11002,Rate_open_stats_snx11002,Rate_close_stats_snx11002,Rate_seek_stats_snx11002,Rate_read_bytes_stats_snx11003,Rate_write_bytes_stats_snx11003,Rate_open_stats_snx11003,Rate_close_stats_snx11003,Rate_seek_stats_snx11003,loadavg_latest,loadavg_5min,loadavg_running_processes,loadavg_total_processes,current_freemem,Rate_SMSG_ntx,Rate_SMSG_tx_bytes,Rate_SMSG_nrx,Rate_SMSG_rx_bytes,Rate_RDMA_ntx,Rate_RDMA_tx_bytes,Rate_RDMA_nrx,Rate_RDMA_rx_bytes,Rate_ipogif0_rx_bytes,Rate_ipogif0_tx_bytes,Tesla_K20X_gpu_util_rate,Tesla_K20X_gpu_memory_used,Tesla_K20X_gpu_temp,Tesla_K20X_gpu_pstate,Tesla_K20X_gpu_power_limit,Tesla_K20X_gpu_power_usage,Flag) values ";

my $filename=$ARGV[0];
my $counter=0;
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


open (my $FH, '<', $filename) or die "Can't open '$filename' for read: $!";
while (my $line = <$FH>) {
    chomp $line;
    $counter++;
#    print "reading line $counter\n";
    if($counter<$numLines)
    {
	my ($ftime,$rest) = split(',',$line,2);
        $values.="(".floor($ftime).','.$rest."),\n";
    }
    else
    {
	my ($ftime,$rest) = split(',',$line,2);
        $values.="(".floor($ftime).','.$rest.")";
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
	$counter=0;
	$values="";
    }
}
if($counter>0)
{
	#remove the tailing comma and newline if it was not a complete set
	chop($values);
	chop($values);
#	push (@queryArray, $queryHeader.$values);
	 $thread_q -> enqueue ( $queryHeader.$values);
}
close $FH or die "Cannot close $filename: $!";

$thread_q->end();

#Wait for threads to all finish processing.
foreach my $thr ( threads -> list() )
{
  $thr -> join();
}
