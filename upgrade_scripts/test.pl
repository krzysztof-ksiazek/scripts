#!/usr/bin/perl -w

use DBI;
use Time::HiRes qw/ time sleep /;

sub RunQuery {

    $dbh = $_[0];
    $line = $_[1];
    $loops = $_[2];
    $sth = $dbh->prepare($line);
    my $start = time;
    for ( $n = 0; $n < $loops ; $n++) {
        $sth->execute or die "SQL Error: $DBI::errstr\n";
    }
    my $end   = time;
    $time = ( $end - $start ) / $loops;
    return $time;

}


$dbh55 = DBI->connect('dbi:mysql:database:host','user','pass') or die "Connection Error: $DBI::errstr\n";
#$dbh51 = DBI->connect('dbi:mysql:database:host','user','pass') or die "Connection Error: $DBI::errstr\n";

open FILE, "/root/test-dbpm.sql" or die $!;
my $loops = 1000;
while (my $line = <FILE>) {
    if ($line =~ /^SELECT/)
    {
        print $line;
        $timesq = RunQuery($dbh55, $line, 1);
#        print $timesq."\n";
        if ($loops * $timesq > 1) {$loops = int(600 / $timesq);}
        if ($loops > 1000) {$loops = 1000;}
        if ($loops < 1) {$loops = 1;}
#        print $loops."\n";
        $time = RunQuery($dbh55, $line, $loops);
        printf('%s%d%s%f%s', 'Average execution time of ', $loops, ' executions on 5.5: ', $time , "\n");
    }
}
