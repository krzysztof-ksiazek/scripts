#!/usr/bin/perl -w

open FILE, "/root/rt51433-dbpm-report.out" or die $!;
open(my $fh, ">", "test-dbpm.sql") or die $!;


my $parse=0;
my $query_parse=0;
my @query_arr=();
my @query_tmp=();

while (my $line = <FILE>) {

    if ($line =~ /## Query time diffs/) {$parse = 1;}
    if ($parse == 1)
    {
#        print $line."\n";
#        print $query_parse." ".$parse."\n";
        if ($line =~ /^SELECT/) {$query_parse  = 1; @query_tmp = ();}
        if ($line =~ /^select/) {$query_parse  = 1; @query_tmp = ();}
        if ($line =~ /-- [0-9]./ ) {$str = "@query_tmp"; $str =~ s/\n//g; push (@query, $str); $query_parse = 0;}
        if ($line =~ /#{2,}.*/ ) {$str = "@query_tmp"; $str =~ s/\n//g; push (@query, $str); $query_parse=0; }

#        print $line."\n";
#        print $query_parse." ".$parse."\n";
        if ($query_parse == 1) { push(@query_tmp, $line);}

    }
    if ($line =~ /Query class/)
    {
        $parse = 0;
    }

}


my %seen;
my @unique = grep { ! $seen{$_}++ } @query;

#print "@unique";
foreach $query (@unique)
{
    $query =~ s/\r//g;
    print $fh $query."\n";
}

close $fh;
