#!/usr/bin/perl -w

open FILE55, "/root/55.result" or die $!;
open FILE51, "/root/51.result" or die $!;

chomp(my @result55 = <FILE55>);
chomp(my @result51 = <FILE51>);

close FILE55;
close FILE51;


if (scalar(@result55) != scalar(@result51))
{
    print "Result files don't contain the same list of queries\n";
    die;
}

my $etime55;
my $etime51;
my $size  = scalar(@result55)."\n";

print "difference in execution time (5.5 - 5.1), execution time ratio (5.5 / 5.1) - close to 1 - similar, the larger, the 5.5 slower\n\n\n";
for (my $i = 0; $i < $size ; $i++)
{
    if ( ( ($result55[$i] =~ /^SELECT/ ) && ($result51[$i] =~ /^SELECT/ ) ) && ($result55[$i] eq $result51[$i]) )
    {
#        print $result55[$i];
        if ( $result55[$i+1] =~m/executions on 5.5: (\d+\.\d+)$/ )
        {
            $etime55 =  $1;

        }
        if ( $result51[$i+1] =~m/executions on 5.1: (\d+\.\d+)$/ )
        {
            $etime51 = $1;
        }
#        print $result55[$i]." : ".$etime55 - $etime51."\n";
        printf('%f %f : %s%s', $etime55 - $etime51, $etime55 / $etime51, $result55[$i],"\n");
    }
}
