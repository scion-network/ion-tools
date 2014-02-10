#!/usr/bin/perl

my %pains = ();
my %pains_context = ();
my $context = "";
my $last_key = "";
while (<STDIN>) {
   $line = $_;
   chomp();
   @columns = split(/ /);
   
   if ($columns[0] =~ /\d{4}-\d{2}-\d{2}/) {
      $key = $columns[4];
      if ($ARGV[0] =~ '-no-line-numbers') {
        $key =~ s/\:.*//;
      }
      $pains{$key} += 1;
      $pains_context{$last_key} .= $context . "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n";
      $context = "";
      $last_key = $key;
   }
   
   $context .= $line;
}
$pains_context{$last_key} .= $context . "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n";


#while (($key, $value) = each %pains) {
   #$inverted_pains{$value} .= $key;
#}

while (($key, $value) = each %pains) {
   print $value . "x for " .  $key . "\n";
   $bpm = int(length($pains_context{$key}) / $value);
   open(DETAIL, ">" . $key . "_occurs_" . $value . "_times_for_" . $bpm . "_bytes_per_message") || print "Can't open $value";
   print DETAIL $pains_context{$key};
   close(DETAIL)
}
