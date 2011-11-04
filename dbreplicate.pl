#!/usr/bin/perl -w

#  $Id: dbreplicate,v 1.9 2001/12/05 15:23:45 kirk Exp $

#  This program was written by Kirk Strauser <kirk@strauser.com> of
#  NMotion, Inc., a website design firm in Springfield, MO, USA.  It
#  was released under the terms of the GPL by permission of Don
#  Hunsaker, the owner of NMotion, Inc.

#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

#### To-do:
#
# Handle generators with an increment value other than 1

use strict;
use DBI;
use FileHandle;
use Getopt::Long;


######################################################################
## Variable declaration and presets                                 ##
######################################################################

my $isql_syscmd;
my $tmp_file;
my $sqlout_eol;
my $db;
my $inline;
my @tables;
my $table;
my $infile = new FileHandle;
my $j;
my $fetchAttr;
my @bool = ('no', 'yes');
my %dbh;
my %opt;
my %opt_def;
my %optctl;
my %server;
my $key;
my %triggers;
my %triggermap;
my $trigger;
my %pg_trans;

######################################################################
## All user-configurable variables are in this section              ##
######################################################################

$isql_syscmd = '/usr/interbase/bin/isql';
$tmp_file    = '/tmp/isql.out';
$sqlout_eol  = ";\n";

$fetchAttr = {
    ib_timestampformat => '%m-%d-%Y %T',
    ib_dateformat => '%m-%d-%Y',
    ib_timeformat => '%H:%M',
};

%opt_def =
    (
     'debug'     => 1,
     'drop'      => 0,
     'create'    => 1,
     'copy'      => 1,
     'help'      => 0,
     'sqlout'    => 1,
     'pgconv'    => 1
     );

%pg_trans =
    (
     'DOUBLE'    => 'DOUBLE PRECISION'
     );

######################################################################
## Process runtime arguments                                        ##
######################################################################

# Preset options to the defaults
%opt = %opt_def;

# Create the option linkage
foreach $key (keys %opt)
{
    $optctl{$key} = \$opt{$key};
}

## Process the options, and print a usage page if the user didn't
## specify correct options or if they explicitly request help
if (not GetOptions(
		   \%optctl,
		   'debug!',
		   'drop!',
		   'create!',
		   'copy!',
		   'sqlout!',
		   'pgconv!',
		   'help'
		   )
    or $opt{'help'}
    or ($opt{'sqlout'} and @ARGV != 1)
    or (not $opt{'sqlout'} and @ARGV != 2)
    )
{
    %opt = %opt_def;
    print <<__END_OF_USAGE__;
Usage: $0 srcusr:srcpass\@srchost:srcdb [dstusr:dstpass\@dsthost:dstdb]

 Optional flags:

  --debug   Print debugging information to STDERR
            (Default: $bool[$opt{'debug'}])
  --drop    Drop all tables in srcdb from dstdb before processing.  Implies 'create'
            (Default: $bool[$opt{'drop'}])
  --create  Create all tables in dstb before processing.
            (Default: $bool[$opt{'create'}])
  --copy    Copy all data in all tables from srcdb to dstdb
            (Default: $bool[$opt{'copy'}])
  --sqlout  Write the SQL create and insert statements to STDOUT *instead* of
               executing them
            (Default: $bool[$opt{'sqlout'}])
  --pgconv  Convert as much data and metadata as possible to a format
               compatible with PostgreSQL
            (Default: $bool[$opt{'pgconv'}])
  --help    Get this information

__END_OF_USAGE__
    exit;
}

## Create the server definitions from the remaining arguments
foreach $db ('src', 'dst')
{
    next if ($db eq 'dst') and $opt{'sqlout'};
    $key = shift @ARGV;
    # user:password@hostname:dbname
    my ($hostname, $dbname, $username, $password);
    my ($up, $hd) = (split /\@/, $key)[0, 1];
    ($username, $password) = (split /:/, $up)[0, 1];
    ($hostname, $dbname) = (split /:/, $hd)[0, 1];
    $server{$db} = {
	'hostname' => $hostname,
	'dbname'   => $dbname,
	'username' => $username,
	'password' => $password
	};
}

## Display selected runtime options before executing the rest of the
## program
print STDERR <<__EOOPTIONS__;
Source database:
  Hostname : $server{'src'}{'hostname'}
  DBName   : $server{'src'}{'dbname'}
  User     : $server{'src'}{'username'}
  Password : $server{'src'}{'password'}

__EOOPTIONS__

if (not $opt{'sqlout'})
{
    print STDERR <<__EOOPTIONS__;
Destination database:
  Hostname : $server{'dst'}{'hostname'}
  DBName   : $server{'dst'}{'dbname'}
  User     : $server{'dst'}{'username'}
  Password : $server{'dst'}{'password'}

__EOOPTIONS__
}

print STDERR <<__EOOPTIONS__;
Options:
  Create   : $bool[$opt{'create'}]
  Drop     : $bool[$opt{'drop'}]
  Copy     : $bool[$opt{'copy'}]

__EOOPTIONS__

# Build all database connections
foreach $db (keys %server)
{
    debug("Connecting to $db");
    $dbh{$db} = DBI->connect("dbi:InterBase:$server{$db}{'dbname'};host=$server{$db}{'hostname'};ib_dialect=3",
			     $server{$db}{'username'},
			     $server{$db}{'password'});
}

######################################################################
## Fetch trigger and generator data                                 ##
######################################################################

$infile = sendIsqlCmd("show triggers");
$inline = <$infile>;
unless ($inline =~ /^There are no triggers in this database/)
{
    $inline = <$infile>;
    while ($inline = <$infile>)
    {
	last if $inline =~ /^SQL>/;
	chomp $inline;
	my ($table, $tname) = (split /\s+/, $inline)[0, 1];
	$tname =~ tr/a-z/A-Z/;
	$triggers{$tname}{'table'} = $table;
    }
}
close $infile;

foreach $trigger (keys %triggers)
{
    debug("Getting trigger $trigger");
    $infile = sendIsqlCmd("show trigger $trigger");
    while ($inline = <$infile>)
    {
	chomp $inline;
	next unless $inline =~ /=/;
	# $inline =~ s/^\s*//;
	# $inline =~ s/;$//;
	my ($tfield, $genstring) = (split /\s*=\s*/, $inline)[0, 1];
	$tfield = (split /\./, $tfield)[1];
	$tfield =~ tr/a-z/A-Z/;
	$genstring =~ s/^.*\((.*)\).*$/$1/;
	my ($generator, $increment) = (split /\s*,\s*/, $genstring)[0, 1];
	$triggers{$trigger}{'field'} = $tfield;
	$triggers{$trigger}{'generator'} = $generator;
	$triggers{$trigger}{'increment'} = $increment;
	$triggermap{$triggers{$trigger}{'table'}}{$tfield} = 1;
	last;
    }
    close $infile;
}

foreach $trigger (keys %triggers)
{
    debug("Getting generator $triggers{$trigger}{'generator'}");
    $infile = sendIsqlCmd("show generator $triggers{$trigger}{'generator'}");
    $inline = <$infile>;
    close $infile;
    chomp $inline;
    $inline =~ s/^.*\s+//;
    $triggers{$trigger}{'curval'} = $inline;
}

foreach $trigger (keys %triggers)
{
    print STDERR <<__EOTRIGGER__;
Trigger: $trigger
    Table    : $triggers{$trigger}{'table'}
    Field    : $triggers{$trigger}{'field'}
    Generator: $triggers{$trigger}{'generator'}
    Increment: $triggers{$trigger}{'increment'}
    Current  : $triggers{$trigger}{'curval'}
__EOTRIGGER__
}


######################################################################
## Get database metadata                                            ##
######################################################################

## Get the list of tables in the database
$infile = sendIsqlCmd("show tables");
$j = 0;
while ($inline = <$infile>)
{
    $inline = trimIsql($inline);
    my $table;
    foreach $table (split /\s+/, $inline)
    {
	next if $table eq '';
	push @tables, { 'name' => $table };
    }
    # last if $j++ == 4;
}
close $infile;

## For each table in the database, get the exact definition
for ($j = 0; $j < @tables; $j++)
{
    print STDERR "\n$tables[$j]{'name'}\n";
    print STDERR "====================\n";
    $infile = sendIsqlCmd("show table $tables[$j]{'name'}");
    my $state = 0;
    my @coldef;
    my @constraints;
    my $constbuf;
    while ($inline = <$infile>)
    {
	$inline = trimIsql($inline);
	last if $inline eq '';
	print STDERR ":: $inline\n";
	for($state)
	{
	    # A column definition or the start of a constraint block
	    /0/ and do {
		if ($inline =~ /^CONSTRAINT\s/)
		{
		    $state = 1;
		    $inline =~ s/:$//;
		    $constbuf = "$inline ";
		    last;
		}
		$inline =~ s/Nullable//;
		$inline =~ s/\s+/ /g;
		$inline =~ s/^\s+//;
		$inline =~ s/\s+$//;
		my ($fieldname, $fieldtype) = (split /\s+/, $inline)[0, 1];
		#### Add field conversion processing here
		if ($opt{'pgconv'})
		{
		    if (defined ($triggermap{$tables[$j]{'name'}}{$fieldname}))
		    {
			$fieldtype = 'SERIAL';
		    }
		    else
		    {
			if (defined ($pg_trans{$fieldtype}))
			{
			    $fieldtype = $pg_trans{$fieldtype};
			}
		    }
		}
		push @coldef, "$fieldname $fieldtype";
		last; };
	    # Inside a constraint block
	    /1/ and do {
		$constbuf .= $inline;
		push @constraints, $constbuf;
		debug("constbuf: $constbuf");
		$state = 0;
		last; };
	}
    }
    close $infile;
    my $createString = "CREATE TABLE $tables[$j]{'name'} (" . join(',', @coldef, @constraints) . ")";
    debug($createString);
    $tables[$j]{'def'} = $createString;
}


######################################################################
## Copy table metadata                                              ##
######################################################################

foreach $table (@tables)
{
    if ($opt{'drop'})
    {
	if ($opt{'sqlout'})
	{
	    print "drop table $$table{'name'}" . $sqlout_eol;
	}
	else
	{
	    print STDERR "Dropping table $$table{'name'}...";
	    print STDERR $dbh{'dst'}->do("drop table $$table{'name'}") ? "Success" : "FAILURE";
	    print STDERR "\n";
	}
    }
    if ($opt{'create'})
    {
	if ($opt{'sqlout'})
	{
	    print $$table{'def'} . $sqlout_eol;
	}
	else
	{
	    print STDERR "Creating table $$table{'name'}...";
	    print STDERR $dbh{'dst'}->do($$table{'def'}) ? "Success" : "FAILURE";
	    print STDERR "\n";
	}
    }
    print STDERR "\n" if $opt{'drop'} or $opt{'create'};
}


######################################################################
## Copy table rows                                                  ##
######################################################################

if ($opt{'copy'})
{
    print STDERR "All metadata processing is finished.  Now starting the table copies.\n\n";

    foreach $table (@tables)
    {
	print STDERR "Copying table contents $$table{'name'}...\n";
	copyTable($$table{'name'});
    }
}


######################################################################
## Create triggers and generators or serial values                  ##
######################################################################

if ($opt{'pgconv'})
{
    foreach $trigger (keys %triggers)
    {
	my $tname = $triggers{$trigger}{'table'};
	my $fname = $triggers{$trigger}{'field'};
	my $slen = length($tname) + length($fname);
	my $overrun = $slen - (31 - 5); # Maximum length of the name is 31, minus _ _seq
	if ($overrun > 0)
	{
	    $tname = substr ($tname, 0, length($tname) - $overrun);
	}
	my $sequence = sprintf ("%s_%s_seq",
				$tname,
				$fname);
	if ($opt{'sqlout'})
	{
	    printf "select setval('%s', %s)%s",
	    $sequence,
	    $triggers{$trigger}{'curval'},
	    $sqlout_eol;
	    debug(sprintf ("Setting sequence %s to %s", $sequence, $triggers{$trigger}{'curval'}));
	}
    }
}
else
{
    debug("Interbase triggers and generators get created here.  Eventually.");
}


######################################################################
## Cleanup                                                          ##
######################################################################

# Close all database connections
foreach $db (keys %server)
{
    $dbh{$db}->disconnect;
}

exit;


######################################################################
## Subroutines                                                      ##
######################################################################

# Given a table name and a list of columns to operate on, copy all
# rows from $dbh{'src'} to $dbh{'dest'}
sub copyTable
{
    my $tableName = shift;
    my @columns;
    my $row;
    my $sth;
    my $source;
    my $dest;
    my $select;
    my $insert;

    $sth = $dbh{'src'}->prepare("select * from $tableName");
    $sth->execute;
    @columns = @{$sth->{'NAME'}};
    $sth->finish;

    # Create the select and insert statements
    $select = "select " . join(',', @columns) . " from $tableName";
    $insert = "insert into $tableName (" . join(',', @columns) . ") values ("
	. '?' . ',?' x (@columns - 1)
	. ")";

    debug("Select: $select");
    debug("Insert: $insert");

    # Prepare the queries
    unless ($source = $dbh{'src'}->prepare($select, $fetchAttr))
    {
	print STDERR "Unable to prepare the source query\n";
	return -1;
    }
    unless ($opt{'sqlout'})
    {
	unless ($dest = $dbh{'dst'}->prepare($insert))
	{
	    print STDERR "Unable to prepare the destination query\n";
	    $source->finish;
	    return -1;
	}
    }

    $source->execute;

    if ($opt{'sqlout'})
    {
	my $stmt = "insert into $tableName (" . join(',', @columns) . ") values (";
	while ($row = $source->fetch)
	{
	    print $stmt;
	    my $printed = 0;
	    foreach (@$row)
	    {
		print ',' if $printed;
		$printed = 1;
		if (not defined $_)
		{
		    print 'NULL';
		    next;
		}
		# Quote all apostrophes
		s/'/''/g;        # '
		# Quote all backslashes
		s/\\/\\\\/g;
		print "\'" . $_ . "\'";
	    }
	    print ")" . $sqlout_eol;
	}
    }
    else
    {
        while ($row = $source->fetch)
	{
	    $dest->execute(@$row);
	}
	$dest->finish;
    }
    $source->finish;
}

# Send a command to isql.  The results are stored in $tmp_file
sub sendIsqlCmd
{
    my $command = shift;
    my $dbname = $server{'src'}{'dbname'};
    $dbname =~ s/^.*://;
    $dbname =~ s/;.*$//;
    debug("ISQL: Sending \"$command\" to $server{'src'}{'hostname'}:$dbname");
    open OUTPIPE, "| $isql_syscmd -U $server{'src'}{'username'} -P $server{'src'}{'password'} $server{'src'}{'hostname'}:$dbname > $tmp_file"
	or die "Unable to open the isql output pipe: $!";
    print OUTPIPE "$command;\n";
    close OUTPIPE;
    $infile->open($tmp_file) or die "Unable to read from the temp file: $!";
    my $foo = <$infile>;
    return $infile;
}

# Clean up input lines from an sendIsqlCmd temp file
sub trimIsql
{
    my $inline = shift;
    chomp $inline;
    $inline =~ s/^SQL>\s*//;
    $inline =~ s/^\s*//;
    $inline =~ s/\s*$//;
    return $inline;
}

# Selectively print debugging messages
sub debug
{
    my $message = shift;
    print STDERR "DEBUG: $message\n" if $opt{'debug'};
}
