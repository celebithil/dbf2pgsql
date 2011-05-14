#!/usr/bin/env perl
use XBase;
use Cwd;
use DBI;
use DBD::Pg;
use Encode qw(encode decode);
use warnings;
use strict;
use Getopt::Std;

our %opts;      #hash of options from command line
&getoptions;    #get options from commandline

#todo comments
my @files    = glob("*.[Dd][Bb][Ff]");
my $basename = $opts{'n'};               #name of sql base
my $login    = $opts{'l'};               #login to sql server
my $password = $opts{'p'};               #password to sql server
my ( $dbh, $sth );#databasehandler
my ( $table, $num, @type, @name, @len, @dec, $num_f );
my $sqlcommand;


if ( $opts{'f'} ) {# open file for record
    open FILEOUT, "> $opts{'f'}" . '.sql';
}

else {
    $dbh = DBI->connect( "DBI:Pg:dbname=postgres", "$login", "$password" )
      or die("Could't connect to database: $DBI:: errstr");
    $dbh->do("drop database $basename");
    $dbh->do("create database $basename");
    $dbh->disconnect();
    $dbh = DBI->connect( "DBI:Pg:dbname=$basename", "$login", "$password" )
      or die("Could't connect to database: $DBI:: errstr");
}

for my $f_table (@files) {
    $table = new XBase "$f_table" or die XBase->errstr;
    $num   = 1 + $table->last_record;                    # number of records
    @type  = ( $table->field_types );                    # array of fields types
    @name  = ( $table->field_names );       # array of fields names
    @len   = ( $table->field_lengths );     # array of fields lengths
    @dec   = ( $table->field_decimals );    # array of fields decimals (?)
    $num_f = scalar(@type);
    $f_table    = substr( $f_table, 0, length($f_table) - 4 );
    $sqlcommand = &create_table($f_table);

    if ( $opts{'f'} ) {# convert data to file
        print( FILEOUT "$sqlcommand\n" );
    }

    else {# convert data to PGSQL
        $sth = $dbh->prepare($sqlcommand);
        $sth->execute;
    }

    print "Table $f_table created\n";
    if ( $num > 0 ) {# if table not empty
        my $cursor = $table->prepare_select();

        unless ( $opts{'f'} ) {# copy in base
            $sqlcommand = "copy $f_table from stdin";
            $dbh->do($sqlcommand) or die $DBI::errstr;

            for ( my $j = 1 ; $j <= $num ; $j++ ) {
                $sqlcommand = '';
                my @record = $cursor->fetch;
                $sqlcommand = &convert_data( \@record );
                $dbh->pg_putcopydata($sqlcommand);

                if ( !( $j % $opts{'c'} ) and $j < $num ) {
                    $dbh->pg_putcopyend();
                    $sqlcommand = "copy $f_table from stdin";
                    $dbh->do($sqlcommand) or die $DBI::errstr;
                    print "$j records of $num from $f_table copied\n";
                }

            }
            $dbh->pg_putcopyend();

        }
        else {# copy in file
            my $buffer = '';
            for ( my $j = 1 ; $j <= $num ; $j++ ) {
                my @record = $cursor->fetch;
                $sqlcommand = &convert_data( \@record );
                $buffer .= $sqlcommand;
                if ( !( $j % $opts{'c'} ) and $j < $num ) {
                    print( FILEOUT "$buffer" );
                    print "$j records of $num from $f_table copied\n";
                    $buffer = '';
                }
            }
            print( FILEOUT "$buffer" );
        }
    }
    print "Table $f_table copied\n";
    $table->close;
}

unless ( $opts{'f'} ) {
    $dbh->disconnect();
}
else { close(FILEOUT); }

sub basename {# get name of base 
    my $full_path = cwd;
    my @dirs      = split( /\//, $full_path );
    my $basename  = lc( $dirs[ scalar(@dirs) - 1 ] );
    return $basename;
}

sub getoptions {# get options from command line
    getopt( 'sdmnlpfc', \%opts );

    unless (%opts) {
        die "
    no params!!!\n
    -l login\n
    -p password\n
    -n basename (if empty, basename = name of current directory)\n
    -s source codepage (default cp866)\n
    -d destination codepage (default cp1251)\n
    -m interpretation of memo field t (text), b (binary), default (t)\n
    -f print sql commands in file (by default dbf converting in base directly)\n
    -c count of records for one time recording to base (default 10000)\n";
    }

    unless ( defined $opts{'s'} ) { $opts{'s'} = 'cp866' }
    unless ( defined $opts{'d'} ) { $opts{'d'} = 'cp1251' }
    unless ( defined $opts{'m'} ) { $opts{'m'} = 't' }
    unless ( defined $opts{'n'} ) { $opts{'n'} = &basename }
    unless ( defined $opts{'c'} ) { $opts{'c'} = 10000 }

}

sub create_table {# make command 'CREATE TABLE'
    my $f_table    = shift;
    my $sqlcommand = "CREATE TABLE $f_table (";
    for ( my $i = 0 ; $i < $num_f ; $i++ ) {
        $sqlcommand .= '"' . $name[$i] . '"' . ' ';
        if ( ( $type[$i] eq 'C' ) or ( $type[$i] eq '0' ) ) {
            $sqlcommand .= 'char(' . $len[$i] . ')';
        }
        elsif ( $type[$i] eq 'D' ) {
            $sqlcommand .= 'date';
        }
        elsif ( $type[$i] eq 'M' ) {
            if    ( $opts{'m'} eq 't' ) { $sqlcommand .= 'text' }
            elsif ( $opts{'m'} eq 'b' ) { $sqlcommand .= 'bytea' }
        }
        elsif ( $type[$i] eq 'L' ) {
            $sqlcommand .= 'boolean';
        }
        elsif ( $type[$i] eq 'N' ) {
            $sqlcommand .= 'numeric(' . $len[$i] . ',' . $dec[$i] . ')';
        }
        elsif ( $type[$i] eq 'B' ) {
            if    ( $len[$i] == 10 ) { $sqlcommand .= 'bytea'; }
            elsif ( $len[$i] == 8 )  { $sqlcommand .= 'bigint'; }
        }
        $sqlcommand .= ', ';
    }
    return substr( $sqlcommand, 0, length($sqlcommand) - 2 ) . ');';
}

sub convert_data {# convert data to copy
    my $sqlcommand = '';
    my $record     = shift;
    for ( my $i = 0 ; $i < $num_f ; $i++ ) {
        if ( $type[$i] eq 'C' ) {

            if ( defined( @{$record}[$i] ) ) {
                @{$record}[$i] =~ s/\\/\\\\/g;
                @{$record}[$i] =~
                  s/\x09|\x0D|\x0A/'\\x'.sprintf ("%02X", unpack("C", $&))/ge;
                @{$record}[$i] =
                  encode( "$opts{'d'}",
                    decode( "$opts{'s'}", @{$record}[$i] ) );
            }

            else { @{$record}[$i] = '\N' }
        }

        elsif ( $type[$i] eq 'D' ) {
            if ( @{$record}[$i] ) {

                if ( length( @{$record}[$i] ) < 8 ) {
                    @{$record}[$i] = sprintf( "%08d", @{$record}[$i] );
                }

                @{$record}[$i] =~ s/(\d{4})(\d{2})(\d{2})/\'$1-$2-$3\'/;
            }
            else { @{$record}[$i] = '\N'; }

        }
        elsif ( ( $type[$i] eq 'N' ) and !( defined( @{$record}[$i] ) ) ) {
            @{$record}[$i] = '0';
        }

        elsif ( $type[$i] eq 'L' ) {
            unless ( defined( @{$record}[$i] ) ) { @{$record}[$i] = 'false' }
            else {
                if ( ( @{$record}[$i] eq 'T' ) or ( @{$record}[$i] eq '1' ) ) {
                    @{$record}[$i] = 'true';
                }
                else { @{$record}[$i] = 'false' }
            }
        }

        elsif ( $type[$i] eq 'M' ) {

            if ( $opts{'m'} eq 't' ) {
                @{$record}[$i] =
                  encode( "$opts{'d'}",
                    decode( "$opts{'s'}", @{$record}[$i] ) );
            }

            else {
                @{$record}[$i] =~
s/[\x00-\x19\x27\x5C\x7F-\xFF]/'\\\\'.sprintf ("%03o", unpack("C", $&))/ge;
            }
        }

        elsif ( $type[$i] eq 'B' ) {
			@{$record}[$i] =~
s/[\x00-\x19\x27\x5C\x7F-\xFF]/'\\\\'.sprintf ("%03o", unpack("C", $&))/ge if    ( $len[$i] == 10 );
        }

        elsif ( ( $type[$i] eq '0' ) && ( @{$record}[$i] eq '' ) ) {
            @{$record}[$i] = '0';
        }
        $sqlcommand .= "@{$record}[$i]" . "\t";
    }
    $sqlcommand = substr( $sqlcommand, 0, length($sqlcommand) - 1 ) . "\n";
    return $sqlcommand;

}
