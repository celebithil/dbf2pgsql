#!/usr/bin/env perl
use warnings;
use strict;
use v5.10;
use XBase;
use Cwd;
use DBI;
use DBD::Pg;
use Encode qw(encode decode);
use Getopt::Std;

my %opts;      #hash of options from command line
&getoptions;    #get options from commandline

#todo comments
my @files    = glob("*.[Dd][Bb][Ff]");
my $basename = $opts{'n'};               #name of sql base
my $login    = $opts{'l'};               #login to sql server
my $password = $opts{'p'};               #password to sql server
my ( $dbh, $sth );                                       #databasehandler
my ( $table, $num, @type, @name, @len, @dec, $num_f );
my $sqlcommand;

if ( $opts{'f'} ) {                                      # open file for record
    open FILEOUT, ">", $opts{'N'} . '.sql';
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
    $num   = 1 + $table->last_record;                  # number of records
    @type  = $table->field_types;                      # array of fields types
    @name  = $table->field_names;                      # array of fields names
    @len   = $table->field_lengths;                    # array of fields lengths
    @dec        = $table->field_decimals;      # array of fields decimals (?)
    $num_f      = @type;                       # number of fields
    $f_table    = substr( $f_table, 0, -4 );
    $sqlcommand = &create_table($f_table);

    if ( $opts{'f'} ) {                        # convert data to file
        print( FILEOUT "$sqlcommand\n" );
    }

    else {                                     # convert data to PGSQL
        $sth = $dbh->prepare($sqlcommand);
        $sth->execute;
    }

    print "Table $f_table created\n";
    if ($num) {                                # if table not empty
        my $cursor = $table->prepare_select();

        unless ( $opts{'f'} ) {                # copy in base
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
        else {    # copy in file
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

sub basename {    # get name of base
    my $full_path = cwd;
    my @dirs      = split( /\//, $full_path );
    my $basename  = lc( $dirs[-1] );
    return $basename;
}

sub getoptions {    # get options from command line
    getopts( 's:d:m:n:l:p:c:N:f', \%opts );

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
    -N name of output file (by default using basename)\n
    -c count of records for one time recording to base (default 10000)\n";
    }

    $opts{'s'} //= 'cp866';
    $opts{'d'} //= 'cp1251';
    $opts{'m'} //= 't';
    $opts{'n'} //= &basename;
    $opts{'c'} //= 10000;
    $opts{'N'} //= $opts{'n'};

}

sub create_table {    # make command 'CREATE TABLE'
    my $f_table    = shift;
    my $sqlcommand = "CREATE TABLE $f_table (";
    for my $i ( 0 .. $#type ) {
        $sqlcommand .= '"' . $name[$i] . '"' . ' ';
        given ( $type[$i] ) {
            when ( 'C' or '0' ) {
                $sqlcommand .= 'char(' . $len[$i] . ')';
            }
            when ('D') {
                $sqlcommand .= 'date';
            }
            when ('M') {
                $sqlcommand .= ( $opts{'m'} eq 't' ) ? 'text' : 'bytea';
            }
            when ('L') {
                $sqlcommand .= 'boolean';
            }
            when ('N') {
                $sqlcommand .= 'numeric(' . $len[$i] . ',' . $dec[$i] . ')';
            }
            when ('B') {
                $sqlcommand .= ( $len[$i] == 10 ) ? 'bytea' : 'bigint';
            }
        }
        $sqlcommand .= ', ';
    }
    return substr( $sqlcommand, 0, length($sqlcommand) - 2 ) . ');';
}

sub convert_data {    # convert data to copy
    my $sqlcommand = '';
    my $record     = shift;
    for my $i ( 0 .. $#type ) {

        given ( $type[$i] ) {
            when ('C') {
                $$record[$i] = ${ &get_quoted_text( \$$record[$i] ) } // '\N';
                break;
            }
            when ('D') {
                $$record[$i] =
                  ( $$record[$i] )
                  ? ${ &get_formated_date( \$$record[$i] ) }
                  : '\N';
                break;
            }
            when ('N') {
                $$record[$i] //= 0;
                break;
            }
            when ('L') {
                $$record[$i] =
                    ( $$record[$i] )         ? 'true'
                  : ( defined $$record[$i] ) ? 'false'
                  :                            '\N';
                break;
            }
            when ('M') {
                $$record[$i] =
                  ( $opts{'m'} eq 't' )
                  ? encode( "$opts{'d'}", decode( "$opts{'s'}", $$record[$i] ) )
                  : ${ &get_quoted_blob( \$$record[$i] ) };
                break;
            }
            when ('B') {
                $$record[$i] =
                    ( $len[$i] == 10 ) ? ${ &get_quoted_blob( \$$record[$i] ) }
                  : ( defined $$record[$i] ) ? $$record[$i]
                  :                            0;
                break;
            }
            when ('0') {
                $$record[$i] //= 0;
                break;
            }

        }

        $sqlcommand .= "@{$record}[$i]" . "\t";
    }
    $sqlcommand = substr( $sqlcommand, 0, length($sqlcommand) - 1 ) . "\n";
    return $sqlcommand;
}

sub get_quoted_text {    #get text data
    my $text_ref = shift;
    $$text_ref =~ s/\\/\\\\/g;
    $$text_ref =~ s/(\x09|\x0D|\x0A)/'\\x'.sprintf ("%02X", unpack("C", $1))/ge;
    $$text_ref = encode( "$opts{'d'}", decode( "$opts{'s'}", $$text_ref ) );
    return $text_ref;
}

sub get_quoted_blob {    #get blob data
    my $blob_ref = shift;
    $$blob_ref =~
s/([\x00-\x19\x27\x5C\x7F-\xFF])/'\\\\'.sprintf ("%03o", unpack("C", $1))/ge;
    return $blob_ref;
}

sub get_formated_date {    #format date data to postgres
    my $date_ref = shift;
    $$date_ref = sprintf( "%08d", $$date_ref ) if ( length($$date_ref) < 8 );
    $$date_ref =~ s/(\d{4})(\d{2})(\d{2})/\'$1-$2-$3\'/;
    return $date_ref;
}
