package Ubic::Service::ZooKeeper;

use strict;
use warnings;

# ABSTRACT: running ZooKeeper as Ubic service

=head1 SYNOPSIS

  use Ubic::Service::ZooKeeper;
  return Ubic::Service::ZooKeeper->new({
      clientPort => 2181,
      dataDir    => '/var/lib/zookeeper',
      tickTime   => 2000,
      dataLogDir => '/var/log/zookeeper',
      initLimit  => 10,
      syncLimit  => 5,
      servers    => {
          1 => { server => "host1:2888:3888" },
          2 => { server => "host2:2888:3888" },
          3 => { server => "host3:2888:3888" },
      },
      myid => 1,

      ubic_log => '/var/log/zookeeper/ubic.log',
      stdout   => '/var/log/zookeeper/stdout.log',
      stderr   => '/var/log/zookeeper/stderr.log',
      user     => 'zookeeper',
      gen_cfg  => '/etc/zookeeper/conf/zoo.cfg',
      pidfile  => '/tmp/zookeeper.pid',

      java_cmd_opt => '-cp /etc/zookeeper/conf:/usr/share/java/zookeeper.jar ' .
                      '-Dcom.sun.management.jmxremote';
});

=head1 DESCRIPTION

This module intends for running/shutdown ZooKeeper as a L<Ubic> service. It accepts all config options of ZooKeeper (and some other options), generates config and myid file and run it.


=head1 METHODS

Typically you need to use only C<new()> method, but you can find below description of some another methods also.

=over

=cut

use parent qw(Ubic::Service::Common);

use File::Copy qw(move);
use File::Spec::Functions qw(catfile);
use IO::Socket::INET;
use Params::Validate qw(:all);
use Time::HiRes qw();
use Ubic::Daemon qw(:all);
use Ubic::Result qw(:all);

=item C<new($params)>

Creates new ZooKeeper service. C<$params> is hashref with different ZooKeeper and Ubic params.

ZooKeeper config params: C<clientPort> (default is C<2181>), C<dataDir> (default is C</var/lib/zookeeper>), C<tickTime> (default is C<2000>), C<dataLogDir>, C<globalOutstandingLimit>, C<preAllocSize>, C<snapCount>, C<traceFile>, C<maxClientCnxns>, C<clientPortAddress>, C<minSessionTimeout>, C<maxSessionTimeout>, C<electionAlg>, C<initLimit>,
C<leaderServer>, C<servers>, C<syncLimit>, C<cnxTimeout>, C<forceSync>, C<jute.maxbuffer>, C<skipACL>.

You can find description for all of this params on L<http://zookeeper.apache.org/doc/trunk/zookeeperAdmin.html#sc_configuration>.

One exception is a C<servers> param. It combines C<server.x> and C<weight.x> params from ZooKeeper config. C<servers> is a hashref where key is a number of server and the values is a hashref with keys C<server> and C<weight>.

All of these params are optional.

Remain params are:

=over

=item I<myid> (optional)

Id of the current server in ZooKeeper cluster. Based on this value Ubic::Service::Zookeeper will create C<myid> file in C<dataDir>.

Default is C<1>.

=item I<status> (optional)

Coderef for checking ZooKeeper status. Default implemetation uses C<ruok> ZooKeeper command.

=item I<user> (optional)

User name that will be used as real and effective user identifier during exec of ZooKeeper.

=item I<ubic_log> (optional)

Path to ubic log.

=item I<stdout> (optional)

Path to ZooKeeper stdout log.

ZooKeeper supports custom logging setup, so in most cases this param is meaningless.

=item I<stderr> (optional)

Path to ZooKeeper stderr log.

ZooKeeper supports custom logging setup, so in most cases this param is meaningless.

=item I<pidfile> (optional)

Pidfile for C<Ubic::Daemon> module.

If not specified it is a /tmp/zookeeper.<clientPort>.pid.

=item I<gen_cfg> (optional)

Generated ZooKeeper config file name.

If not specified it is a /tmp/zoo.<clientPort>.cfg.

=item I<java_cmd_opt> (optional)

The last but not least param. C<Ubic::Service::ZooKeeper> don't know anything about zookeeper.jar and another .jar places and other Java specific options (but don't place in C<java_cmd_opt> path to java and path to .cfg file, C<Ubic::Service::ZooKeeper> do it yourself).

So for successful running ZooKeeper using this module you should pass all needed for C<java> options such as C<-cp> (classpath), system property values (C<-D> flag) and other options. See ZooKeeper administration guide for more specific details.

=back

=cut

sub new {
    my $class = shift;

    my $opt_num = { type => SCALAR, regex => qr/^\d+$/o, optional => 1 };
    my $opt_str = { type => SCALAR, optional => 1 };

    my $params = validate(@_, {
        # zookeeper config options
        ### minimum zookeeper config
        clientPort => { %$opt_num, default => 2181 },
        dataDir    => { type => SCALAR, default => '/var/lib/zookeeper' },
        tickTime   => { %$opt_num, default => 2000 },

        ### advanced zookeeper config
        dataLogDir             => $opt_str,
        globalOutstandingLimit => $opt_num,
        preAllocSize           => $opt_num,
        snapCount              => $opt_num,
        traceFile              => $opt_str,
        maxClientCnxns         => $opt_num,
        clientPortAddress      => $opt_str,
        minSessionTimeout      => $opt_num,
        maxSessionTimeout      => $opt_num,

        ### zookeeper cluster options
        electionAlg  => $opt_num,
        initLimit    => $opt_num,
        leaderServes => $opt_str,
        # num, hostname, port and weight of each server
        servers      => { type => HASHREF, optional => 1 },
        syncLimit    => $opt_num,
        cnxTimeout   => $opt_num,

        ### unsafe zookeeper options
        forceSync        => $opt_str,
        'jute.maxbuffer' => $opt_num,
        skipACL          => $opt_str,


        # zookeeper myid
        myid => { %$opt_num, default => 1 },


        # ubic specific options
        status        => { type => CODEREF, optional => 1 },
        user          => $opt_str,
        ubic_log      => $opt_str,
        stdout        => $opt_str,
        stderr        => $opt_str,
        pidfile       => $opt_str,
        port          => $opt_num,

        gen_cfg       => $opt_str,
        java_cmd_opt  => { %$opt_str, default => '' },
    });

    if (!$params->{pidfile}) {
        $params->{pidfile} = '/tmp/zookeeper.' . $params->{clientPort} . '.pid';
    }
    if (!$params->{gen_cfg}) {
        $params->{gen_cfg} = '/tmp/zoo.' . $params->{clientPort} . '.cfg';
    }

    return bless $params => $class;
}


=item C<bin()>

Get command-line with all arguments in the arrayref form.

=cut

sub bin {
    my $self = shift;

    my @cmd = ("java", $self->{java_cmd_opt}, $self->gen_cfg);

    return \@cmd;
}

sub _ubic_params_list {
    return qw/status user ubic_log stdout stderr pidfile gen_cfg port/;
}

=item C<create_cfg_file()>

Generates .cfg file basing on constuctor params.

=cut

sub create_cfg_file {
    my $self = shift;

    my $fname = $self->gen_cfg;
    my $tmp_fname = $fname . ".tmp";
    my %params = %$self;

    for ('myid', _ubic_params_list()) {
        delete $params{$_};
    }
    my $servers = delete $params{servers};

    open(my $tmp_fh, '>', $tmp_fname) or die "Can't open file [$tmp_fname]: $!";

    foreach my $p (sort keys %params) {
        my $v = $params{$p};
        print $tmp_fh "$p=$v\n";
    }
    print $tmp_fh "\n";
    foreach my $server_num (sort {$a <=> $b} keys %$servers) {
        my $s = $servers->{$server_num};
        my $server = $s->{server};
        print $tmp_fh "server.${server_num}=$server\n";

        if ($s->{weight}) {
            print $tmp_fh "weight.${server_num}=$s->{weight}\n";
        }
    }

    close($tmp_fh) or die "Can't close file [$tmp_fname]: $!";
    move($tmp_fname, $fname) or die "Can't move file [${tmp_fname}] to [$fname]: $!";
}


=item C<create_myid_file()>

Generates C<myid> file basing on C<myid> and C<dataDir> params in constructor.

=cut

sub create_myid_file {
    my $self = shift;

    my $fname = catfile($self->{dataDir}, 'myid');
    my $tmp_fname = $fname . ".tmp";

    open(my $tmp_fh, '>', $tmp_fname) or die "Can't open file [$tmp_fname]: $!";
    print $tmp_fh $self->{myid}, "\n";
    close($tmp_fh) or die "Can't close file [$tmp_fname]: $!";
    move($tmp_fname, $fname) or die "Can't move file [${tmp_fname} to [$fname]: $!";
}

sub start_impl {
    my $self = shift;

    $self->create_cfg_file;
    $self->create_myid_file;

    my $daemon_opts = { bin => $self->bin, pidfile => $self->pidfile, term_timeout => 5 };
    for (qw/ubic_log stdout stderr/) {
        $daemon_opts->{$_} = $self->{$_} if defined $self->{$_};
    }
    start_daemon($daemon_opts);

    return;
}

sub stop_impl {
    my $self = shift;

    return stop_daemon($self->pidfile, { timeout => 7 });
}

sub status_impl {
    my $self = shift;

    my $running = check_daemon($self->pidfile);
    return result('not running') unless ($running);

    my $sock = IO::Socket::INET->new(
        PeerAddr => "localhost",
        PeerPort => $self->port,
        Proto    => "tcp",
        Timeout  => 1,
        Blocking => 0,
    ) or return result('not running');

    $sock->print('ruok');
    my $resp = '';
    for (1..10) {
        my $buff;
        $sock->sysread($buff, 4);
        $resp .= $buff if defined($buff);
        last if (length($resp) >= 4);
        Time::HiRes::sleep(0.1);
    }
    if ($resp eq 'imok') {
        return result('running');
    } else {
        return result('broken');
    }
}

sub user {
    my $self = shift;

    return $self->{user} if defined $self->{user};
    return $self->SUPER::user;
}

=item C<pidfile()>

Get pidfile name.

=cut

sub pidfile {
    my $self = shift;

    return $self->{pidfile};
}

sub gen_cfg {
    my $self = shift;

    return $self->{gen_cfg};
}

sub port {
    my $self = shift;

    return $self->{port} if defined $self->{port};
    return $self->{clientPort};
}

sub timeout_options {
    return {
        start => { trials => 15, step => 0.1 },
        stop  => { trials => 15, step => 0.1 }
    };
}

=back

=head1 SEE ALSO

L<http://zookeeper.apache.org/doc/trunk/zookeeperAdmin.html>

L<Ubic>

=cut

1;
