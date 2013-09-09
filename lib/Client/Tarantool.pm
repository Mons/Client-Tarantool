package Client::Tarantool;

use 5.008008;

our $VERSION = '0.01';

use strict;
use Protocol::Tarantool ':constant';
use Protocol::Tarantool::Spaces;
use Carp;
use IO::Socket::INET;


sub tcp_connect($$) {
	return IO::Socket::INET->new(
		Proto => 'tcp',
		PeerAddr => $_[0],
		PeerPort => $_[1],
	) or die "$!";
}

sub init {
	my $self = shift;
	$self->{debug} ||= 0;
	$self->{timeout} ||= 5;
	$self->{reconnect} = 0.1 unless exists $self->{reconnect};
	
	$self->{spaces} = Protocol::Tarantool::Spaces->new( delete $self->{spaces});
	$self->{seq}  = 0;
	$self->{req} = {};
	
	if (exists $self->{server}) {
		($self->{host},$self->{port}) = split ':',$self->{server},2;
	} else {
		$self->{server} = join ':',$self->{host},$self->{port};
	}
}

sub new {
	my $pk = shift;
	my $self = bless {
		host      => '0.0.0.0',
		port      => '33013',
		usehash   => 1,
		@_,
	}, $pk;
	$self->init();
	$self->{fh} = tcp_connect $self->{host},$self->{port}
		or die "Can't connect: $!";
	return $self;
}

sub tuple2hash {
	my $this = shift;
	my $space = shift;
	my $tuple = shift;
	$this->{tntcache}{ $space } ||= $this->{spaces}->space( $space )->{fnames};
	my %h;my @tail;
	( @h{ @{ $this->{tntcache}{ $space } } }, @tail ) = @$tuple;
	$h{'~'} = \@tail if @tail;
	return \%h;
}

sub hash2tuple {
	my $this = shift;
	my $space = shift;
	my $hash = shift;
	$this->{tntcache}{ $space } ||= $this->{spaces}->space( $space )->{fnames};
	my @a;
	for ( @{ $this->{tntcache}{ $space } } ) {
		push @a, $hash->{$_};
	}
	push @a, @{ $hash->{'~'} } if exists $hash->{'~'};
	return \@a;
}

sub _hash_flags_to_mask($) {
	return $_[0] || 0 unless ref $_[0];
	my $flags = shift;
	
	if (ref $flags eq 'ARRAY') {
		my %flags; @flags{ @$flags } = (1) x @$flags;
		$flags = \%flags;
	}
	elsif (ref $flags eq 'HASH') {
		# ok
	}
	else {
		croak "Flags could be bitmask or hashref or arrayref";
	}
	return +
		( $flags->{return} ? TNT_FLAG_RETURN : 0 )
		|
		( $flags->{ret} ? TNT_FLAG_RETURN : 0 )
		|
		( $flags->{add} ? TNT_FLAG_ADD : 0 )
		|
		( $flags->{replace} ? TNT_FLAG_REPLACE : 0 )
		|
		( $flags->{rep} ? TNT_FLAG_REPLACE : 0 )
		|
		( $flags->{quiet} ? TNT_FLAG_BOX_QUIET : 0 )
		|
		( $flags->{notstore} ? TNT_FLAG_NOT_STORE : 0 )
	;
}

sub select : method { #( space, keys, [ { index, limit, offset } ] )
	my $self = shift;
	eval {
		my $sno = shift;
		my $keys = shift;
		my $space = $self->{spaces}->space( $sno );
		#warn "space = ".dumper $space;
		my $opts = shift || {};
		
		my $index = $space->index( $opts->{index} || 0 );
		($keys,my $format) = $space->keys( $keys, $index );
		
		# req_id, ns, idx, offset, limit, keys, [ format ]
		#warn "select from space no = $space->{no} @{ $keys }";
		my $pk = Protocol::Tarantool::select(
			++$self->{seq},
			$space->{no},
			$index->{no},
			$opts->{offset} || 0,
			$opts->{limit} || 0xFFFFFFFF,
			$keys,
			$format,
		);
		#warn "Created select: \n".xd $pk;
		my $wr = syswrite($self->{fh}, $pk );
		#warn "written $wr $!";
		my $read = read $self->{fh},my $buf, 12;
		$read == 12 or die "read failed: $!";
		my $need = Protocol::Tarantool::peek_size( \$buf );
		#warn "Read header, need +$need";
		my $read = read $self->{fh},$buf, $need, 12;
		$read == $need or die "read failed: $!";
		#warn join ' ',unpack '(H2)*',$buf;
		#warn "unpacking with $format/$space->{unpack}";
		my $r = Protocol::Tarantool::response(\$buf, $space->{unpack}, $space->{default_unpack}[0] );
		if ($r->{code} == 0) {
			return $r->{tuples} unless $self->{usehash};
			return [ map { $self->tuple2hash( $space,$_ ) } @{ $r->{tuples} } ]
		} else {
			#warn Data::Dumper::Dumper $r;
			die "$r->{code}: $r->{status}";
		}
	1} or do {
		die "$@";
	};
	#my $space = $self->{spaces}->find( $_[0] );
}

sub update : method { #( space, primary-key, cb )
	my $self = shift;
	eval {
		my $sno   = shift;
		my $key   = shift;
		my $ops   = shift;
		my $flags = _hash_flags_to_mask( shift );
		
		my $space = $self->{spaces}->space( $sno );
		
		my ($keys,$format) = $space->keys( [ $key ], 0, 1 );
		$ops = $space->updates( $ops );
		@$keys != 1 and croak "delete takes only one primary key";
		
		# req_id, ns, idx, offset, limit, keys, [ format ]
		my $pk = Protocol::Tarantool::update(
			++$self->{seq},
			$space->{no},
			$flags,
			$keys->[0],
			$ops,
			$format,
		);
		#warn "Created update: \n".xd $pk;
		my $wr = syswrite($self->{fh}, $pk );
		#warn "written $wr $!";
		my $read = read $self->{fh},my $buf, 12;
		$read == 12 or die "read failed: $!";
		my $need = Protocol::Tarantool::peek_size( \$buf );
		#warn "Read header, need +$need";
		my $read = read $self->{fh},$buf, $need, 12;
		$read == $need or die "read failed: $!";
		#warn join ' ',unpack '(H2)*',$buf;
		my $r = Protocol::Tarantool::response(\$buf, $format ? ($format, $space->{default_unpack}[0]) : ( $space->{unpack}, $space->{default_unpack}[0] ));
		if ($r->{code} == 0) {
			return $r->{tuples} unless $self->{usehash};
			return [ map { $self->tuple2hash( $space,$_ ) } @{ $r->{tuples} } ]
		} else {
			#warn Data::Dumper::Dumper $r;
			die "$r->{code}: $r->{status}";
		}
	1} or do {
		die $@;
	};
}

sub insert : method { #( space, tuple [,flags], cb )
	my $self = shift;
	eval {
		my $sno   = shift;
		my $tuple = shift;
		my $flags = _hash_flags_to_mask( shift );
		
		my $space = $self->{spaces}->space( $sno );
		($tuple,my $format) = $space->tuple( $tuple );
		
		# req_id, ns, idx, offset, limit, keys, [ format ]
		++$self->{seq};
		my $pk = Protocol::Tarantool::insert(
			$self->{seq},
			$space->{no},
			$flags,
			$tuple,
			$format,
		);
		#warn "Created insert: \n".xd $pk;
		my $wr = syswrite($self->{fh}, $pk );
		#warn "written $wr $!";
		my $read = read $self->{fh},my $buf, 12;
		$read == 12 or die "read failed: $!";
		my $need = Protocol::Tarantool::peek_size( \$buf );
		#warn "Read header, need +$need";
		my $read = read $self->{fh},$buf, $need, 12;
		$read == $need or die "read failed: $!";
		#warn join ' ',unpack '(H2)*',$buf;
		my $r = Protocol::Tarantool::response(\$buf, $format ? ($format, $space->{default_unpack}[0]) : ( $space->{unpack}, $space->{default_unpack}[0] ));
		if ($r->{code} == 0) {
			return $r->{tuples} unless $self->{usehash};
			return [ map { $self->tuple2hash( $space,$_ ) } @{ $r->{tuples} } ]
		} else {
			#warn Data::Dumper::Dumper $r;
			die "$r->{code}: $r->{status}";
		}
	1} or do {
		die $@;
	};
}

sub delete : method { #( space, primary-key, cb )
	my $self = shift;
	my $cb   = pop;
	eval {
		my $sno   = shift;
		my $key   = shift;
		my $flags = _hash_flags_to_mask( shift );
		my $space = $self->{spaces}->space( $sno );
		#warn "space = ".dumper $space;
		
		my ($keys,$format) = $space->keys( [ $key ], 0, 1 );
		@$keys != 1 and croak "delete takes only one primary key";
		
		# req_id, ns, idx, offset, limit, keys, [ format ]
		my $pk = Protocol::Tarantool::delete(
			++$self->{seq},
			$space->{no},
			$flags,
			$keys->[0],
			$format,
		);
		#warn "Created delete: \n".xd $pk;
		my $wr = syswrite($self->{fh}, $pk );
		#warn "written $wr $!";
		my $read = read $self->{fh},my $buf, 12;
		$read == 12 or die "read failed: $!";
		my $need = Protocol::Tarantool::peek_size( \$buf );
		#warn "Read header, need +$need";
		my $read = read $self->{fh},$buf, $need, 12;
		$read == $need or die "read failed: $!";
		#warn join ' ',unpack '(H2)*',$buf;
		my $r = Protocol::Tarantool::response(\$buf, $format ? ($format, $space->{default_unpack}[0]) : ( $space->{unpack}, $space->{default_unpack}[0] ));
		if ($r->{code} == 0) {
			return $r->{tuples} unless $self->{usehash};
			return [ map { $self->tuple2hash( $space,$_ ) } @{ $r->{tuples} } ]
		} else {
			#warn Data::Dumper::Dumper $r;
			die "$r->{code}: $r->{status}";
		}
	1} or do {
		die $@;
	};
}

sub lua : method { #( space, tuple [,flags], cb )
	my $self = shift;
	eval {
		my $proc   = shift;
		my $tuple = shift;
		my $opts = shift;
		my $informat  = ref $opts eq 'HASH' ? $opts->{in} : '';
		my $outformat = ref $opts eq 'HASH' ? $opts->{out} : '';
		my $flags = _hash_flags_to_mask( $opts );
		
		# req_id, ns, idx, offset, limit, keys, [ format ]
		my $pk = Protocol::Tarantool::lua(
			++$self->{seq},
			$flags,
			$proc,
			$tuple,
			$informat,
#			$format,
		);
		#warn "Created lua $proc. id=$self->{seq}: \n".xd $pk;
		my $wr = syswrite($self->{fh}, $pk );
		#warn "written $wr $!";
		my $read = read $self->{fh},my $buf, 12;
		$read == 12 or die "read failed: $!";
		my $need = Protocol::Tarantool::peek_size( \$buf );
		#warn "Read header, need +$need";
		my $read = read $self->{fh},$buf, $need, 12;
		$read == $need or die "read failed: $!";
		#warn join ' ',unpack '(H2)*',$buf;
		my $r = Protocol::Tarantool::response(\$buf, $outformat ? ($outformat) : ( ));
		#warn Data::Dumper::Dumper $r;
		if ($r->{code} == 0) {
			return $r->{tuples};
		} else {
			#warn Data::Dumper::Dumper $r;
			die "$r->{code}: $r->{status}";
		}
	1} or do {
		die $@;
	};
}

sub luado : method { #( code, [,flags] cb )
	my $self = shift;
	#eval {
		my $code = shift;
		my $opts = shift;
		my $informat  = ref $opts eq 'HASH' ? $opts->{in} : '';
		my $outformat = ref $opts eq 'HASH' ? $opts->{out} : '';
		my $flags = _hash_flags_to_mask( $opts );
		
		# req_id, ns, idx, offset, limit, keys, [ format ]
		my $pk = Protocol::Tarantool::lua(
			++$self->{seq},
			$flags,
			"box.dostring",
			[$code],
			$informat,
#			$format,
		);
		my $wr = syswrite($self->{fh}, $pk );
		#warn "written $wr $!";
		my $read = read $self->{fh},my $buf, 12;
		$read == 12 or die "read failed: $!";
		my $need = Protocol::Tarantool::peek_size( \$buf );
		#warn "Read header, need +$need";
		my $read = read $self->{fh},$buf, $need, 12;
		$read == $need or die "read failed: $!";
		#warn join ' ',unpack '(H2)*',$buf;
		my $r = Protocol::Tarantool::response(\$buf, $outformat ? ($outformat) : ( ));
		#warn Data::Dumper::Dumper $r;
		if ($r->{code} == 0) {
			return if $r->{count} == 0;
			my @t = @{$r->{tuples}[0]};
			return @t;
		} else {
			#warn Data::Dumper::Dumper $r;
			die "$r->{code}: $r->{status}: $r->{errstr}";
		}
	#1} or do {
	#	die $@;
	#};
}



1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

Client::Tarantool - Perl extension for blah blah blah

=head1 SYNOPSIS

  use Client::Tarantool;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for Client::Tarantool, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

Mons Anderson, E<lt>mons@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013 by Mons Anderson

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.16.1 or,
at your option, any later version of Perl 5 you may have available.


=cut
