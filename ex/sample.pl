#!/usr/bin/env perl

use strict;
use lib '../lib';
use Client::Tarantool;
use Data::Dumper;

my $t = Client::Tarantool->new(
	server => '0.0.0.0:33013',
	spaces => {
		0 => {
			name => 'test',
			fields => [qw(id name)],
			types  => [qw(INT STR)],
			indexes => {
				0 => {
					name => 'primary',
					fields => ['id'],
				}
			}
		}
	}
);

my ($version,$lsn) = $t->luado('return { box.info.version, box.info.lsn }', { out => 'pl' });
print $version,"\n";
print $lsn,"\n";
my $res = $t->select(0,[[1]]);
warn Dumper $res;