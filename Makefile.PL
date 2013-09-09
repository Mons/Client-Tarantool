use 5.008008;
use ExtUtils::MakeMaker;
WriteMakefile(
    NAME              => 'Client::Tarantool',
    VERSION_FROM      => 'lib/Client/Tarantool.pm',
    PREREQ_PM         => {},
    ABSTRACT_FROM  => 'lib/Client/Tarantool.pm',
    AUTHOR         => 'Mons Anderson <mons@cpan.org>',
);
