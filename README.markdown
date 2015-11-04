One of the problems with the near perfect SpyMemcached is the lack of UDP support. 
As Facebook mentions in their 2013 NSDI paper, it might be desirable to issue GETs using UDP as TCP will not scale for excessively large number of transient connections.
This slightly modified version of Spy works with UDP, at least it's goal is to make it work with UDP.

The refactor is absolutely going to be frown upon by Software Engineering people, but is indeed a quick dirty hack to make it work. I call it Half-Lifting (maybe there is another proper term of this).

This code is by no means production quality and is for experimental purposes only.

# Limitaion of the original Spy:

The tight coupling of SocketChannel with MemcachedNode which in turns couples with pretty much everything else is the culprit for the difficulty in performing a proper refactoring.

In fact, had the authors used a composition pattern in MemcachedNode (decouple it again with SocketChannel), and only use part of it as parameter to other part of the world, it would be much easier to extend it.

# Changes

In order to achieve the effect using minimal effort, the fllowing changes are made:

`DefaultConnectionFactory` is subclassed by `DefaultUDPSudoConnectionFactory` which simply overrides the factory method `createMemcachedNode` to produce UDP nodes `AsciiUDPMemcachedNodeImpl`, instead of TCP nodes. The factory method `createConnection` is also overriden to produce `MemcachedUDPSudoConnection`, which is (you might have guessed) a subclass of `MemcachedConnection`. In general, we should have lifted `MemcachedConnection` and `DefaultUDPSudoConnectionFactory` to two base classes and go from there, but we prefer this half-lifting because it is faster, although it is very confusing.

In order to make this work, we need to decouple the `SocketChanel` from being created by the connections (you cannot hide or run). This is done by changing all signatures of `SocketChannel` to `AbstractSelectableChannel`. Then in the now parent class any reference to this variable will perform a cast to `SocketChannel` first. All methods containing such accesses are overriden in the now child class (UDP-related classes).

We do this to `TCPMemcachedNodeImpl` too. In order to keep the naming more consistent (the authors made no reference to TCP at all but in this only class), we also renamed `TCPMemcachedNodeImpl` to `MemcachedNodeImpl`, then a subclass of it is created and named `UDPMemcachedNodeImpl`.

We followed the authors flow and created `AsciiUDPMemcachedNode` and is concrete. It will be trivial to create a binary version of that based on the original code.

Since `Tap` protocol is not supported in the vanila memcached, we removed it from all instances together with all tests.



# Building

Spymemcached can be compiled using Apache Ant by running the following
command:

    ant

This will generate binary, source, and javadoc jars in the build
directory of the project.

To run the Spymemcached tests against Membase Server run the
following command:

    ant test -Dserver.type=membase

To test Spymemcached against Membase running on a different host
use the following command:

    ant test -Dserver.type=membase \
        -Dserver.address_v4=ip_address_of_membase

# Testing

The latest version of spymemcached has a set of command line arguments
that can be used to configure the location of your testing server. The
arguments are listed below.

    -Dserver.address_v4=ipv4_address_of_testing_server

This argument is used to specify the ipv4 address of your testing
server. By default it is set to localhost.

    -Dserver.address_v6=ipv6_address_of_testing_server

This argument is used to set the ipv6 address of your testing server.
By default it is set to ::1. If an ipv6 address is specified then an
ipv4 address must be specified otherwise there may be test failures.

    -Dserver.port_number=port_number_of_memcached

This argument is used when memcahched is started on a port other than
11211

    -Dtest.type=ci

This argument is used for CI testing where certain unit tests might
be temporarily failing.

# More Information

For more information about Spymemcached see the links below:

## Project Page The

[Spymemcached Project Home](http://code.google.com/p/spymemcached/)
contains a wiki, issue tracker, and downloads section.

## Github

[The gitub page](http://github.com/dustin/java-memcached-client)
contains the latest Spymemcached source.

## Couchbase.org

At [couchbase.org](http://www.couchbase.org/code/couchbase/java) you
can find a download's section for the latest release as well as an
extensive tutorial to help new users learn how to use Spymemcached.
