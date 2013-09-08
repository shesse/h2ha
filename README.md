h2ha
====

This is a H2 database server (see h2database.com) extended
by a high availability solution.

High availability is realized by implementing a new pluggable
file system module for the H2 database. This module duplicates 
all database files to a second machine and forwards all updates
to the mirror system.

A control logic based on UDP heartbeating controls activation and
deactivation of the H2 database server on the two machines. A
server will be started on the secondary when the primary fails.


Note: in some cases it was necessary to decide for a trade-off 
between automatic operation and the potential of loss of
data of a short time period. In these cases automatic 
operation and good performance where favored. That means, that
you should not use this solution if it is essential for you
not to loose any commited data.

These are the cases where data loss can occur:

- forwarding data from primary to secondary system is done
  asynchronously. A transaction commit on the primary takes
  a short time (usually significantly less than a second) before
  it is recorded on the secondary. If the primary or the network
  fails during this time, the transaction may be lost.

- h2ha does an automatic recovery after a network partitioning
  occurs. When the network is partitioned in a way so that primary
  and secondary cannot see each other, each of them will become active
  and execute transactions. When resolving such a situation after
  network connectivity has been re-established, h2ha will discard
  the changes done within one of the instances and continue working
  with the other.
  

The software is currently based on H2 version 1.3.158. This version
is used without any changes.

It consists of 3 parts:

- com.shesse.h2ha
  The server software

- com.shesse.jdbcproxy
  A JDBC driver that acts as a proxy to two h2ha JDBC driver instances:
  one that connects to the primary and one that connects to the secondary.
  The second one is only used when the first one encounters a connection
  error.
  
- com.shesse.dbdup
  Not strictly a part of H2HA. It is contained for historical reasons
  and contains code for an application level duplication of a DB.
  This code can be used for migrating already existing data from another 
  DB (e.g. MySQL) to H2
  



Building
========

h2ha uses ant as build tool. Typing "ant" without any arguments will
construct dist/h2ha.jar. This is the server packed as an executable
jar file.

You may start it using java -jar dist/h2ha.jar [arguments]

A start without any arguments will give a usage message detailing
the possible arguments.




