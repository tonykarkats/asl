Advanced Systems Lab HS16 Project
=================================

* Compile with : ant (Java 1.7)
* JAR file will be available at dist/

System Description
------------------
A distributed middleware system for the memcached key-value store.
The middleware accepts client connections, load balances the requests across the
memcached server backends using consistent hashing and then sends the responses back to
the clients. GET, SET and DELETE requests are supported and the SETs/DELETEs can also
be carried out with replication across the requested number of servers for improved
fault-tolerance.


Full Description PDF
--------------------
http://systems.ethz.ch/sites/default/files/file/asl2016/project.pdf
