/**
 * H2 database server with file system replication to provide high availability.
 * 
 * This server uses H2 database code without any modifications. It registers
 * the file path prefix ha:// and replicates all DB files created
 * within this file system and all modifications to it.
 * <p>
 * The implementation builds a virtual file system using the class
 * FileSystemHa and provides access to it using FilePathHa, which 
 * is derived from H2's FilePath base class.
 * <p>
 * Server startup and control code is located within H2HaServer. It creates
 * a FileSystemHa and an instance of ReplicationServer to listen for
 * incoming control connections of other replicas of the same database 
 * (design allows multiple replicas but in practice it will only be one).
 * The ReplicationServer will create a new instance of ReplicationServerInstance
 * for each incoming connection.
 * <p>
 * H2HaServer will maintain a control queue and read state change events
 * from it. All state change events are applied to a finite state machine
 * to decide an the actions and state transitions (see hafsm.properties).
 * State change events may come from local sources or via communication 
 * channel from a replica. 
 * <p>
 * Upon start, the FSM calls startHaClient, which will create a ReplicationClientInstance
 * to contact the peer replica. Depending on the availability of a peer and
 * its current state, the FSM will decide on the role
 * of this server. The ReplicationClientInstance stays in contact with the 
 * peer and changes current state as needed. When is decides that 
 * the current instance must become an active database server, it 
 * starts an H2 instance. At other times it may decide that the 
 * current instance may no longer be active. In this
 * case, it stops the H2 database instance.
 * <p>
 * The described mechanism of communication with a H2HA server may not
 * only be used to let the replicas talk to each other. It can also be 
 * used for other communication needs. Currently there exists the
 * class ReplicationServerStatus which implements a remote query for server 
 * status. It is only used within the JUnit tests.
 *
 * @author Stephan Hesse
 */
package com.shesse.h2ha;