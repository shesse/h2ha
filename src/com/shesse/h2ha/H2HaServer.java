/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.sql.SQLException;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.h2.store.fs.FileSystem;
import org.h2.tools.Server;

/**
 *
 * @author sth
 */
public class H2HaServer
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(H2HaServer.class);
    
    /** */
    private FileSystemHa fileSystem;
    
    /** */
    private ReplicationServer server;

    /** */
    @SuppressWarnings("unused")
    private Server tcpDatabaseServer;
    
    /** */
    private int maxConnectRetries = 5;
    
    /** */
    private String peerHost = null;
    
    /** */
    private String haBaseDir = null;
    
    /** */
    private enum Role { NEGOTIATING, MASTER, SLAVE };
    
    /** */
    private volatile Role role = Role.NEGOTIATING;
    
    /** */
    private int masterPriority = 10;
    
    /** */
    private String uuid = UUID.randomUUID().toString();
    
    /** */
    private enum State { //
        INITIAL, //
        CONNECTING, //
        SLAVE, //
        SLAVE_SYNC, //
        LOST_MASTER, //
        STARTING_AS_MASTER, //
        MASTER, //
        WAITING_CONNECT_RETRY,//
    };
    
    
    /** */
    private volatile State state = State.INITIAL;
    
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public H2HaServer()
    {
        log.debug("H2HaServer()");
    }


    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @param args
     * @throws InterruptedException 
     */
    public static void main(String[] args) 
    throws InterruptedException
    {
        new H2HaServer().run(args);
    }

    /**
     * @throws InterruptedException 
     * 
     */
    private void run(String[] args) 
    throws InterruptedException
    {
        for (int i = 0; i < args.length-1; i++) {
            if (args[i].equals("-connectRetry")) {
                try {
                    maxConnectRetries = Integer.parseInt(args[++i]);
                } catch (NumberFormatException x) {
                    log.error("inhalid connectRetry: "+x);
                }
            } else if (args[i].equals("-haPeerHost")) {
                peerHost = args[++i];
                
            } else if (args[i].equals("-haBaseDir")) {
                haBaseDir = args[++i];
                 
            } else if (args[i].equals("-masterPriority")) {
                try {
                    masterPriority = Integer.parseInt(args[++i]);
                } catch (NumberFormatException x) {
                    log.error("inhalid masterPriority: "+x);
                }
            }
        }        

        if (haBaseDir == null) {
            System.err.println("mandatory flag -haBaseDir is missing");
            System.exit(1);
        }
        
        fileSystem = new FileSystemHa(args);
        server = new ReplicationServer(this, fileSystem, args);
        server.start();
        
        if (peerHost == null) {
            log.warn("no haPeerHost specified - running in master only mode!");
            role = Role.MASTER;
            state = State.STARTING_AS_MASTER;
            
        } else {
 
            for (;;) {
        	ReplicationClientInstance client = new ReplicationClientInstance(this, fileSystem, args);
        	
                int retryCount = 0;
                role = Role.NEGOTIATING;
                while (retryCount < maxConnectRetries && !client.isConnected()) {
                    state = State.CONNECTING;
                    if (client.tryToConnect()) {
                        break;
                    }
                    retryCount++;

                    try {
                        state = State.WAITING_CONNECT_RETRY;
                        Thread.sleep(500);
                    } catch (InterruptedException x) {
                    }
               }

                if (client.isConnected()) {
                    state = State.SLAVE;
                    client.run();
                    if (role == Role.MASTER) break;
                    log.info("slave mode has ended - verifying that peer is down");
                    state = State.LOST_MASTER;
                    
                } else {
                    if (client.isConsistentData()) {
                        log.info("could not contact peer - we will become master!");
                        role = Role.MASTER;
                        state = State.STARTING_AS_MASTER;
                        break;
                        
                    } else {
                        log.warn("cannot establish slave mode - but our database is unusable");
                        log.warn("... keep waiting for a master");
                        state = State.WAITING_CONNECT_RETRY;
                        try {
                            Thread.sleep(20000);
                        } catch (InterruptedException x) {
                        }
                   }
                }
            }
        }
        
        // when program flow reaches this line, we have determined that
        // there is no other master and we need to enter master mode.
        try {
            tcpDatabaseServer = Server.createTcpServer(args).start();
            state = State.MASTER;
            log.info("DB server is ready to accept connections");
            
        } catch (SQLException x) {
            log.error("SQLException when starting database server", x);
            System.exit(1);
        }

    }

    /**
     * may be called when running as a master to ensure that
     * all outstanding changes have been sent to all clients
     */
    public static void pushToAllReplicators()
    {
        FileSystem fs = FileSystem.getInstance("ha://");
        if (fs instanceof FileSystemHa) {
            ((FileSystemHa)fs).flushAll();
        } else {
            throw new IllegalStateException("did not get a FileSystemHa for url)ha://");
        }
    }
    
    /**
     * may be called when running as a master to ensure that
     * all outstanding changes have been received by all clients
     */
    public static void syncWithAllReplicators()
    {
        log.debug("syncWithAllReplicators has been called");
        FileSystem fs = FileSystem.getInstance("ha://");
        if (fs instanceof FileSystemHa) {
            ((FileSystemHa)fs).syncAll();
        } else {
            throw new IllegalStateException("did not get a FileSystemHa for url)ha://");
        }
    }
    
    /**
     * computes deterministic role assignment depending on
     * current local role, local and remote master priority and
     * local and remote UUID.
     * @return true if negotiation results in a master role for the
     * local system
     */
    public synchronized boolean negotiateMasterRole(int otherMasterPriority, String otherUuid)
    {
        if (role == Role.NEGOTIATING) {
            if (masterPriority > otherMasterPriority) {
                role = Role.MASTER;
                log.debug("our priority is higher - we become "+role);
                
            } else if (masterPriority < otherMasterPriority) {
                role = Role.SLAVE;
                log.debug("our priority is lower - we become "+role);
                
            } else {
                int cmp = uuid.compareTo(otherUuid);
                if (cmp < 0) {
                    role = Role.MASTER;
                    log.debug("our uuid is lower - we become "+role);
                    
                } else {
                    // UUIDs should never be equal
                    role = Role.SLAVE;
                    log.debug("our uuid is higher or equal - we become "+role);
                }
            }
        }
        
        if (role == Role.MASTER) {
            return true;
        } else {
            return false;
        }
    }
    
    /**
     * @return
     */
    public synchronized boolean tryToSetMasterRole()
    {
        if (role == Role.SLAVE) return false;
        
        role = Role.MASTER;
        return true;
    }
    
    /**
     * 
     */
    public void setSlaveRole()
    {
	role = Role.SLAVE;
    }


    /**
     * 
     */
    public void slaveSyncCompleted()
    {
	if (state == State.SLAVE) {
	    state = State.SLAVE_SYNC;
	    
	} else {
	    throw new IllegalStateException("got slaveSyncCompleted when in state "+state);
	}
    }


    /**
     * 
     */
    public int getMasterPriority()
    {
        return masterPriority;
    }
    
    /**
     * 
     */
    public String getUuid()
    {
        return uuid;
    }

    /**
     * 
     */
    public boolean isActive()
    {
        boolean result;
        if (role == Role.SLAVE && state == State.SLAVE_SYNC) {
            result = true;
        } else {
            result = (state == State.MASTER);
        }
        
        log.debug("isActive() role="+role+", state="+state+" -> "+result);
        return result;
    }

    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
