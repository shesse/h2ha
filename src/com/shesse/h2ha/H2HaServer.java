/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
    private String[] args;
    
    /** */
    private FileSystemHa fileSystem;
    
    /** */
    private ReplicationServer server;

    /** */
    private Server tcpDatabaseServer;
    
    /** */
    private String peerHost = null;
    
    /** */
    private String haBaseDir = null;
    
    /** */
    private int masterPriority = 10;
    
    /** */
    private ReplicationClientInstance client = null;
    
    /** */
    private ReplicationServerInstance[] servers = new ReplicationServerInstance[0];
    
    /** */
    private BlockingQueue<Runnable> controlQueue = new LinkedBlockingQueue<Runnable>();
    
    /** */
    private volatile boolean shutdownRequested = false;
    
    /** */
    private String uuid = UUID.randomUUID().toString();
    
    /** */
    private Properties hafsm = new Properties();

    /** */
    public enum FailoverState { //
	INITIAL, //
	STARTING_STANDALONE, //
	MASTER_STANDALONE, //
	IDLE, //
	STARTING, //
	MASTER, //
	STOPPING, //
	TRANSFERING, //
	TRANSFERED, //
	SLAVE_SYNCING, //
	SLAVE, //
	SLAVE_STOPPING, //
    };

    /** */
    public enum Event { //
	HA_STARTUP, //
	NO_PEER, // we don't have a peer 
	MASTER_STARTED, //
	MASTER_STOPPED, //
	CONNECTED_TO_PEER, //
	CANNOT_CONNECT, // parameter is: indication if local data is valid for a DB
	SYNC_COMPLETED, //
	PEER_STATE, // parameter is: state of HA peer
	DISCONNECTED, //
	TRANSFER_MASTER, //
	SLAVE_STOPPED, //
    };

    /** */
    private volatile FailoverState failoverState = FailoverState.INITIAL;
    
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public H2HaServer(String[] args)
    {
        log.debug("H2HaServer()");
        
        this.args = args;
        
        for (int i = 0; i < args.length-1; i++) {
            if (args[i].equals("-haPeerHost")) {
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

        InputStream fsmStream = getClass().getResourceAsStream("hafsm.properties");
        if (fsmStream == null) {
            throw new IllegalStateException("cannot find hafsm.properties");
        }
        
        try {
	    hafsm.load(fsmStream);
	} catch (IOException e) {
            throw new IllegalStateException("cannot read hafsm.properties", e);
	}
	
	for (String key: hafsm.stringPropertyNames()) {
	    String[] trans = hafsm.getProperty(key, "").split("\\s+");
	    if (trans.length != 2) {
		throw new IllegalStateException("invalid FSM transition for "+key);
	    }
	    
	    try {
		getAction(trans[0]);
	    } catch (NoSuchMethodException x) {
		throw new IllegalStateException("unknown action in FSM transition for "+key);
	    }
	    

	    try {
		FailoverState.valueOf(trans[1]);
	    } catch (IllegalArgumentException x) {
		throw new IllegalStateException("unknown target state in FSM transition for "+key);
	    }
	}
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
	try {
	    new H2HaServer(args).run();
	    
	} catch (Throwable x) {
	    log.fatal("unexepected exception within HA server main thread", x);
	    System.exit(1);
	}
    }

    /**
     * @throws InterruptedException 
     * 
     */
    private void run() 
    throws InterruptedException
    {
        
        fileSystem = new FileSystemHa(this, args);
        server = new ReplicationServer(this, fileSystem, args);
        server.start();
        
        if (peerHost == null) {
            log.warn("no haPeerHost specified - running in master only mode!");
            applyEvent(Event.NO_PEER, null);
            
        } else {
            applyEvent(Event.HA_STARTUP, null);
        }
        
        while (!shutdownRequested) {
            Runnable queueEntry = controlQueue.take();
            queueEntry.run();
        }
    }
    
    /**
     * 
     */
    private void enqueue(Runnable queueEntry)
    {
	try {
	    controlQueue.put(queueEntry);
	} catch (InterruptedException x) {
	    log.error("InterruptedException", x);
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
            throw new IllegalStateException("did not get a FileSystemHa for url ha://");
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
            throw new IllegalStateException("did not get a FileSystemHa for url ha://");
        }
    }
    
    /**
     * must be called on the actual master system
     * @throws SQLException 
     */
    public static void transferMasterRole()
    throws SQLException
    {
	FileSystem fs = FileSystem.getInstance("ha://");
        if (fs instanceof FileSystemHa) {
            ((FileSystemHa)fs).getHaServer().transferMasterRoleImpl();
        } else {
            throw new IllegalStateException("did not get a FileSystemHa for url ha://");
        }
    }
    
    /**
     * @throws SQLException 
     * 
     */
    private void transferMasterRoleImpl()
    throws SQLException
    {
	if (failoverState != FailoverState.MASTER) {
	    throw new SQLException("master role can only be transferred from an active master");
	}
	
	if (client == null) {
	    throw new SQLException("master role can only be transferred from within a failover configuration");
	}
	
	applyEvent(Event.TRANSFER_MASTER, null);
    }


    /**
     * computes deterministic role assignment depending on
     * current local role, local and remote master priority and
     * local and remote UUID.
     * @return true if the local system is the configured master system
     */
    public boolean weAreConfiguredMaster(int otherMasterPriority, String otherUuid)
    {
	if (!client.isConsistentData()) {
	    return false;
	}
	
	boolean localWins;
	if (masterPriority > otherMasterPriority) {
	    localWins = true;

	} else if (masterPriority < otherMasterPriority) {
	    localWins = false;

	} else {
	    int cmp = uuid.compareTo(otherUuid);
	    if (cmp < 0) {
		localWins = true;

	    } else {
		// UUIDs should never be equal
		localWins = false;
	    }
	}

	return localWins;
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
    public FailoverState getFailoverState()
    {
	return failoverState;
    }
    
    
    /**
     * @param server
     */
    public synchronized void registerServer(ReplicationServerInstance server)
    {
	for (ReplicationProtocolInstance r : servers) {
	    if (r == server) {
		return;
	    }
	}

	ReplicationServerInstance[] newServers =
	    new ReplicationServerInstance[servers.length + 1];
	System.arraycopy(servers, 0, newServers, 0, servers.length);
	newServers[servers.length] = server;
	servers = newServers;
    }


    /**
     * @param server
     */
    public void deregisterServer(ReplicationProtocolInstance server)
    {
	for (int i = 0; i < servers.length; i++) {
	    if (servers[i] == server) {
		ReplicationServerInstance[] newServers =
		    new ReplicationServerInstance[servers.length - 1];
		System.arraycopy(servers, 0, newServers, 0, i);
		System.arraycopy(servers, i + 1, newServers, i, newServers.length - i);
		servers = newServers;
		return;
	    }
	}
    }


    /**
     * 
     */
    public void applyEvent(final Event event, final Object parameter)
    {
	enqueue(new Runnable(){
	    public void run()
	    {
		applyEventImpl(event, parameter);
	    }
	});
    }
    
    /**
     * 
     */
    private void applyEventImpl(Event event, Object parameter)
    {
	log.debug("applyEventImpl "+event+", param="+parameter);
	String eventKey = String.valueOf(event);
	
	if (parameter != null) {
	    eventKey += "."+parameter;
	}
	
	String key = failoverState+"."+eventKey;
	
	
	String transition = hafsm.getProperty(key);
	if (transition == null) {
	    throw new IllegalStateException("cannot find FSM entry for '"+key+"'");
	}
	
	log.debug("transition lookup: "+key+" -> "+transition);

	String[] transitionParts = transition.split("\\s+");
	if (transitionParts.length != 2) {
	    throw new IllegalStateException("not a valid transition for '"+key+"': "+transition);
	}
	
	String actionName = transitionParts[0];
	String newStateName = transitionParts[1];
	
	FailoverState newState = FailoverState.valueOf(newStateName);
	FailoverState oldState = failoverState;
	if (newState != oldState) {
	    
	    log.info("changing state from "+oldState+" to "+newState+" (event was "+eventKey+")");
	    failoverState = newState;
	    
	    try {
		Method action = getAction(actionName);
		action.invoke(this, oldState, event, newState, parameter);
		
	    } catch (IllegalArgumentException x) {
		throw new IllegalStateException("illegal argument for FSM action '"+actionName+"'", x);

	    } catch (IllegalAccessException x) {
		throw new IllegalStateException("illegal access for FSM action '"+actionName+"'", x);

	    } catch (InvocationTargetException x) {
		throw new IllegalStateException("caught exception within FSM action '"+actionName+"'", x.getCause());

	    } catch (SecurityException x) {
		throw new IllegalStateException("security exception for FSM action '"+actionName+"'", x);

	    } catch (NoSuchMethodException x) {
		throw new IllegalStateException("could not find action '"+actionName+"' for FSM transition '"+key+"'");

	    }
	    
	    if (client != null) {
		try {
		    client.sendStatus();
		} catch (IOException e) {
		}
	    }
	    
	    ReplicationServerInstance[] sdup = servers;
	    for (ReplicationServerInstance server: sdup) {
		server.enqueue(new ReplicationMessage() {
			private static final long serialVersionUID = 1L;

			@Override
			protected void process(ReplicationProtocolInstance instance)
			throws Exception
			{
			    if (instance instanceof ServerSideProtocolInstance) {
				((ServerSideProtocolInstance)instance).sendStatus();
			    }
			}

			@Override
			public int getSizeEstimate()
			{
			    return 4;
			}

			@Override
			public String toString()
			{
			    return "send hb";
			}
		    }
		);
	    }
	}
    }
    
    /**
     * @throws NoSuchMethodException 
     * 
     */
    private Method getAction(String actionName)
    throws NoSuchMethodException
    {
	try {
	    return getClass().getMethod(actionName, FailoverState.class, Event.class, FailoverState.class, Object.class);

	} catch (SecurityException x) {
	    throw new IllegalStateException("security exception for FSM action '"+actionName+"'", x);

	}
    }
    
    /**
     * 
     */
    public void noAction(FailoverState oldState, Event event, FailoverState newState, Object parameter)
    {
    }
    
    /**
     * 
     */
    public void fatalError(FailoverState oldState, Event event, FailoverState newState, Object parameter)
    {
	log.fatal("invalid state / event combination: "+oldState+" / "+event);
	System.exit(1);
    }
    
    /**
     * 
     */
    public void logUnexpected(FailoverState oldState, Event event, FailoverState newState, Object parameter)
    {
	if (parameter == null) {
	    log.warn("unexpected state / event combination: "+oldState+" / "+event);
	} else {
	    log.warn("unexpected state / event combination: "+oldState+" / "+event+"("+parameter+")");
	}
    }
    
    /**
     * 
     */
    public void startHaClient(FailoverState oldState, Event event, FailoverState newState, Object parameter)
    {
        client = new ReplicationClientInstance(this, fileSystem, args);
        new Thread(client, "ReplicationClient").start(); 
    }

    /**
     * 
     */
    public void startDbServer(FailoverState oldState, Event event, FailoverState newState, Object parameter)
    {
	try {
	    tcpDatabaseServer = Server.createTcpServer(args).start();
	    log.info("DB server is ready to accept connections");
	    applyEvent(Event.MASTER_STARTED, null);

	} catch (SQLException x) {
	    log.error("SQLException when starting database server", x);
	    System.exit(1);
	}
    }

    /**
     * 
     */
    public void stopDbServer(FailoverState oldState, Event event, FailoverState newState, Object parameter)
    {
	log.info("shutting down DB server");
	tcpDatabaseServer.stop();
	applyEvent(Event.MASTER_STOPPED, null);
    }

    /**
     * 
     */
    public void sendListFilesRequest(FailoverState oldState, Event event, FailoverState newState, Object parameter)
    {
	client.sendListFilesRequest();
    }
    
    
    /**
     * 
     */
    public void sendStopReplicationRequest(FailoverState oldState, Event event, FailoverState newState, Object parameter)
    {
	client.sendStopReplicationRequest();
    }
    
    
    /**
     * 
     */
    public void issueConnEvent(FailoverState oldState, Event event, FailoverState newState, Object parameter)
    {
	client.issueConnEvent();
    }
    
    
    /**
     * 
     */
    public void issuePeerEvent(FailoverState oldState, Event event, FailoverState newState, Object parameter)
    {
	client.issuePeerEvent();
    }
    
    
    /**
     * 
     */
    public boolean isActive()
    {
	return failoverState == FailoverState.MASTER_STANDALONE ||
	    failoverState == FailoverState.MASTER || failoverState == FailoverState.SLAVE;
    }

    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
