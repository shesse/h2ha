/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import javax.net.ServerSocketFactory;

import org.apache.log4j.Logger;

/**
 *
 * @author sth
 */
public class ReplicationServer
    extends Thread
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(ReplicationServer.class);
    
    /** */
    private H2HaServer haServer;
    
    /** */
    private FileSystemHa fileSystem;

    /** */
    private int listenPort = 8234;
    
    /** */
    private InetAddress peerRestriction = null;

    /** */
    private int maxQueueSize = 5000; 
    
    /** */
    private long maxEnqueueWait = 60000;
    
    /** */
    private int maxWaitingMessages = 0;
    
    /** */
    private long statisticsInterval = 300000;
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
	/**
     */
	public ReplicationServer(H2HaServer haServer, FileSystemHa fileSystem, String[] args)
	{
		super("ReplicationServer");

		log.debug("ReplicationServer()");

		this.haServer = haServer;
		this.fileSystem = fileSystem;
		boolean restrictPeer = false;
		String peerHost = null;
		
		for (int i = 0; i < args.length - 1; i++) {
			if (args[i].equals("-haListenPort")) {
				try {
					listenPort = Integer.parseInt(args[i + 1]);
				} catch (NumberFormatException x) {
					log.error("inhalid haListenPort: " + x);
				}

			} else if (args[i].equals("-haPeerHost")) {
				peerHost = args[++i];

			} else if (args[i].equals("-haRestrictPeer")) {
				restrictPeer = true;

			} else if (args[i].equals("-haMaxQueueSize")) {
				try {
					maxQueueSize = Integer.parseInt(args[i + 1]);
				} catch (NumberFormatException x) {
					log.error("inhalid haMaxQueueSize: " + x);
				}

			} else if (args[i].equals("-haMaxEnqueueWait")) {
				try {
					maxEnqueueWait = Long.parseLong(args[i + 1]);
				} catch (NumberFormatException x) {
					log.error("inhalid haMaxEnqueueWait: " + x);
				}

			} else if (args[i].equals("-haMaxWaitingMessages")) {
				try {
					maxWaitingMessages = Integer.parseInt(args[i + 1]);
				} catch (NumberFormatException x) {
					log.error("inhalid haMaxWaitingMessages: " + x);
				}

			} else if (args[i].equals("-statisticsInterval")) {
				try {
					statisticsInterval = Integer.parseInt(args[i + 1]);
				} catch (NumberFormatException x) {
					log.error("inhalid statisticsInterval: " + x);
				}
			}
		}
		
		if (restrictPeer && peerHost != null) {
			try {
				peerRestriction = InetAddress.getByName(peerHost);
				
			} catch (UnknownHostException x) {
				log.error("unknown host name: "+peerHost);
				System.exit(1);
			}
		}
	}

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * 
     */
    public void run()
    {
        try {
            body();
        } catch (Throwable x) {
            log.error("caught unexpected exception within ReplicationServer", x);
        }
        log.debug("replication server terminated");
    }
    
	/**
	 * @throws IOException
	 */
	private void body()
		throws IOException
	{
		log.debug("replication server instance has been started");

		ServerSocket serverSocket = ServerSocketFactory.getDefault().createServerSocket();
		serverSocket.setReuseAddress(true);

		serverSocket.bind(new InetSocketAddress(listenPort));

		log.info("ready to accept replication connections on port " + listenPort);
		while (!serverSocket.isClosed()) {
			Socket connSocket = serverSocket.accept();

			SocketAddress remoteAddress = connSocket.getRemoteSocketAddress();
			if (peerRestriction == null ||
				(remoteAddress instanceof InetSocketAddress && peerRestriction.equals(((InetSocketAddress) remoteAddress).getAddress()))) {
				log.debug("accepted incoming replication connection");
				String instanceName = "replServer-" + String.valueOf(remoteAddress);
				new Thread(new ReplicationServerInstance(instanceName, maxQueueSize,
					maxEnqueueWait, maxWaitingMessages, statisticsInterval, haServer, fileSystem,
					connSocket), "ha-server-conn").start();

			} else {
				log.warn("rejected incoming HA connection from invalid address "+remoteAddress);
				try {
					connSocket.close();
				} catch (IOException x) {
				}
			}

		}
	}

    /**
     * @return the fileSystem
     */
    public FileSystemHa getFileSystem()
    {
        return fileSystem;
    }

    /**
     * 
     */
    public int getListenPort()
    {
	return listenPort;
    }
    


    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
