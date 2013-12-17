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
import java.util.List;

import javax.net.ServerSocketFactory;

import org.apache.log4j.Logger;

/**
 * Listens for incoming control connections from other replicas. It creates a
 * new instance of ReplicationServerInstance for each such connection.
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
	public ReplicationServer(H2HaServer haServer, FileSystemHa fileSystem, List<String> args)
	{
		super("ReplicationServer");

		log.debug("ReplicationServer()");

		this.haServer = haServer;
		this.fileSystem = fileSystem;
		boolean restrictPeer = false;
		String peerHost = null;

		listenPort = H2HaServer.findOptionWithInt(args, "-haListenPort", 8234);
		peerHost = H2HaServer.findOptionWithValue(args, "-haPeerHost", null);
		restrictPeer = H2HaServer.findOption(args, "-haRestrictPeer");
		maxQueueSize = H2HaServer.findOptionWithInt(args, "-haMaxQueueSize", 5000);
		maxEnqueueWait = H2HaServer.findOptionWithInt(args, "-haMaxEnqueueWait", 60000);
		maxWaitingMessages = H2HaServer.findOptionWithInt(args, "-haMaxWaitingMessages", 0);
		statisticsInterval = H2HaServer.findOptionWithInt(args, "-statisticsInterval", 300000);

		if (restrictPeer) {
			String restrictHost = peerHost;
			if (restrictHost == null) {
				restrictHost = "127.0.0.1";
			}

			try {
				peerRestriction = InetAddress.getByName(restrictHost);

			} catch (UnknownHostException x) {
				log.error("unknown host name: " + restrictHost);
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
				log.warn("rejected incoming HA connection from invalid address " + remoteAddress);
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
