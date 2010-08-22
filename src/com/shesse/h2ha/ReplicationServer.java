/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
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
    private int maxWaitingMessages = 0;
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public ReplicationServer(H2HaServer haServer, FileSystemHa fileSystem, String[] args)
    {
        log.debug("ReplicationServer()");
        
        this.haServer = haServer;
        this.fileSystem = fileSystem;
        
        for (int i = 0; i < args.length-1; i++) {
            if (args[i].equals("-haListenPort")) {
                try {
                    listenPort = Integer.parseInt(args[i+1]);
                } catch (NumberFormatException x) {
                    log.error("inhalid haListenPort: "+x);
                }
                
            } else if (args[i].equals("-haMaxWaitingMessages")) {
                try {
                    maxWaitingMessages = Integer.parseInt(args[i+1]);
                } catch (NumberFormatException x) {
                    log.error("inhalid haMaxWaitingMessages: "+x);
                }
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
        
        log.info("ready to accept replication connections on port "+listenPort);
        while (!serverSocket.isClosed()) {
            Socket connSocket = serverSocket.accept();
            log.debug("accepted incoming replication connection");
            String instanceName = String.valueOf(connSocket.getRemoteSocketAddress());
            new Thread(new ReplicationServerInstance(instanceName, maxWaitingMessages, haServer, fileSystem, connSocket)).start();
            
        }
    }

    /**
     * @return the fileSystem
     */
    public FileSystemHa getFileSystem()
    {
        return fileSystem;
    }
    


    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
