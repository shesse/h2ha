/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.net.SocketException;

import org.apache.log4j.Logger;

/**
 *
 * @author sth
 */
public class ReplicationProtocolReceiver
extends Thread
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(ReplicationProtocolReceiver.class);
    
    /** */
    private ReplicationProtocolInstance sender;
    
    /** */
    private Socket socket;
    
    /** */
    private String instanceName;
   
    /** */
    private volatile boolean terminationRequested = false;
    

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @throws IOException 
     */
    public ReplicationProtocolReceiver(ReplicationProtocolInstance sender, Socket socket) throws IOException
    {
        super("replClientRecv");
        log.debug("ReplicationProtocolReceiver()");

        this.sender = sender;
        this.socket = socket;
        
        instanceName="recv:"+sender.getInstanceName();
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
            
        } catch (SocketException x) {
            if (x.getMessage().contains("reset")) {
        	// treat like EOF
            } else {
        	log.warn(instanceName+": caught socket exception on replication connection: "+x.getMessage());
            }
            
        } catch (EOFException x) {
            
        } catch (Throwable x) {
            log.fatal(instanceName+": unexpected error within replication client receiver", x);

        } finally {
            log.info(instanceName+": got end of connection");
            sender.terminate();
        }
    }
    
    /**
     * @throws ClassNotFoundException 
     * @throws IOException 
     * 
     */
    private void body() throws IOException, ClassNotFoundException
    {
        ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
        Object messageObject;
        while (!terminationRequested && (messageObject = ois.readObject()) != null) {
            sender.processReceivedMessage(messageObject);
        }
    }
    
    /**
     * 
     */
    public void terminate()
    {
        terminationRequested = true;
        try {
            socket.close();
        } catch (IOException x) {
        }
    }
    

    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
