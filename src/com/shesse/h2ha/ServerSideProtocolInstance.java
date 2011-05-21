/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;


import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.h2.store.fs.FileObject;

import com.shesse.h2ha.H2HaServer.FailoverState;


/**
 *
 * @author sth
 */
public abstract class ServerSideProtocolInstance 
extends ReplicationProtocolInstance
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(ServerSideProtocolInstance.class);

    /** */
    protected H2HaServer haServer;

    /** */
    protected FileSystemHa fileSystem;
    
    /** */
    private MessageDigest md5Digest;
    
    /** */
    private Map<String, FileObject> openFiles = new HashMap<String, FileObject>();
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public ServerSideProtocolInstance(String instanceName, int maxWaitingMessages, H2HaServer haServer, FileSystemHa fileSystem)
    {
	super(instanceName, maxWaitingMessages);
	
        this.haServer = haServer;
        this.fileSystem = fileSystem;

        try {
            md5Digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException x) {
            throw new IllegalStateException("cannot find MD5 algorithm", x);
        }
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * {@inheritDoc}
     *
     * @see com.shesse.h2ha.ReplicationProtocolInstance#getCurrentFailoverState()
     */
    @Override
    protected FailoverState getCurrentFailoverState()
    {
	return haServer.getFailoverState();
    }

    /**
     * @throws IOException 
     * 
     */
    protected void sendStatus()
    throws IOException
    {
	if (Thread.currentThread() == instanceThread) {
	    // send directly if within sender thread
	    sendToPeer(new StatusMessage(getCurrentFailoverState(), haServer.getMasterPriority(), haServer.getUuid()));

	} else {
	    // not in sender thread: enqueue message that will send a heartbeat
	    // when processed.
	    // The heartbeat will carry state information of the time when it
	    // is sent out.
	    messageQueue.add(new ReplicationMessage() {
		private static final long serialVersionUID = 1L;

		@Override
		protected void process(ReplicationProtocolInstance instance)
		throws Exception
		{
		    try {
			sendToPeer(new StatusMessage(getCurrentFailoverState(), haServer.getMasterPriority(), haServer.getUuid()));
		    } catch (IOException x) {
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
	    });
	}
    }


    /**
     * 
     */
    protected Set<FileInfo> discoverExistingFiles()
    {
        Set<FileInfo> existingFiles = new HashSet<FileInfo>();

        synchronized (fileSystem) {
            discoverFilesWithinDirectory(existingFiles, FileSystemHa.getRoot());
        }
        
        return existingFiles;
    }

    /**
     * 
     */
    private void discoverFilesWithinDirectory(Set<FileInfo> existingFiles, String directory)
    {
        log.debug("discovering local files within "+directory);
        for (FileInfo sub: fileSystem.listFileInfos(directory)) {
            String haName = sub.getHaName();
            if (fileSystem.isDirectory(haName)) {
                log.debug("file "+haName+" is a subdirectory");
                discoverFilesWithinDirectory(existingFiles, haName);
                
            } else if (fileSystem.exists(haName)) {
                if (sub.isDatabaseFile()) {
                    existingFiles.add(sub);
                }
            }
        }
        log.debug("end of local files discovery within "+directory);
   }

    /**
     *  Berechnet die MD5 Quersumme aus den übergebenen Bytes
     * @param in
     * @return
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     */
    protected byte[] computeMd5(byte[] in, int offset, int len)
    {
        md5Digest.reset();
        md5Digest.update(in, offset, len);
        return md5Digest.digest();
    }

    /**
     * @throws IOException 
     * 
     */
    protected FileObject getFileObject(String haName)
    throws IOException
    {
        FileObject fo = openFiles.get(haName);
        if (fo == null) {
            fo = fileSystem.openFileObject(haName, "rw");
            openFiles.put(haName, fo);
        }

        return fo;
    }

    /**
     * 
     * @param haName
     * @throws IOException
     */
    protected void closeFileObject(String haName, long lastModified)
    throws IOException
    {
        FileObject fo = openFiles.remove(haName);
        if (fo != null) {
            fo.close();
        }
        if (fileSystem.exists(haName)) {
            fileSystem.setLastModified(haName, lastModified);
        }
    }
    
    /**
     * 
     */
    protected void closeAllFileObjects()
    {
        for (FileObject fo: openFiles.values()) {
            try {
                fo.close();
            } catch (IOException x) {
                log.debug("error when trying to close a FileObject", x);
            }
        }
        
        openFiles.clear();
    }
    

    /**
     * 
     */
    protected static class StatusMessage
    extends ReplicationMessage
    {
	private static final long serialVersionUID = 1L;

	private FailoverState failoverState;
	private int masterPriority;
	private String uuid;

	public StatusMessage(FailoverState failoverState, int masterPriority, String uuid)
	{
	    this.failoverState = failoverState;
	    this.masterPriority = masterPriority;
	    this.uuid = uuid;
	}

	@Override
	protected void process(ReplicationProtocolInstance instance)
	throws Exception
	{
	    instance.peerStatusReceived(failoverState, masterPriority, uuid);
	}

	@Override
	public int getSizeEstimate()
	{
	    return 4;
	}

	@Override
	public String toString()
	{
	    return "status";
	}
    }
}