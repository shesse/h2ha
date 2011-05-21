/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.h2.store.fs.FileObject;


/**
 * ReplicationServerInstance is a thread
 * Independent from any database thread. It may be blocked 
 * temporarily when writing to the communication channel.
 * <p>
 * protocol: uses a TCP connection to the client and exchanges
 * objects of classes derived from MessageBase via Java serialization.
 * <p>
 * <ul>
 * <li>Initially the server sends a ListOfFiles message containing
 * information for all files it currently knows of.
 * <li>The client compares the server's list with it's local
 * available files and decides which files it wants to receive. It
 * responds with a SendFileRequest message, that contains a list of 
 * all files the client wants to receive. Each file is accompanied with
 * a code to select one of several possible file transmission 
 * methods. They are 
 * <ul>
 * <li>FULL: full transmission of the file is requested
 * <li>DELTA: a mode that attempts to only transfer modified blocks 
 * in the file. This is done in a two-step process: initially, the server
 * sends for each block in the file only a checksum. The client then
 * specifically requests those blocks for which it's local checksum
 * differs from the server's checksum.
 * <li>other mechanisms may be added when required 
 * </ul>
 * <li>The server sends - depending on the requirements - a sequence
 * of FileData or FileChecksum messages. Multiple messages may be used for large
 * files. For zero length files no FileData or FileChecksum
 * message need to be sent.
 * <li>End of file is indicated by a EndOfFile message for FULL file transmission and 
 * EndOfChecksums message for DELTA transmission.
 * <li>When the server has finished processing the list of files
 * it received in the SendFileRequest, it will send a SendFileConfirm.
 * For DELTA mode, this means, that the  EndOfChecksums has been sent.
 * <li>For DELTA mode, the client may respond to the FileChecksum messages with 
 * a SendBlockRequest message.
 * <li>The server responds to each SendBlockRequest messages with a FileData
 * message.
 * <li>The client responds to the EndOfChecksums message with a 
 * FileProcessed message, This is only used in DELTA mode.
 * <li>The server responds to the FileProcessed message with a EndOfFile
 * message. This is only used in DELTA mode.
 * <li>Due to sequence preservation the client will receive the SendFileConfirm
 * after the last EndOfChecksums and the last (FULL mode) EndOfFile message. 
 * <li>When seeing the SendFileConfirm, the client sends a 
 * LiveModeRequest message.
 * <li>The request is received by the server after all DELTA mode block
 * requests and terminating FileProcessed messages. It will enter live+
 * mode and send back a LiveModeConfirm.
 * <li>After receiving this, the client will also enter live mode.
 * <li>When a client has received all required information, it will send a
 * </ul>
 * 
 * @author sth
 */
public class ReplicationServerInstance
extends ServerSideProtocolInstance
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(ReplicationServerInstance.class);

   /** */
    private Map<FileInfo, SyncStatus> syncStatusByHaName = new HashMap<FileInfo, SyncStatus>();

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @throws IOException 
     */
    public ReplicationServerInstance(String instanceName, int maxWaitingMessages, H2HaServer haServer, FileSystemHa fileSystem, Socket socket)
    throws IOException
    {
        super(instanceName, maxWaitingMessages, haServer, fileSystem);
        
        haServer.registerServer(this);
        
        setSocket(socket);
        
        log.debug("ReplicationServerInstance()");
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * 
     */
    public void run()
    {
        log.info("a new client has connected");

        try {
            super.run();
        } finally {
            log.info("end of Client connection");
            fileSystem.deregisterReplicator(this);
            haServer.deregisterServer(this);
        }
    }
    
    /**
     * 
     */
    private SyncStatus getSyncStatus(String haName)
    {
        return getSyncStatus(fileSystem.getFileInfoForHaName(haName));
    }

    /**
     * 
     */
    private SyncStatus getSyncStatus(FileInfo fileInfo)
    {
        SyncStatus syncStatus = syncStatusByHaName.get(fileInfo);
        if (syncStatus == null) {
            syncStatus = new SyncStatus(fileInfo);
            syncStatusByHaName.put(fileInfo, syncStatus);
        }
        return syncStatus;
    }
    
    /**
     *
     */
    public boolean isActive()
    {
        return haServer.isActive();
    }
    
    /**
     * {@inheritDoc}
     *
     * @see com.shesse.h2ha.ReplicationProtocolInstance#sendHeartbeat()
     */
    @Override
    protected void sendHeartbeat()
	throws IOException
    {
	super.sendHeartbeat();
	sendStatus();
    }

    /**
     * This method may only be called from within the protocol instance
     * thread.
     * 
     * @throws IOException 
     * @throws SQLException 
     * 
     */
    public void processSendListOfFilesRequestMessage() 
    throws IOException, SQLException
    {
        fileSystem.registerReplicator(this);
        
        final List<String> entries = new ArrayList<String>();
        for (FileInfo fi: discoverExistingFiles()) {
            if (fi.mustReplicate()) {
        	String haName = fi.getHaName();
        	entries.add(haName);
            }
        }
        
        sendToPeer(new ListOfFilesConfirm(entries));
    }
    


    /**
     * This method may only be called from within the protocol instance
     * thread.
     * 
     * @param entries
     * @throws IOException 
     */
    public void processSendFileRequestMessage(List<FileRequestData> entries) throws IOException
    {
        if (log.isDebugEnabled()) log.debug("got SendFileRequest: "+entries);
        for (FileRequestData entry: entries) {
            if (entry.getTransmissionMethod() == FileRequestData.TransmissionMethod.FULL) {
                sendFullFile(entry);
            } else if (entry.getTransmissionMethod() == FileRequestData.TransmissionMethod.DELTA) {
                sendFileChecksums(entry);
            }
        }
        
        sendToPeer(new SendFileConfirmMessage());
    }


    /**
     * This method may only be called from within the protocol instance
     * thread.
     * 
     * @param entry
     * @throws IOException 
     */
    private void sendFullFile(FileRequestData entry) throws IOException
    {
        String haName = entry.getHaName();
        log.debug("sending file "+haName);

        SyncStatus syncStatus = getSyncStatus(haName);
        
        FileObject fo = getFileObject(haName);
        long fileSize = fo.length();
        byte[] buffer = new byte[4096];
        long offset = 0;
        syncStatus.endIgnore = fileSize;
        while (offset < fileSize) {
            try {
                int rdlen = buffer.length;
                long endrd = offset + rdlen;
                if (endrd > fileSize) {
                    rdlen = (int)(fileSize - offset);
                    endrd = fileSize;
                    buffer = new byte[rdlen];
                }

                syncStatus.beginIgnore = endrd;
                 
                if (fo instanceof FileObjectHa) {
                    ((FileObjectHa)fo).readFullyNocache(buffer, 0, rdlen);
                } else {
                    fo.readFully(buffer, 0, rdlen);
                }
                sendToPeer(new FileDataMessage(haName, offset, buffer));
                offset += rdlen;
                
            } catch (EOFException x) {
                long nfs = fo.length();
                if (nfs < fileSize) {
                    log.debug("adjusting to shrinking random access file");
                    fileSize = nfs;
                } else {
                    throw x;
                }
            }
        }
        
        syncStatus.beginIgnore = Long.MAX_VALUE;
        sendToPeer(new EndOfFileMessage(haName, fileSize, fileSystem.getLastModified(haName)));

        log.debug("file sent: "+haName);
    }

    /**
     * This method may only be called from within the protocol instance
     * thread.
     * 
     * @param entry
     * @throws IOException 
     * @throws NoSuchAlgorithmException 
     * @throws InvalidKeyException 
     */
    private void sendFileChecksums(FileRequestData entry)
    throws IOException
    {
        String haName = entry.getHaName();
        log.debug("sending checksums for file "+haName);
        
        SyncStatus syncStatus = getSyncStatus(haName);
        
        // Optimization: shortcut when file size and last modified 
        // have not changed
        long length = fileSystem.length(haName);
        long lastModified = fileSystem.getLastModified(haName);
        syncStatus.endIgnore = length;
        if (entry.getExistingFileLength() == length && entry.getExistingLastModified() == lastModified) {
            log.debug("file "+haName+" is unchanged");
            syncStatus.beginIgnore = Long.MAX_VALUE;
            
        } else {

            FileObject fo = getFileObject(haName);
            long fileSize = fo.length();
            byte[] buffer = new byte[4096];
            long offset = 0;
            while (offset < fileSize) {
                try {
                    int rdlen = buffer.length;
                    long endrd = offset + rdlen;
                    if (endrd > fileSize) {
                        rdlen = (int)(fileSize - offset);
                        endrd = fileSize;
                        buffer = new byte[rdlen];
                    }

                    syncStatus.beginIgnore = endrd;
                    
                    if (fo instanceof FileObjectHa) {
                        ((FileObjectHa)fo).readFullyNocache(buffer, 0, rdlen);
                    } else {
                	fo.readFully(buffer, 0, rdlen);
                    }
                    sendToPeer(new FileChecksumMessage(haName, offset, rdlen, computeMd5(buffer, 0, rdlen)));
                    offset += rdlen;

                } catch (EOFException x) {
                    long nfs = fo.length();
                    if (nfs < fileSize) {
                        log.debug("adjusting to shrinking random access file");
                        fileSize = nfs;
                    } else {
                        throw x;
                    }
                }
            }
        }
        
        syncStatus.beginIgnore = Long.MAX_VALUE;
        sendToPeer(new EndOfChecksumsMessage(haName));
        
        log.debug("checksums sent: "+haName);
    }


    /**
     * This method may only be called from within the protocol instance
     * thread.
     * 
     * @param haName
     * @param offset
     * @param length
     * @throws IOException 
     */
    public void processSendBlockRequestMessage(String haName, long offset, int length)
    throws IOException
    {
        log.debug("got SendBlockRequest - ha="+haName+", offset="+offset+", length="+length);
        FileObject fo = getFileObject(haName);
        long fileSize = fo.length();
        for (;;) {
            try {
                byte[] buffer = new byte[length];
                fo.seek(offset);
                if (fo instanceof FileObjectHa) {
                    ((FileObjectHa)fo).readFullyNocache(buffer, 0, length);
                } else {
                    fo.readFully(buffer, 0, length);
                }
                sendToPeer(new FileDataMessage(haName, offset, buffer));
                return;
                
            } catch (EOFException x) {
                long nfs = fo.length();
                if (nfs < fileSize) {
                    log.debug("adjusting to shrinking random access file");
                    fileSize = nfs;
                    if (offset + length < fileSize) {
                        length = (int)(fileSize - offset);
                        if (length < 0 ) length = 0;
                    }
                } else {
                    throw x;
                }
            }
        }
    }


    /**
     * This method may only be called from within the protocol instance
     * thread.
     * 
     * @param haName
     * @throws IOException 
     */
    public void processFileProcessedMessage(String haName)
    throws IOException
    {
        log.debug("got FileProcessed - ha="+haName);
        long length = fileSystem.length(haName);
        long lastModified = fileSystem.getLastModified(haName);
        sendToPeer(new EndOfFileMessage(haName, length, lastModified));
    }


    /**
     * This method may only be called from within the protocol instance
     * thread.
     * 
     * @throws IOException 
     * 
     */
    public void processLiveModeRequestMessage()
    throws IOException
    {
        log.debug("got LiveModeRequest");
        log.info("a connection to a slave is entering realtime mode - we stay master");
        sendToPeer(new LiveModeConfirmMessage());
    }

    
    /**
     * 
     */
    public void processStopReplicationRequest()
    throws IOException
    {
        fileSystem.deregisterReplicator(this);
        sendToPeer(new StopReplicationConfirmMessage());
    }


    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////
    /**
     * 
     */
    private static class ListOfFilesConfirm
    extends MessageToClient
    {
        private static final long serialVersionUID = 1L;
        List<String> entries;
        
        ListOfFilesConfirm(List<String> entries)
        {
            this.entries = entries;
        }
        
        @Override
        protected void processMessageToClient(ReplicationClientInstance instance) throws Exception
        {
            instance.processListOfFilesConfirmMessage(entries);

        }

	@Override
	public int getSizeEstimate()
	{
	    return 20*entries.size();
	}
	
	@Override
	public String toString()
	{
	    return "list of files: "+entries;
	}
   }
    
    
    /**
     * 
     */
    private static class SendFileConfirmMessage
    extends MessageToClient
    {
        private static final long serialVersionUID = 1L;
         
        SendFileConfirmMessage()
        {
        }
        
        @Override
        protected void processMessageToClient(ReplicationClientInstance instance) throws Exception
        {
            instance.processSendFileConfirmMessage();
       }

	@Override
	public int getSizeEstimate()
	{
	    return 4;
	}
	
	@Override
	public String toString()
	{
	    return "send file cnf";
	}
    }
    
    
    /**
     * 
     */
    private static class FileDataMessage
    extends MessageToClient
    {
        private static final long serialVersionUID = 1L;
        String haName;
        long offset;
        byte[] data;
        
        FileDataMessage(String haName, long offset, byte[] data)
        {
            this.haName = haName;
            this.offset = offset;
            this.data = data;
        }
        
        @Override
        protected void processMessageToClient(ReplicationClientInstance instance) throws Exception
        {
            instance.processFileDataMessage(haName, offset, data);

        }

	@Override
	public int getSizeEstimate()
	{
	    return 30+data.length;
	}
	
	@Override
	public String toString()
	{
	    return "fiel data "+haName+", offs="+offset+", len="+data.length;
	}
   }
    
    
    /**
     * 
     */
    private static class FileChecksumMessage
    extends MessageToClient
    {
        private static final long serialVersionUID = 1L;
        String haName;
        long offset;
        int length;
        byte[] checksum;
        
        FileChecksumMessage(String haName, long offset, int length, byte[] checksum)
        {
            this.haName = haName;
            this.offset = offset;
            this.length = length;
            this.checksum = checksum;
         }
        
        @Override
        protected void processMessageToClient(ReplicationClientInstance instance) throws Exception
        {
            instance.processFileChecksumMessage(haName, offset, length, checksum);

        }

	@Override
	public int getSizeEstimate()
	{
	    return 32+checksum.length;
	}
	
	@Override
	public String toString()
	{
	    return "file checksum "+haName+", offs="+offset+", len="+length;
	}
    }
    
    
    /**
     * 
     */
    private static class EndOfFileMessage
    extends MessageToClient
    {
        private static final long serialVersionUID = 1L;
        String haName;
        long length;
        long lastModified;
        
        EndOfFileMessage(String haName, long length, long lastModified)
        {
            this.haName = haName;
            this.length = length;
            this.lastModified = lastModified;
        }
        
        @Override
        protected void processMessageToClient(ReplicationClientInstance instance) throws Exception
        {
            instance.processEndOfFileMessage(haName, length, lastModified);

        }

	@Override
	public int getSizeEstimate()
	{
	    return 36;
	}
	
	@Override
	public String toString()
	{
	    return "end of file "+haName+", len="+length+", mod="+new Date(lastModified);
	}
    }
    
    
    /**
     * 
     */
    private static class EndOfChecksumsMessage
    extends MessageToClient
    {
        private static final long serialVersionUID = 1L;
        String haName;
        
        EndOfChecksumsMessage(String haName)
        {
            this.haName = haName;
        }
        
        @Override
        protected void processMessageToClient(ReplicationClientInstance instance)
        throws Exception
        {
            instance.processEndOfChecksumsMessage(haName);

        }

	@Override
	public int getSizeEstimate()
	{
	    return 20;
	}
	
	@Override
	public String toString()
	{
	    return "end of checksums "+haName;
	}
   }
    
    
    /**
     * 
     */
    private static class LiveModeConfirmMessage
    extends MessageToClient
    {
        private static final long serialVersionUID = 1L;
        
        LiveModeConfirmMessage()
        {
        }
        
        @Override
        protected void processMessageToClient(ReplicationClientInstance instance) throws Exception
        {
            instance.processLiveModeConfirmMessage();

        }

	@Override
	public int getSizeEstimate()
	{
	    return 4;
	}
	
	@Override
	public String toString()
	{
	    return "live mode cnf";
	}
    }
    
    
    /**
     * 
     */
    private static class StopReplicationConfirmMessage
    extends MessageToClient
    {
        private static final long serialVersionUID = 1L;
        
        StopReplicationConfirmMessage()
        {
        }
        
        @Override
        protected void processMessageToClient(ReplicationClientInstance instance) throws Exception
        {
            instance.processStopReplicationConfirmMessage();

        }

	@Override
	public int getSizeEstimate()
	{
	    return 4;
	}
	
	@Override
	public String toString()
	{
	    return "live mode cnf";
	}
    }
    
    
    
    /**
     * 
     */
    private static class SyncStatus
    {
	@SuppressWarnings("unused")
	FileInfo fileInfo;
 	@SuppressWarnings("unused")
	volatile long beginIgnore = 0L;
 	@SuppressWarnings("unused")
	volatile long endIgnore = Long.MAX_VALUE;
        SyncStatus(FileInfo fileInfo)
        {
            this.fileInfo = fileInfo;
        }
    }
    
}
