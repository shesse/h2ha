/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.h2.store.fs.FileObject;


/**
 * 
 * @author sth
 */
public class ReplicationClientInstance
extends ServerSideProtocolInstance
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(ReplicationClientInstance.class);
    
    /** */
    private String peerHost = "replication-peer";
    
    /** */
    private int peerPort = 8234;

    /** */
    private int connectTimeout = 10000;
    
    /** */
    private static final String dirtyFlagFile = "dirty.flag";
    
    /** */
    private String dirtyFlagUrl;
    
    /** */
    private int fileDeltasRequested = 0;
    
    /** */
    private int fullFilesRequested = 0;
    
    /** */
    private int endOfChecksumReceived = 0;
    
    /** */
    private int endOfFileReceived = 0;
    
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @param receiver 
     */
    public ReplicationClientInstance(H2HaServer haServer, FileSystemHa fileSystem, String[] args)
    {
        super("server", 0, haServer, fileSystem);
        log.debug("ReplicationClientInstance()");

        dirtyFlagUrl = FileSystemHa.getRoot()+dirtyFlagFile;

        for (int i = 0; i < args.length-1; i++) {
            if (args[i].equals("-haPeerHost")) {
                peerHost = args[i+1];
                i++;

            } else if (args[i].equals("-haPeerPort")) {
                try {
                    peerPort = Integer.parseInt(args[i+1]);
                    i++;
                } catch (NumberFormatException x) {
                    log.error("inhalid haPeerPort: "+x);
                }
            } else if (args[i].equals("-haConnectTimeout")) {
                try {
                    connectTimeout = Integer.parseInt(args[i+1]);
                    i++;
                } catch (NumberFormatException x) {
                    log.error("inhalid haConnectTimeout: "+x);
                }
            }
        }
        
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * Tries to initiate a connection connection to the peer.
     * @return true if connection was successful.
     */
    public boolean tryToConnect()
    {
        return tryToConnect(peerHost, peerPort, connectTimeout);
    }

    /**
     * @throws InterruptedException 
     * 
     */
    public void run() 
    {
        log.debug("replication client instance has been started");
        try {
            sendToPeer(new NegotiateRoleRequest(haServer.getMasterPriority(), haServer.getUuid()));
            super.run();
        } catch (IOException x) {
            log.error("Error when receiving messages as client", x);
        }
    }
    
    /**
     * 
     */
    public void setDirtyFlag(boolean dirtyFlag)
    {
        if (dirtyFlag) {
            fileSystem.createNewFile(dirtyFlagUrl);
        } else {
            fileSystem.delete(dirtyFlagUrl);
        }
    }
    
    /**
     * 
     */
    public boolean isConsistentData()
    {
        return !fileSystem.exists(dirtyFlagUrl);
    }

   /**
     * @param peerIsMaster
     * @throws IOException 
     */
    public void processNegotiateRoleConfirmMessage(boolean peerIsMaster) 
    throws IOException
    {
        if (peerIsMaster) {
            log.info("peer is master - negotiated role for this instance is: slave");

        } else if (haServer.tryToSetMasterRole()) {
            log.info("peer is slave - negotiated role for this instance is: master");
            terminate();
            return;
            
        } else {
            log.error("other side refuses master role - and we are also not able to become master");
            log.error("entering slave mode");
        }
        
        sendToPeer(new SendListOfFilesRequest());
    }


    /**
     * This method may only be called from within the protocol instance
     * thread.
     * 
     * @throws IOException 
     * 
     */
    public void processListOfFilesConfirmMessage(List<String> files) 
    throws IOException
    {
        if (log.isDebugEnabled()) log.debug("got ListOfFiles: "+files);
        
        setDirtyFlag(true);
        
        List<FileRequestData> fileRequests = new ArrayList<FileRequestData>();
        fileDeltasRequested = 0;
        fullFilesRequested = 0;
        endOfFileReceived = 0;
        endOfChecksumReceived = 0;
        
        Set<FileInfo> remainingFiles = discoverExistingFiles();
        for (String haName: files) {
            FileInfo fi = fileSystem.getFileInfoForHaName(haName);

            FileRequestData dataRequest;
            if (remainingFiles.remove(fi)) {
                 log.debug("file "+fi+" exists - asking for delta info");
                dataRequest = new FileRequestData(haName, 
                    FileRequestData.TransmissionMethod.DELTA,
                    fileSystem.length(haName),
                    fileSystem.getLastModified(haName));
                fileDeltasRequested++;
 
            } else {
                log.debug("file "+fi+" does not exist - requesting full transfer");
                dataRequest = new FileRequestData(haName, 
                    FileRequestData.TransmissionMethod.FULL, -1, -1);
                fullFilesRequested++;
            }

            fileRequests.add(dataRequest);
        }
        
        for (FileInfo fi: remainingFiles) {
            log.debug("file "+fi+" does not exist on server - deleting it");
            new File(fi.getLocalName()).delete();
        }
        
        log.info("HA sync: " + files.size() + " files on server - requested " +
            fileDeltasRequested + " deltas and " + fullFilesRequested + " full files");
        sendToPeer(new SendFileRequest(fileRequests));
    }

    /**
     * @param haName
     * @param offset
     * @param data
     * @throws IOException 
     */
    public void processFileDataMessage(String haName, long offset, byte[] data)
    throws IOException
    {
        log.debug("got FileData - ha="+haName+", offset="+offset+", length="+data.length);
        FileObject fo = getFileObject(haName);
        fo.seek(offset);
        fo.write(data, 0, data.length);
    }


    /**
     * This method may only be called from within the protocol instance
     * thread.
     * 
     * @param haName
     * @param offset
     * @param length
     * @param checksum
     * @throws IOException 
     */
    public void processFileChecksumMessage(String haName, long offset, int length, byte[] checksum) 
    throws IOException
    {
        FileObject fo = getFileObject(haName);
        
        boolean segmentDiffers = false;
        
        if (offset+length > fo.length() && length > 0) {
            log.debug("got FileChecksum - ha="+haName+", offset="+offset+", length="+length+": peer longer than local");
            segmentDiffers = true;

        } else {
            fo.seek(offset);
            byte[] buffer = new byte[length];
            fo.readFully(buffer, 0, length);
            
            byte[] localMd5 = computeMd5(buffer, 0, length);
            if (!Arrays.equals(localMd5, checksum)) {
                log.debug("got FileChecksum - ha="+haName+", offset="+offset+", length="+length+": checksums differ");
                segmentDiffers = true;
            } else {
                log.debug("got FileChecksum - ha="+haName+", offset="+offset+", length="+length+": same checksums");
            }
        }

        if (segmentDiffers) {
            sendToPeer(new SendBlockRequest(haName, offset, length));
        }
    }


    /**
     * This method may only be called from within the protocol instance
     * thread.
     * 
     * @param haName
     * @param length
     * @param lastModified
     * @throws IOException 
     */
    public void processEndOfChecksumsMessage(String haName)
    throws IOException
    {
        log.debug("got EndOfChecksums - ha="+haName);
        endOfChecksumReceived++;
        log.info("HA sync: " + endOfChecksumReceived + " of " + fileDeltasRequested +
            " file checksums complete");
        sendToPeer(new FileProcessed(haName));
    }


    /**
     * @param haName
     * @throws IOException 
     */
    public void processEndOfFileMessage(String haName, long length, long lastModified)
    throws IOException
    {
        log.debug("got EndOfFile - ha="+haName+", length="+length+", mod="+lastModified);
        FileObject fo = getFileObject(haName);
        fo.setFileLength(length);
        fileSystem.setLastModified(haName, lastModified);
        endOfFileReceived++;
        log.info("HA sync: " + endOfFileReceived + " of " +
            (fileDeltasRequested + fullFilesRequested) + " files complete");
     }


    /**
     * This method may only be called from within the protocol instance
     * thread.
     * 
     * @throws IOException 
     * 
     */
    public void processSendFileConfirmMessage()
    throws IOException
    {
        log.debug("got SendFileConfirm");
        sendToPeer(new LiveModeRequest());
    }

    /**
     * 
     */
    public void processLiveModeConfirmMessage()
    {
        log.debug("got LiveModeConfirm");
        log.info("entering realtime replication mode");
        setDirtyFlag(false);
        closeAllFileObjects();
    }

    /**
     * @param original
     * @param copy
     */
    public void processCopyMessage(String original, String copy)
    {
        fileSystem.copy(original, copy);
    }

    /**
     * @param fileName
     */
    public void processCreateDirsMessage(String fileName)
    {
        fileSystem.createDirs(fileName);
    }

    /**
     * @param fileName
     */
    public void processCreateNewFileMessage(String fileName)
    {
        fileSystem.createNewFile(fileName);
    }

    /**
     * @param fileName
     */
    public void processTryDeleteMessage(String fileName)
    {
        fileSystem.tryDelete(fileName);
    }

    /**
     * @param fileName
     */
    public void processDeleteMessage(String fileName)
    {
        fileSystem.delete(fileName);
    }

    /**
     * @param directory
     */
    public void processDeleteRecursiveMessage(String directory, boolean tryOnly)
    {
        fileSystem.deleteRecursive(directory, tryOnly);
    }

    /**
     * @param directoryName
     */
    public void processMkdirsMessage(String directoryName)
    {
        fileSystem.mkdirs(directoryName);
    }

    /**
     * @param oldName
     */
    public void processRenameMessage(String oldName, String newName)
    {
        fileSystem.rename(oldName, newName);
    }

    /**
     * @param fileName
     */
    public void processSetReadOnlyMessage(String fileName)
    {
        fileSystem.setReadOnly(fileName);
    }

    /**
     * @param haName
     * @throws IOException 
     */
    public void processFoCloseMessage(String haName, long lastModified)
    throws IOException
    {
        closeFileObject(haName, lastModified);
    }

    /**
     * @param haName
     * @throws IOException 
     */
    public void processFoSetFileLengthMessage(String haName, long newLength)
    throws IOException
    {
        FileObject fo = getFileObject(haName);
        fo.setFileLength(newLength);
    }
    
    /**
     * @param haName
     * @throws IOException 
     */
    public void processFoWriteMessage(String haName, long filePointer, byte[] data)
    throws IOException
    {
        FileObject fo = getFileObject(haName);
        fo.seek(filePointer);
        fo.write(data, 0, data.length);
    }
    
    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////
    /**
     * 
     */
    private static class NegotiateRoleRequest
    extends MessageToServer
    {
        private static final long serialVersionUID = 1L;
        int masterPriority;
        String uuid;
        
        NegotiateRoleRequest(int masterPriority, String uuid)
        {
            this.masterPriority = masterPriority;
            this.uuid = uuid;
        }
        
        @Override
        protected void processMessageToServer(ReplicationServerInstance instance)
        throws Exception
        {
            instance.processNegotiateRoleRequestMessage(masterPriority, uuid);

        }

	@Override
	public int getSizeEstimate()
	{
	    return 35;
	}
    }

    /**
     * 
     */
    private static class SendListOfFilesRequest
    extends MessageToServer
    {
        private static final long serialVersionUID = 1L;
        
        SendListOfFilesRequest()
        {
        }
        
        @Override
        protected void processMessageToServer(ReplicationServerInstance instance)
        throws Exception
        {
            instance.processSendListOfFilesRequestMessage();
         }

	@Override
	public int getSizeEstimate()
	{
	    return 4;
	}
   }

    /**
     * 
     */
    private static class SendFileRequest
    extends MessageToServer
    {
        private static final long serialVersionUID = 1L;
        List<FileRequestData> entries;
        
        SendFileRequest(List<FileRequestData> entries)
        {
            this.entries = entries;
        }
        
        @Override
        protected void processMessageToServer(ReplicationServerInstance instance)
        throws Exception
        {
            instance.processSendFileRequestMessage(entries);

        }

	@Override
	public int getSizeEstimate()
	{
	    return 45*entries.size();
	}
    }

    /**
     *
     */
    private static class SendBlockRequest
    extends MessageToServer
    {
        private static final long serialVersionUID = 1L;
        String haName;
        long offset;
        int length;

        public SendBlockRequest(String haName, long offset, int length)
        {
            this.haName = haName;
            this.offset = offset;
            this.length = length;
        }

        
        @Override
        protected void processMessageToServer(ReplicationServerInstance instance)
        throws Exception
        {
            instance.processSendBlockRequestMessage(haName, offset, length);
        }

	@Override
	public int getSizeEstimate()
	{
	    return 32;
	}
    }

    /**
     *
     */
    private static class FileProcessed
    extends MessageToServer
    {
        private static final long serialVersionUID = 1L;
        String haName;
 
        public FileProcessed(String haName)
        {
            this.haName = haName;
        }

        
        @Override
        protected void processMessageToServer(ReplicationServerInstance instance)
        throws Exception
        {
            instance.processFileProcessedMessage(haName);
        }

	@Override
	public int getSizeEstimate()
	{
	    return 20;
	}
    }

    /**
     *
     */
    private static class LiveModeRequest
    extends MessageToServer
    {
        private static final long serialVersionUID = 1L;

        public LiveModeRequest()
        {
        }


        @Override
        protected void processMessageToServer(ReplicationServerInstance instance)
        throws Exception
        {
            instance.processLiveModeRequestMessage();
        }

	@Override
	public int getSizeEstimate()
	{
	    return 4;
	}
   }

}
