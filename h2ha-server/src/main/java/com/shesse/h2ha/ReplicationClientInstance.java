/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.h2.store.fs.FilePath;

import com.shesse.h2ha.H2HaServer.Event;
import com.shesse.h2ha.H2HaServer.FailoverState;


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
	private FilePath dirtyFlagPath;

	/** */
	private int maxConnectRetries = 5;

	/** */
	private long waitBetweenReconnects = 20000;

	/** */
	private long waitBetweenConnectRetries = 500;

	/** */
	private long earliestNextConnect = 0;

	/** */
	private int fileDeltasRequested = 0;

	/** */
	private int fullFilesRequested = 0;

	/** */
	private int endOfChecksumReceived = 0;

	/** */
	private int endOfFileReceived = 0;

	/** */
	private FailoverState peerState = FailoverState.INITIAL;

	/** */
	private int peerMasterPriority = 0;

	/** */
	private String peerUuid = "-";

	/** */
	private boolean autoFailback = false;

	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @param receiver
	 */
	public ReplicationClientInstance(H2HaServer haServer, FileSystemHa fileSystem, List<String> args)
	{
		super("replClient", 0, 0, 0, haServer, fileSystem);
		log.debug("ReplicationClientInstance()");

		dirtyFlagPath = fileSystem.getHaBaseDir().getPath(dirtyFlagFile);
		long statisticsInterval = 300000L;

		peerHost = H2HaServer.findOptionWithValue(args, "-haPeerHost", "replication-peer");
		peerPort = H2HaServer.findOptionWithInt(args, "-haPeerPort", 8234);
		connectTimeout = H2HaServer.findOptionWithInt(args, "-haConnectTimeout", 10000);
		statisticsInterval = H2HaServer.findOptionWithInt(args, "-statisticsInterval", 300000);
		maxConnectRetries = H2HaServer.findOptionWithInt(args, "-connectRetry", 5);
		autoFailback = H2HaServer.findOption(args, "-autoFailback");

		setInstanceName("replClient-" + peerHost + ":" + peerPort);
		setParameters(statisticsInterval);
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 */
	public void run()
	{
		try {
			body();
		} catch (Throwable x) {
			log.fatal("unexepected exception within HA client thread", x);
			System.exit(1);
		}
	}


	/**
	 */
	private void body()
	{
		log.debug("replication client instance has been started");
		for (;;) {
			// we will wait between reconnect attempts to prevent
			// busy waits when no peer can be reached
			long now = System.currentTimeMillis();
			if (earliestNextConnect > now) {
				try {
					Thread.sleep(earliestNextConnect - now);
				} catch (InterruptedException e) {
					log.error("InterruptedException", e);
				}

			} else {
				earliestNextConnect = System.currentTimeMillis() + waitBetweenReconnects;
				try {
					establishAndMaintainConnection();

				} catch (Throwable x) {
					log.error("unexepected exception within peer connection thread", x);

				} finally {
					peerState = FailoverState.INITIAL;
				}
			}
		}
	}


	/**
	 */
	private void establishAndMaintainConnection()
	{
		// we will repeat a limited number of connect attempts in quick
		// succession to be sure that the peer is really not available
		// and that is it not a temporary glitch that causes our
		// connect problem
		//
		int retryCount = 0;
		while (retryCount < maxConnectRetries && !isConnected()) {
			if (tryToConnect()) {
				break;
			}
			retryCount++;

			try {
				Thread.sleep(waitBetweenConnectRetries);
			} catch (InterruptedException x) {
			}
		}

		if (isConnected()) {
			log.info("peer has been contacted");
			earliestNextConnect = 0;

			issueConnEvent();

			super.run();
			log.info("connection to peer has ended");
			haServer.applyEvent(Event.DISCONNECTED, null, null);

		} else {
			log.info("could not contact peer");
			issueConnEvent();

		}
	}

	/**
	 * Tries to initiate a connection connection to the peer.
	 * 
	 * @return true if connection was successful.
	 */
	private boolean tryToConnect()
	{
		if (tryToConnect(peerHost, peerPort, connectTimeout)) {
			setInstanceName("replClient-" + String.valueOf(socket.getRemoteSocketAddress()));
			return true;

		} else {
			return false;
		}
	}

	/**
	 * 
	 */
	public void sendListFilesRequest()
	{
		log.info("HA sync: requesting list of files");
		send(new SendListOfFilesRequest());
	}

	/**
	 * 
	 */
	public void sendStopReplicationRequest()
	{
		send(new StopReplicationRequest());
	}

	/**
	 * 
	 */
	public void issueConnEvent()
	{
		if (isConnected()) {
			haServer.applyEvent(Event.CONNECTED_TO_PEER, null, null);

		} else {
			if (isConsistentData()) {
				haServer.applyEvent(Event.CANNOT_CONNECT, "valid", null);
			} else {
				log.info("no peer and local database is in an inconsistent state - we need to wait for a consistent master");
				haServer.applyEvent(Event.CANNOT_CONNECT, "invalid", null);
			}
		}
	}

	/**
	 * 
	 */
	public void issuePeerEvent()
	{
		String eventParam = peerState.toString();
		String optParam;
		if (haServer.weAreConfiguredMaster(peerMasterPriority, peerUuid)) {
			// local system is configured master
			optParam = "local";
		} else if (autoFailback) {
			// local sytem is configured slave and autoFailback selected
			optParam = "xfer";

		} else {
			// local system is configured slave - no autoFailback configured
			optParam = "peer";
		}

		haServer.applyEvent(Event.PEER_STATE, eventParam, optParam);
	}

	/**
	 * 
	 */
	public void setDirtyFlag(boolean dirtyFlag)
	{
		if (dirtyFlag) {
			dirtyFlagPath.createFile();
		} else {
			dirtyFlagPath.delete();
		}
	}

	/**
	 * 
	 */
	public boolean isConsistentData()
	{
		return !dirtyFlagPath.exists();
	}

	/**
	 * @return
	 */
	public String getPeerHost()
	{
		return peerHost;
	}

	/**
	 * @return
	 */
	public int getPeerPort()
	{
		return peerPort;
	}

	/**
	 * @return
	 */
	public FailoverState getPeerState()
	{
		return peerState;
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
	 * {@inheritDoc}
	 * 
	 * @see com.shesse.h2ha.ReplicationProtocolInstance#peerStatusReceived(int,
	 *      java.lang.String)
	 */
	@Override
	protected void peerStatusReceived(FailoverState peerState, int peerMasterPriority,
		String peerUuid)
	{
		this.peerState = peerState;
		this.peerMasterPriority = peerMasterPriority;
		this.peerUuid = peerUuid;

		super.peerStatusReceived(peerState, peerMasterPriority, peerUuid);

		issuePeerEvent();
	}


	/**
	 * This method may only be called from within the protocol instance thread.
	 * 
	 * @throws IOException
	 * 
	 */
	public void processListOfFilesConfirmMessage(List<String> haNames)
		throws IOException
	{
		if (log.isDebugEnabled())
			log.debug("got ListOfFiles: " + haNames);

		setDirtyFlag(true);

		List<FileRequestData> fileRequests = new ArrayList<FileRequestData>();
		fileDeltasRequested = 0;
		fullFilesRequested = 0;
		endOfFileReceived = 0;
		endOfChecksumReceived = 0;

		Set<FilePathHa> remainingFiles = discoverExistingFiles();
		for (String haName : haNames) {
			FilePathHa haPath = new FilePathHa(fileSystem, haName, true);
			FileRequestData dataRequest;
			if (remainingFiles.remove(haPath)) {
				log.debug("file " + haPath + " exists - asking for delta info");
				dataRequest =
					new FileRequestData(haName, FileRequestData.TransmissionMethod.DELTA,
						haPath.size(), haPath.lastModified());
				fileDeltasRequested++;

			} else {
				log.debug("file " + haPath + " does not exist - requesting full transfer");
				dataRequest =
					new FileRequestData(haName, FileRequestData.TransmissionMethod.FULL, -1, -1);
				fullFilesRequested++;
			}

			fileRequests.add(dataRequest);
		}

		for (FilePathHa haPath : remainingFiles) {
			log.debug("file " + haPath + " does not exist on server - deleting it");
			haPath.delete();
		}

		log.info("HA sync: " + haNames.size() + " files on server - requested " +
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
		log.debug("got FileData - ha=" + haName + ", offset=" + offset + ", length=" + data.length);
		FileChannel fc = getFileChannel(haName);
		fc.position(offset);
		fc.write(ByteBuffer.wrap(data, 0, data.length));
	}


	/**
	 * This method may only be called from within the protocol instance thread.
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
		FileChannel fc = getFileChannel(haName);

		boolean segmentDiffers = false;

		if (offset + length > fc.size() && length > 0) {
			log.debug("got FileChecksum - ha=" + haName + ", offset=" + offset + ", length=" +
				length + ": peer longer than local");
			segmentDiffers = true;

		} else {
			fc.position(offset);
			ByteBuffer buffer = ByteBuffer.allocate(length);
			int rlength;
			if (fc instanceof FileChannelHa) {
				rlength = ((FileChannelHa) fc).readNoCache(buffer);
			} else {
				rlength = fc.read(buffer);
			}

			if (rlength < length) {
				log.debug("unexpected EOF when reading local file - ha=" + haName + ", offset=" +
					offset + ", length=" + length + ": got only " + rlength);
				segmentDiffers = true;
			} else {
				buffer.hasArray();
				byte[] localMd5 = computeMd5(buffer);
				if (!Arrays.equals(localMd5, checksum)) {
					log.debug("got FileChecksum - ha=" + haName + ", offset=" + offset +
						", length=" + length + ": checksums differ");
					segmentDiffers = true;
				} else {
					log.debug("got FileChecksum - ha=" + haName + ", offset=" + offset +
						", length=" + length + ": same checksums");
				}
			}
		}

		if (segmentDiffers) {
			sendToPeer(new SendBlockRequest(haName, offset, length));
		}
	}


	/**
	 * This method may only be called from within the protocol instance thread.
	 * 
	 * @param haName
	 * @param length
	 * @param lastModified
	 * @throws IOException
	 */
	public void processEndOfChecksumsMessage(String haName)
		throws IOException
	{
		log.debug("got EndOfChecksums - ha=" + haName);
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
		log.debug("got EndOfFile - ha=" + haName + ", length=" + length + ", mod=" + lastModified);

		FilePathHa fp = getFilePath(haName);
		FileChannel fc = getFileChannel(fp);
		fc.truncate(length);
		closeFileObject(fp, lastModified);
		endOfFileReceived++;
		log.info("HA sync: " + endOfFileReceived + " of " +
			(fileDeltasRequested + fullFilesRequested) + " files complete");
	}


	/**
	 * This method may only be called from within the protocol instance thread.
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
		haServer.applyEvent(Event.SYNC_COMPLETED, null, null);
	}

	/**
	 * 
	 */
	public void processStopReplicationConfirmMessage()
	{
		log.info("this server has stopped replicating the master");
		haServer.applyEvent(Event.SLAVE_STOPPED, null, null);
	}

	/**
	 * @param fileName
	 */
	public void processCreateDirectoryMessage(String haName)
	{
		FilePathHa fp = getFilePath(haName);
		fp.createDirectory();
	}

	/**
	 * @param fileName
	 */
	public void processCreateFileMessage(String haName)
	{
		FilePathHa fp = getFilePath(haName);
		fp.createFile();
	}

	/**
	 * @param fileName
	 */
	public void processDeleteMessage(String haName)
	{
		FilePathHa fp = getFilePath(haName);
		fp.delete();
	}

	/**
	 * @param oldName
	 */
	public void processMoveToMessage(String oldName, String newName)
	{
		FilePathHa oldFp = getFilePath(oldName);
		FilePathHa newFp = getFilePath(newName);
		oldFp.moveTo(newFp);
	}

	/**
	 * @param haName
	 * @throws IOException
	 */
	public void processCloseMessage(String haName, long lastModified)
		throws IOException
	{
		closeFileChannel(haName, lastModified);
	}

	/**
	 * @param fileName
	 */
	public void processSetReadOnlyMessage(String haName)
	{
		FilePathHa fp = getFilePath(haName);
		fp.setReadOnly();
	}

	/**
	 * @param haName
	 * @throws IOException
	 */
	public void processTruncateMessage(String haName, long newLength)
		throws IOException
	{
		FileChannel fc = getFileChannel(haName);
		fc.truncate(newLength);
	}

	/**
	 * @param haName
	 * @throws IOException
	 */
	public void processWriteMessage(String haName, long filePointer, byte[] data)
		throws IOException
	{
		FileChannel fc = getFileChannel(haName);
		fc.position(filePointer);
		ByteBuffer buffer = ByteBuffer.wrap(data);
		fc.write(buffer);
	}

	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////
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

		@Override
		public String toString()
		{
			return "send list of files req";
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
			return 45 * entries.size();
		}

		@Override
		public String toString()
		{
			return "send file req: nentries=" + entries.size();
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

		@Override
		public String toString()
		{
			return "send block req " + haName + ", offs=" + offset + ", len=" + length;
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

		@Override
		public String toString()
		{
			return "file processed " + haName;
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

		@Override
		public String toString()
		{
			return "live mode req";
		}
	}

	/**
	 * 
	 */
	private static class StopReplicationRequest
		extends MessageToServer
	{
		private static final long serialVersionUID = 1L;

		StopReplicationRequest()
		{
		}

		@Override
		protected void processMessageToServer(ReplicationServerInstance instance)
			throws Exception
		{
			instance.processStopReplicationRequest();
		}

		@Override
		public int getSizeEstimate()
		{
			return 4;
		}

		@Override
		public String toString()
		{
			return "stop replication req";
		}
	}

}
