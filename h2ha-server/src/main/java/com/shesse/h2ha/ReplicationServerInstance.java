/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.shesse.h2ha.H2HaServer.FailoverState;


/**
 * Handles an incoming connection from a single replicator.
 * <p>
 * ReplicationServerInstance is a thread Independent from any database thread.
 * It may be blocked temporarily when writing to the communication channel.
 * <p>
 * protocol: uses a TCP connection to the client and exchanges objects of
 * classes derived from MessageBase via Java serialization.
 * <p>
 * <ul>
 * <li>Initially the server sends a ListOfFiles message containing information
 * for all files it currently knows of.
 * <li>The client compares the server's list with it's local available files and
 * decides which files it wants to receive. It responds with a SendFileRequest
 * message, that contains a list of all files the client wants to receive. Each
 * file is accompanied with a code to select one of several possible file
 * transmission methods. They are
 * <ul>
 * <li>FULL: full transmission of the file is requested
 * <li>DELTA: a mode that attempts to only transfer modified blocks in the file.
 * This is done in a two-step process: initially, the server sends for each
 * block in the file only a checksum. The client then specifically requests
 * those blocks for which it's local checksum differs from the server's
 * checksum.
 * <li>other mechanisms may be added when required
 * </ul>
 * <li>The server sends - depending on the requirements - a sequence of FileData
 * or FileChecksum messages. Multiple messages may be used for large files. For
 * zero length files no FileData or FileChecksum message need to be sent.
 * <li>End of file is indicated by a EndOfFile message for FULL file
 * transmission and EndOfChecksums message for DELTA transmission.
 * <li>When the server has finished processing the list of files it received in
 * the SendFileRequest, it will send a SendFileConfirm. For DELTA mode, this
 * means, that the EndOfChecksums has been sent.
 * <li>For DELTA mode, the client may respond to the FileChecksum messages with
 * a SendBlockRequest message.
 * <li>The server responds to each SendBlockRequest messages with a FileData
 * message.
 * <li>The client responds to the EndOfChecksums message with a FileProcessed
 * message, This is only used in DELTA mode.
 * <li>The server responds to the FileProcessed message with a EndOfFile
 * message. This is only used in DELTA mode.
 * <li>Due to sequence preservation the client will receive the SendFileConfirm
 * after the last EndOfChecksums and the last (FULL mode) EndOfFile message.
 * <li>When seeing the SendFileConfirm, the client sends a LiveModeRequest
 * message.
 * <li>The request is received by the server after all DELTA mode block requests
 * and terminating FileProcessed messages. It will enter live+ mode and send
 * back a LiveModeConfirm.
 * <li>After receiving this, the client will also enter live mode.
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
	private Timestamp startTime = new Timestamp(System.currentTimeMillis());

	/** */
	private Map<FilePathHa, SyncInfo> syncInfos = new HashMap<FilePathHa, SyncInfo>();


	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @throws IOException
	 */
	public ReplicationServerInstance(String instanceName, int maxQueueSize, long maxEnqueueWait,
										int maxWaitingMessages, long statisticsInterval,
										H2HaServer haServer, FileSystemHa fileSystem, Socket socket)
		throws IOException
	{
		super(instanceName, maxQueueSize, maxEnqueueWait, maxWaitingMessages, haServer, fileSystem);

		haServer.registerReplicationInstance(this);

		setSocket(socket);
		setParameters(statisticsInterval);

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
		log.info(getInstanceName() + ": a new client has connected");

		try {
			super.run();
		} finally {
			log.info(getInstanceName() + ": end of Client connection");
			fileSystem.deregisterReplicator(this);
			haServer.deregisterReplicationInstance(this);
		}
	}

	/**
     *
     */
	public boolean isActive()
	{
		return haServer.isActive();
	}


	/**
	 * @return
	 */
	public Boolean isMaster()
	{
		return haServer.isMaster();
	}

	/**
	 * 
	 * @return
	 */
	public Timestamp getStartTime()
	{
		return startTime;
	}

	/**
     * 
     */
	public SyncInfo getSyncInfo(FilePathHa filePath)
	{
		SyncInfo syncInfo = syncInfos.get(filePath);
		if (syncInfo == null) {
			syncInfo = new SyncInfo(filePath);
			syncInfos.put(filePath, syncInfo);
		}
		return syncInfo;
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
	 * @see com.shesse.h2ha.ReplicationProtocolInstance#logStatistics()
	 */
	@Override
	protected void logStatistics()
	{
		log.info(instanceName + ": failoverState = " + haServer.getFailoverState());
	}

	/**
	 * This method may only be called from within the protocol instance thread.
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
		for (FilePathHa fi : discoverExistingFiles()) {
			if (fi.mustReplicate()) {
				String haName = fi.getNormalizedHaName();
				entries.add(haName);
			}
		}

		log.info(getInstanceName() + ": slave has requested list of files - we send " +
			entries.size() + " entries");

		sendToPeer(new ListOfFilesConfirm(entries));
	}


	/**
	 * This method may only be called from within the protocol instance thread.
	 * 
	 * @param entries
	 * @throws IOException
	 */
	public void processSendFileRequestMessage(List<FileRequestData> entries)
		throws IOException
	{
		if (log.isDebugEnabled())
			log.debug("got SendFileRequest: " + entries);
		for (FileRequestData entry : entries) {
			if (entry.getTransmissionMethod() == FileRequestData.TransmissionMethod.FULL) {
				sendFullFile(entry);
			} else if (entry.getTransmissionMethod() == FileRequestData.TransmissionMethod.DELTA) {
				sendFileChecksums(entry);
			}
		}

		sendToPeer(new SendFileConfirmMessage());
	}


	/**
	 * This method may only be called from within the protocol instance thread.
	 * 
	 * @param entry
	 * @throws IOException
	 */
	private void sendFullFile(FileRequestData entry)
		throws IOException
	{
		String haName = entry.getHaName();
		log.info(getInstanceName() + ": sending file " + haName);

		log.debug("sending file " + haName);

		FilePathHa filePath = getFilePathHa(haName);
		SyncInfo syncInfo = getSyncInfo(filePath);

		FileChannel fc = getFileChannel(haName);
		long fileSize = fc.size();
		ByteBuffer buffer = ByteBuffer.allocate(4096);
		long offset = 0;
		fc.position(offset);
		syncInfo.setEndIgnore(fileSize);
		while (offset < fileSize) {
			buffer.clear();
			try {
				int rdlen = buffer.capacity();
				long endrd = offset + rdlen;
				if (endrd > fileSize) {
					rdlen = (int) (fileSize - offset);
					endrd = fileSize;
					buffer.limit(rdlen);
				}

				syncInfo.setBeginIgnore(endrd);

				if (fc instanceof FileChannelHa) {
					((FileChannelHa) fc).readNoCache(buffer);
				} else {
					fc.read(buffer);
				}

				buffer.flip();
				byte[] sendBuffer = new byte[buffer.limit()];
				buffer.get(sendBuffer);
				sendToPeer(new FileDataMessage(haName, offset, sendBuffer));
				offset += rdlen;

			} catch (EOFException x) {
				long nfs = fc.size();
				if (nfs < fileSize) {
					log.debug("adjusting to shrinking random access file");
					fileSize = nfs;
					fc.position(offset);
				} else {
					throw x;
				}
			}
		}

		syncInfo.setBeginIgnore(Long.MAX_VALUE);

		sendToPeer(new EndOfFileMessage(haName, fileSize, filePath.lastModified()));

		log.info(getInstanceName() + ": file sent: " + haName);
	}

	/**
	 * This method may only be called from within the protocol instance thread.
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
		log.info(getInstanceName() + ": sending checksums for file " + haName);

		FilePathHa filePath = getFilePathHa(haName);
		SyncInfo syncInfo = getSyncInfo(filePath);

		// Optimization: shortcut when file size and last modified
		// have not changed
		long length = filePath.size();
		long lastModified = filePath.lastModified();
		syncInfo.setEndIgnore(length);
		if (entry.getExistingFileLength() == length &&
			entry.getExistingLastModified() == lastModified) {
			log.debug("file " + haName + " is unchanged");
			syncInfo.setBeginIgnore(Long.MAX_VALUE);

		} else {

			FileChannel fc = getFileChannel(haName);
			long fileSize = fc.size();
			ByteBuffer buffer = ByteBuffer.allocate(4096);
			long offset = 0;
			fc.position(offset);
			while (offset < fileSize) {
				buffer.clear();
				try {
					int rdlen = buffer.capacity();
					long endrd = offset + rdlen;
					if (endrd > fileSize) {
						rdlen = (int) (fileSize - offset);
						endrd = fileSize;
						buffer.limit(rdlen);
					}

					syncInfo.setBeginIgnore(endrd);

					if (fc instanceof FileChannelHa) {
						((FileChannelHa) fc).readNoCache(buffer);
					} else {
						fc.read(buffer);
					}

					buffer.flip();

					sendToPeer(new FileChecksumMessage(haName, offset, rdlen, computeMd5(buffer)));
					offset += rdlen;

				} catch (EOFException x) {
					long nfs = fc.size();
					if (nfs < fileSize) {
						log.debug("adjusting to shrinking random access file");
						fileSize = nfs;
						fc.position(offset);
					} else {
						throw x;
					}
				}
			}
		}

		syncInfo.setBeginIgnore(Long.MAX_VALUE);
		sendToPeer(new EndOfChecksumsMessage(haName));

		log.info(getInstanceName() + ": checksums sent: " + haName);
	}


	/**
	 * This method may only be called from within the protocol instance thread.
	 * 
	 * @param haName
	 * @param offset
	 * @param length
	 * @throws IOException
	 */
	public void processSendBlockRequestMessage(String haName, long offset, int length)
		throws IOException
	{
		log.debug("got SendBlockRequest - ha=" + haName + ", offset=" + offset + ", length=" +
			length);
		FileChannel fc = getFileChannel(haName);
		long fileSize = fc.size();
		for (;;) {
			try {
				ByteBuffer buffer = ByteBuffer.allocate(length);
				fc.position(offset);
				if (fc instanceof FileChannelHa) {
					((FileChannelHa) fc).readNoCache(buffer);
				} else {
					fc.read(buffer);
				}
				buffer.flip();
				byte[] sendBuffer = new byte[buffer.limit()];
				buffer.get(sendBuffer);
				sendToPeer(new FileDataMessage(haName, offset, sendBuffer));
				return;

			} catch (EOFException x) {
				long nfs = fc.size();
				if (nfs < fileSize) {
					log.debug("adjusting to shrinking random access file");
					fileSize = nfs;
					if (offset + length < fileSize) {
						length = (int) (fileSize - offset);
						if (length < 0)
							length = 0;
					}
				} else {
					throw x;
				}
			}
		}
	}


	/**
	 * This method may only be called from within the protocol instance thread.
	 * 
	 * @param haName
	 * @throws IOException
	 */
	public void processFileProcessedMessage(String haName)
		throws IOException
	{
		log.info(getInstanceName() + ": file has been processed: " + haName);
		FilePathHa fp = getFilePathHa(haName);
		long length = fp.size();
		long lastModified = fp.lastModified();
		sendToPeer(new EndOfFileMessage(haName, length, lastModified));
	}


	/**
	 * This method may only be called from within the protocol instance thread.
	 * 
	 * @throws IOException
	 * 
	 */
	public void processLiveModeRequestMessage()
		throws IOException
	{
		log.debug("got LiveModeRequest");
		log.info(getInstanceName() +
			": slave connection is entering realtime mode - we stay master");
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


	/**
	 * @param dbNameAndParameters
	 * @param adminUser
	 * @param adminPassword
	 * @throws SQLException
	 */
	public void createDatabase(String dbNameAndParameters, String adminUser, String adminPassword)
		throws SQLException
	{
		if (haServer.getFailoverState() != FailoverState.MASTER) {
			throw new SQLException("server is not in a valid state for creating a database: " +
				haServer.getFailoverState());
		}

		String url = "jdbc:h2:" + getFilePathHa(dbNameAndParameters);
		try {
			Class.forName("org.h2.Driver");
		} catch (ClassNotFoundException x) {
			log.error("ClassNotFoundException", x);
			throw new SQLException("ClassNotFoundException", x);
		}
		Connection conn = DriverManager.getConnection(url, adminUser, adminPassword);
		conn.close();
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
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
		{
			instance.processListOfFilesConfirmMessage(entries);

		}

		@Override
		public int getSizeEstimate()
		{
			return 20 * entries.size();
		}

		@Override
		public String toString()
		{
			return "list of files: " + entries;
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
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
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
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
		{
			instance.processFileDataMessage(haName, offset, data);

		}

		@Override
		public int getSizeEstimate()
		{
			return 30 + data.length;
		}

		@Override
		public String toString()
		{
			return "fiel data " + haName + ", offs=" + offset + ", len=" + data.length;
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
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
		{
			instance.processFileChecksumMessage(haName, offset, length, checksum);

		}

		@Override
		public int getSizeEstimate()
		{
			return 32 + checksum.length;
		}

		@Override
		public String toString()
		{
			return "file checksum " + haName + ", offs=" + offset + ", len=" + length;
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
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
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
			return "end of file " + haName + ", len=" + length + ", mod=" + new Date(lastModified);
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
			return "end of checksums " + haName;
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
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
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
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
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

}
