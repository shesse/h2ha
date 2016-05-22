/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.h2.store.fs.FilePath;

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
	private Map<FilePathHa, FileChannel> openFiles = new HashMap<FilePathHa, FileChannel>();


	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
     */
	public ServerSideProtocolInstance(String instanceName, int maxQueueSize, H2HaServer haServer,
										FileSystemHa fileSystem)
	{
		super(instanceName, maxQueueSize);

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
			sendToPeer(new StatusMessage(getCurrentFailoverState(), haServer.getMasterPriority(),
				haServer.getUuid()));

		} else {
			// not in sender thread: enqueue message that will send a heartbeat
			// when processed.
			// The heartbeat will carry state information of the time when it
			// is sent out.
			enqueue(new ReplicationMessage() {
				private static final long serialVersionUID = 1L;

				@Override
				protected void process(ReplicationProtocolInstance instance)
					throws Exception
				{
					try {
						sendToPeer(new StatusMessage(getCurrentFailoverState(),
							haServer.getMasterPriority(), haServer.getUuid()));
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
	protected Set<FilePathHa> discoverExistingFiles()
	{
		Set<FilePathHa> existingFiles = new HashSet<FilePathHa>();

		synchronized (fileSystem) {
			discoverFilesWithinDirectory(existingFiles, fileSystem.getHaBaseDir());
		}

		return existingFiles;
	}

	/**
     * 
     */
	private void discoverFilesWithinDirectory(Set<FilePathHa> existingFiles, FilePathHa directory)
	{
		log.debug("discovering local files within " + directory);
		for (FilePath subPath : directory.newDirectoryStream()) {
			FilePathHa sub = (FilePathHa) subPath;
			String haName = sub.getNormalizedHaName();
			if (sub.isDirectory()) {
				log.debug("file " + haName + " is a subdirectory");
				discoverFilesWithinDirectory(existingFiles, sub);

			} else if (sub.exists()) {
				if (sub.isDatabaseFile()) {
					existingFiles.add(sub);
				}
			}
		}
		log.debug("end of local files discovery within " + directory);
	}

	/**
	 * Berechnet die MD5 Quersumme aus dem übergebenen Buffer
	 * 
	 * @param in
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 */
	protected byte[] computeMd5(ByteBuffer buffer)
	{
		if (buffer.hasArray()) {
			int offs = buffer.arrayOffset() + buffer.position();
			int length = buffer.limit() - buffer.position();
			md5Digest.reset();
			md5Digest.update(buffer.array(), offs,length);
			byte[] ret =  md5Digest.digest();
			
			if (log.isDebugEnabled()) {
				StringBuilder sb = new StringBuilder();
				long mysum = 0;
				for (int i = 0; i < length; i++) {
					mysum = (mysum * 113) + ( buffer.array()[offs+i] & 0xff);
				}
				for (int i = 0; i < length && i < 10; i++) {
					sb.append(String.format("%02x ", buffer.array()[offs+i]));
				}
				String bufDump = sb.toString();
				
				sb = new StringBuilder();
				for (int i = 0; i < ret.length; i++) {
					sb.append(String.format("%02x ", ret[i]));
				}
				String hashDump = sb.toString();

				
				log.debug("md5 offs="+offs+", len="+length+": "+bufDump+"-> "+hashDump+" (mysum="+mysum);
			}
			return ret;

		} else {
			throw new IllegalArgumentException("only array based buffers are supported");
		}
	}
	
	/**
     * 
     */
	protected FilePathHa getFilePathHa(String haName)
	{
		return new FilePathHa(fileSystem, haName, false);
	}

	/**
	 * @throws IOException
	 * 
	 */
	protected FileChannel getFileChannel(String haName)
		throws IOException
	{
		return getFileChannel(getFilePathHa(haName));
	}

	/**
	 * @throws IOException
	 * 
	 */
	protected FileChannel getFileChannel(FilePathHa fp)
		throws IOException
	{
		FileChannel fc = openFiles.get(fp);
		if (fc == null) {
			fc = fp.open("rw");
			openFiles.put(fp, fc);
		}

		return fc;
	}

	/**
	 * @throws IOException
	 * 
	 */
	protected FileChannel getBaseFileChannel(String haName)
		throws IOException
	{
		FileChannel fc = getFileChannel(haName);
		if (fc instanceof FileChannelHa) {
			fc = ((FileChannelHa)fc).getBaseChannel();
		}
		return fc;
	}

	/**
	 * @throws IOException
	 * 
	 */
	protected FileChannel getBaseFileChannel(FilePathHa fp)
		throws IOException
	{
		FileChannel fc = getFileChannel(fp);
		if (fc instanceof FileChannelHa) {
			fc = ((FileChannelHa)fc).getBaseChannel();
		}
		return fc;
	}

	/**
	 * 
	 * @param haName
	 * @throws IOException
	 */
	protected void closeFileChannel(String haName, long lastModified)
		throws IOException
	{
		closeFileObject(getFilePathHa(haName), lastModified);
	}

	/**
	 * 
	 * @param haName
	 * @throws IOException
	 */
	protected void closeFileObject(FilePathHa filePath, long lastModified)
		throws IOException
	{
		FileChannel fc = openFiles.remove(filePath);
		if (fc != null) {
			fc.close();
		}
		if (filePath.exists()) {
			filePath.lastModified(lastModified);
		}
	}

	/**
     * 
     */
	protected void closeAllFileObjects()
	{
		for (FileChannel fo : openFiles.values()) {
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