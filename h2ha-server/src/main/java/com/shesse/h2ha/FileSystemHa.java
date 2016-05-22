/**
 * (c) St. Hesse,   2009
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.h2.store.fs.FilePath;

/**
 * 
 * @author sth
 */
public class FileSystemHa
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	private static Logger log = Logger.getLogger(FileSystemHa.class);

	/** */
	private H2HaServer haServer;

	/** */
	private FilePath localBaseDir = null;

	/** */
	private FilePathHa haBaseDir = null;

	/** */
	private ReplicationServerInstance[] replicators = new ReplicationServerInstance[0];

	/** */
	private long replicationRawBytes = 0L;

	/** */
	private long blockCacheLookups = 0L;

	/** */
	private long blockCacheHits = 0L;

	/** */
	private long blockCacheAdds = 0L;


	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @throws TerminateThread
	 */
	public FileSystemHa(H2HaServer haServer, List<String> args)
		throws TerminateThread
	{
		this.haServer = haServer;

		String baseDirArg = H2HaServer.findOptionWithValue(args, "-haBaseDir", null);

		if (baseDirArg == null) {
			throw new TerminateThread("missing flag: -haBaseDir");
		}

		localBaseDir = FilePath.get(baseDirArg).toRealPath();
		haBaseDir = new FilePathHa(this, "", false);
		FilePath.register(haBaseDir);

	}


	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * @param replicator
	 */
	public synchronized void registerReplicator(ReplicationServerInstance replicator)
	{
		// log.debug("registerReplicator - FS="+System.identityHashCode(this));

		for (ReplicationProtocolInstance r : replicators) {
			if (r == replicator)
				return;
		}

		ReplicationServerInstance[] newReplicators =
			new ReplicationServerInstance[replicators.length + 1];
		System.arraycopy(replicators, 0, newReplicators, 0, replicators.length);
		newReplicators[replicators.length] = replicator;
		replicators = newReplicators;
		log.info("new number of replicators: " + replicators.length);
	}


	/**
	 * @param replicator
	 */
	public void deregisterReplicator(ReplicationProtocolInstance replicator)
	{
		// log.debug("deregisterReplicator - FS="+System.identityHashCode(this));

		for (int i = 0; i < replicators.length; i++) {
			if (replicators[i] == replicator) {
				ReplicationServerInstance[] newReplicators =
					new ReplicationServerInstance[replicators.length - 1];
				System.arraycopy(replicators, 0, newReplicators, 0, i);
				System.arraycopy(replicators, i + 1, newReplicators, i, newReplicators.length - i);
				replicators = newReplicators;
				log.info("new number of replicators: " + replicators.length);
				return;
			}
		}
	}

	/**
     * 
     */
	public void sendToReplicators(MessageToClient message)
	{
		// use a local reference to the replicators array to prevent
		// any interference that may be cause by changes from other threads
		// log.debug("sendToReplicators - FS="+System.identityHashCode(this)+", repl="+System.identityHashCode(replicators)+", nrep="+replicators.length);

		ReplicationServerInstance[] reps = replicators;
		for (ReplicationServerInstance rep : reps) {
			rep.send(message);
		}
	}

	/**
     * 
     */
	public void enqueueForAllReplicators(ReplicationMessage message)
	{
		// use a local reference to the replicators array to prevent
		// any interference that may be cause by changes from other threads
		// log.debug("sendToReplicators - FS="+System.identityHashCode(this)+", repl="+System.identityHashCode(replicators)+", nrep="+replicators.length);

		ReplicationServerInstance[] reps = replicators;
		for (ReplicationServerInstance rep : reps) {
			rep.enqueue(message);
		}
	}

	/**
	 * force() has been called on a FileChannelHa. This is most probably the
	 * attempt to ensure persistency on commit. There are different ways to deal
	 * with this with respect to forwarding changes to replicators:
	 * <ul>
	 * <li>Don't delay the sender and accept loosing some milliseconds of
	 * already commited updates. In this case we don't need to do anything here.
	 * <li>wait until all pending updates have been sent out to the replicators
	 * <li>ask replicators to acknowldge that all data has been forced on their
	 * side
	 * <ul>
	 * The first possibility is the most performant and is in line with H2
	 * default beahviour (see
	 * http://www.h2database.com/html/advanced.html#durability_problems). The
	 * current implementation of h2ha uses this variant.
	 */
	public void force()
	{
		// do nothing - see above

		// as an alternative it would be possible to call flushAll or
		// syncAll from here. A small enhancement could add a boolean
		// force argument to syncAll to do a force on the replicator's side
	}


	/**
	 * 
	 */
	public void logStatistics()
	{
		// use a local reference to the replicators array to prevent
		// any interference that may be cause by changes from other threads
		final ReplicationServerInstance[] reps = replicators;
		for (ReplicationServerInstance rep: reps) {
			rep.logStatistics();
		}
	}

	/**
	 * flushes all replicator connections.
	 */
	public void flushAll()
	{
		// log.debug("flushAll - FS="+System.identityHashCode(this)+", repl="+System.identityHashCode(replicators)+", nrep="+replicators.length);

		// use a local reference to the replicators array to prevent
		// any interference that may be cause by changes from other threads
		final ReplicationServerInstance[] reps = replicators;

		if (reps.length == 0) {
			// special case: no replicators registered - optimization!

		} else if (reps.length == 1) {
			// special case: exactly one replicator - optimization!
			try {
				reps[0].flush();
			} catch (InterruptedException x) {
			}

		} else {
			// this will work with any number of replicators
			// at the price of less performance.
			// To minimize waiting time, flushing to multiple
			// replicators is done in parallel threads.
			Thread[] waitThreads = new Thread[reps.length];
			for (int i = 0; i < reps.length; i++) {
				final int fi = i;
				waitThreads[i] = new Thread() {
					public void run()
					{
						try {
							reps[fi].flush();
						} catch (InterruptedException x) {
						}
					}
				};
			}

			for (Thread wt : waitThreads) {
				try {
					wt.join();
				} catch (InterruptedException x) {
				}
			}
		}
	}

	/**
	 * Performs a syncConnection on all replicator connections. It will flush
	 * all connections and wait until all messages have been processed.
	 */
	public void syncAll()
	{
		// log.debug("syncAll - FS="+System.identityHashCode(this)+", repl="+System.identityHashCode(replicators)+", nrep="+replicators.length);

		// use a local reference to the replicators array to prevent
		// any interference that may be cause by changes from other threads
		final ReplicationServerInstance[] reps = replicators;

		if (reps.length == 0) {
			// special case: no replicators registered - optimization!
			log.debug("syncAll with no replicators registered");

		} else if (reps.length == 1) {
			// special case: exactly one replicator - optimization!
			log.debug("syncAll with exactly one replicator ");
			try {
				reps[0].syncConnection();
			} catch (InterruptedException x) {
			} catch (IOException x) {
			}

		} else {
			// this will work with any number of replicators
			// at the price of less performance.
			// To minimize waiting time, syncing with multiple
			// replicators is done in parallel threads.
			log.debug("syncAll with multiple replicators");
			Thread[] waitThreads = new Thread[reps.length];
			for (int i = 0; i < reps.length; i++) {
				final int fi = i;
				waitThreads[i] = new Thread() {
					public void run()
					{
						try {
							reps[fi].syncConnection();
						} catch (InterruptedException x) {
						} catch (IOException x) {
						}
					}
				};
			}

			for (Thread wt : waitThreads) {
				try {
					wt.join();
				} catch (InterruptedException x) {
				}
			}
		}
		log.debug("syncAll finshed");
	}

	/**
	 * @return the haServer
	 */
	public H2HaServer getHaServer()
	{
		return haServer;
	}

	/**
	 * 
	 */
	public FilePath getLocalBaseDir()
	{
		return localBaseDir;
	}

	/**
	 * 
	 */
	public FilePathHa getHaBaseDir()
	{
		return haBaseDir;
	}


	/**
	 * @return the replicationRawBytes
	 */
	public long getReplicationRawBytes()
	{
		return replicationRawBytes;
	}


	/**
	 * @return the blockCacheLookups
	 */
	public long getBlockCacheLookups()
	{
		return blockCacheLookups;
	}


	/**
	 * @return the blockCacheHits
	 */
	public long getBlockCacheHits()
	{
		return blockCacheHits;
	}


	/**
	 * @return the blockCacheAdds
	 */
	public long getBlockCacheAdds()
	{
		return blockCacheAdds;
	}

	/**
	 * @param fileSystem
	 * @param b
	 * @param off
	 * @param len
	 */
	public void sendWrite(FilePathHa filePath, long filePointer, byte[] b, int off,
		int len)
	{
		if (!filePath.mustReplicate()) {
			return;
		}

		replicationRawBytes += len;
		// b may be changed by the caller upon return, so
		// we need to copy the data before placing it into the queue
		// N.b.: MvStore passes quite long buffers (6MB size has been 
		// observed). We split this down to a more manageable size
		// to ensure a better dynamic when transferring data.
		
		synchronized (filePath.getNormalizedHaName().intern()) {
			// We want to make sure that the break-up
			// of a single local file system operation into 
			// multiple messages will not cause race conditions.
			// So we synchronize on an object that uniquely
			// identifies a target file.
			
			final int maxWriteMessageSize = ReplicationProtocolInstance.transferGranularity;
			while (len > 0) {
				int blen = (len > maxWriteMessageSize ? maxWriteMessageSize : len);
				
				byte[] dupData = new byte[blen];
				System.arraycopy(b, off, dupData, 0, blen);
				
				sendToReplicators(new WriteMessage(filePath.getNormalizedHaName(), filePointer, dupData));
				
				off += blen;
				len -= blen;
				filePointer += blen;
			}
		}
		
	}

	/**
	 * @param filePath
	 */
	public void sendCreateDirectory(FilePathHa filePath)
	{
		if (filePath.mustReplicate()) {
			sendToReplicators(new CreateDirectoryMessage(filePath.getNormalizedHaName()));
		}
	}


	/**
	 * @param filePath
	 */
	public void sendCreateFile(FilePathHa filePath)
	{
		if (filePath.mustReplicate()) {
			sendToReplicators(new CreateFileMessage(filePath.getNormalizedHaName()));
		}
	}


	/**
	 * @param filePath
	 */
	public void sendDelete(FilePathHa filePath)
	{
		if (filePath.mustReplicate()) {
			sendToReplicators(new DeleteMessage(filePath.getNormalizedHaName()));
		}
	}


	/**
	 * @param filePath
	 * @param filePath2
	 */
	public void sendMoveTo(FilePathHa from, FilePathHa to, boolean atomicReplace)
	{
		if (from.mustReplicate() && to.mustReplicate()) {
			sendToReplicators(new MoveToMessage(from.getNormalizedHaName(),
				to.getNormalizedHaName(), atomicReplace));

		} else if (from.mustReplicate() || to.mustReplicate()) {
			log.error("attempt to rename with differing replication requirements: " + from +
				" to " + to);
		}
	}


	/**
	 * @param filePath
	 */
	public void sendTruncate(FilePathHa filePath, long fileLength)
	{
		if (filePath.mustReplicate()) {
			sendToReplicators(new TruncateMessage(filePath.getNormalizedHaName(), fileLength));
		}
	}

	/**
	 * @param filePath
	 */
	public void sendClose(FilePathHa filePath)
	{
		if (filePath.mustReplicate()) {
			long lastModified = filePath.getBasePath().lastModified();
			sendToReplicators(new CloseMessage(filePath.getNormalizedHaName(), lastModified));
		}
	}


	/**
	 * @param filePathHa
	 */
	public void sendSetReadOnly(FilePathHa filePath)
	{
		if (filePath.mustReplicate()) {
			sendToReplicators(new SetReadOnlyMessage(filePath.getNormalizedHaName()));
		}
	}

	/**
	 * @param string
	 * @param data2
	 * @param l
	 * @param m
	 */
	private static void logBytes(Logger log, String message, byte[] data, long offs, long length)
	{
		if (length > 10) {
			length = 10;
		}
		dumpToDebug(log, message, data, (int)offs, (int)length);
	}
	
	/**
	 * @param log
	 * @param buffer
	 * @param offs
	 * @param length
	 */
	private static void dumpToDebug(Logger log, String message, byte[] buffer, int offs, int length)
	{
		if (log.isDebugEnabled()) {
			message += ": ";
			while (length > 0) {
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < length && i < 32; i++) {
					sb.append(String.format("%02x ", buffer[offs+i]));
				}
				log.debug(message + sb);
				message = "  ";
				length -= 32;
				offs += 32;
			}
		}
	}




	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////
	/** */
	private static class CreateDirectoryMessage
		extends MessageToClient
	{
		private static final long serialVersionUID = 1L;
		String fileName;

		CreateDirectoryMessage(String fileName)
		{
			this.fileName = fileName;
		}

		@Override
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
		{
			instance.processCreateDirectoryMessage(fileName);
		}

		@Override
		public int getSizeEstimate()
		{
			return 20;
		}

		@Override
		public String toString()
		{
			return "create dirs " + fileName;
		}
	}

	/** */
	private static class CreateFileMessage
		extends MessageToClient
	{
		private static final long serialVersionUID = 1L;
		String fileName;

		CreateFileMessage(String fileName)
		{
			this.fileName = fileName;
		}

		@Override
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
		{
			instance.processCreateFileMessage(fileName);
		}

		@Override
		public int getSizeEstimate()
		{
			return 20;
		}

		@Override
		public String toString()
		{
			return "create new file " + fileName;
		}
	}

	/** */
	private static class WriteMessage
		extends MessageToClient
	{
		private static final long serialVersionUID = 1L;
		String haName;
		long filePointer;
		byte[] data;
		
		// only used for testing
		private static final long samplingPoint = 65536;

		WriteMessage(String haName, long filePointer, byte[] data)
		{
			this.haName = haName;
			this.filePointer = filePointer;
			this.data = data;
			
			// only used for testing
			if (filePointer <= samplingPoint && filePointer+data.length > samplingPoint) {
				logBytes(log, "sending for "+samplingPoint, data, samplingPoint-filePointer, filePointer+data.length - samplingPoint);
			}
		}

		@Override
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
		{
			// only used for testing
			if (filePointer <= samplingPoint && filePointer+data.length > samplingPoint) {
				logBytes(log, "processing for "+samplingPoint, data, samplingPoint-filePointer, filePointer+data.length - samplingPoint);
			}
			instance.processWriteMessage(haName, filePointer, data);
		}

		/**
		 * {@inheritDoc}
		 * 
		 * @see com.shesse.h2ha.ReplicationMessage#needToSend(com.shesse.h2ha.ReplicationProtocolInstance)
		 */
		@Override
		public boolean needToSend(ReplicationProtocolInstance instance)
		{
			if (instance instanceof ReplicationServerInstance) {
				ReplicationServerInstance rsi = (ReplicationServerInstance) instance;
				FilePathHa filePath = new FilePathHa(rsi.fileSystem, haName, true);
				SyncInfo syncInfo = rsi.getSyncInfo(filePath);

				if (filePointer < syncInfo.getBeginIgnore()) {
					log.debug("need to send " + haName + ", offset=" + filePointer +
						" < beginIgn=" + syncInfo.getBeginIgnore());
					return true;
				}
				if (filePointer + data.length > syncInfo.getEndIgnore()) {
					log.debug("need to send " + haName + ", offset=" + (filePointer + data.length) +
						" > endIgn=" + syncInfo.getEndIgnore());
					return true;
				}

				log.debug("don't need to send " + haName + ", offset=" + filePointer + ", length=" +
					data.length + " within " + syncInfo.getBeginIgnore() + " - " +
					syncInfo.getEndIgnore());
				return false;

			} else {
				return true;
			}
		}

		@Override
		public int getSizeEstimate()
		{
			return 30 + data.length;
		}

		@Override
		public String toString()
		{
			return "write message " + haName + ", offs=" + filePointer + ", len=" + data.length;
		}
	}


	/** */
	private static class DeleteMessage
		extends MessageToClient
	{
		private static final long serialVersionUID = 1L;
		String fileName;

		DeleteMessage(String fileName)
		{
			this.fileName = fileName;
		}

		@Override
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
		{
			instance.processDeleteMessage(fileName);
		}

		@Override
		public int getSizeEstimate()
		{
			return 20;
		}

		@Override
		public String toString()
		{
			return "delete " + fileName;
		}
	}

	/** */
	private static class MoveToMessage
		extends MessageToClient
	{
		private static final long serialVersionUID = 1L;
		String oldName;
		String newName;
		boolean atomicReplace;

		MoveToMessage(String oldName, String newName, boolean atomicReplace)
		{
			this.oldName = oldName;
			this.newName = newName;
			this.atomicReplace = atomicReplace;
		}

		@Override
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
		{
			instance.processMoveToMessage(oldName, newName, atomicReplace);
		}

		@Override
		public int getSizeEstimate()
		{
			return 50;
		}

		@Override
		public String toString()
		{
			return "rename " + oldName + " to " + newName;
		}
	}

	/** */
	private static class TruncateMessage
		extends MessageToClient
	{
		private static final long serialVersionUID = 1L;
		String haName;
		long newLength;

		TruncateMessage(String haName, long newLength)
		{
			this.haName = haName;
			this.newLength = newLength;
		}

		@Override
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
		{
			instance.processTruncateMessage(haName, newLength);
		}

		@Override
		public int getSizeEstimate()
		{
			return 30;
		}

		@Override
		public String toString()
		{
			return "set file length " + haName + ": " + newLength;
		}
	}


	/** */
	private static class CloseMessage
		extends MessageToClient
	{
		private static final long serialVersionUID = 1L;
		String haName;
		long lastModified;

		CloseMessage(String haName, long lastModified)
		{
			this.haName = haName;
			this.lastModified = lastModified;
		}

		@Override
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
		{
			instance.processCloseMessage(haName, lastModified);
		}

		@Override
		public int getSizeEstimate()
		{
			return 30;
		}

		@Override
		public String toString()
		{
			return "close " + haName + ", mod=" + new Date(lastModified);
		}
	}

	/** */
	private static class SetReadOnlyMessage
		extends MessageToClient
	{
		private static final long serialVersionUID = 1L;
		String fileName;

		SetReadOnlyMessage(String fileName)
		{
			this.fileName = fileName;
		}

		@Override
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
		{
			instance.processSetReadOnlyMessage(fileName);
		}

		@Override
		public int getSizeEstimate()
		{
			return 20;
		}

		@Override
		public String toString()
		{
			return "set read only " + fileName;
		}
	}

}
