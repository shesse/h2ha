/**
 * (c) St. Hesse,   2009
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

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
	private enum BlocksizeLearningState {
		INITIAL, LEARNING, LEARNED
	};

	/** */
	private BlocksizeLearningState blocksizeLearningState = BlocksizeLearningState.INITIAL;

	/** */
	private int learnedBlocksize = 0;

	/** */
	private int blocksizeOccurenceCount = 0;

	/** */
	private LinkedHashMap<String, byte[]> blockCache;

	/** */
	private static final int REQUIRED_LEARN_COUNT = 5;

	/** */
	private int haCacheSize = DEFAULT_CACHE_SIZE;

	/** */
	private static final int DEFAULT_CACHE_SIZE = 1000;

	/** */
	private long replicationRawBytes = 0L;

	/** */
	private long replicationCroppedBytes = 0L;

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
		haCacheSize = H2HaServer.findOptionWithInt(args, "-haCacheSize", DEFAULT_CACHE_SIZE);

		if (baseDirArg == null) {
			throw new TerminateThread("missing flag: -haBaseDir");
		}

		localBaseDir = FilePath.get(baseDirArg).toRealPath();
		haBaseDir = new FilePathHa(this, "", false);
		FilePath.register(haBaseDir);

		if (haCacheSize > 0) {
			blockCache = new LinkedHashMap<String, byte[]>((haCacheSize * 4) / 3, 0.75f, true) {
				private static final long serialVersionUID = 1L;

				/**
				 * {@inheritDoc}
				 * 
				 * @see java.util.LinkedHashMap#removeEldestEntry(java.util.Map.Entry)
				 */
				@Override
				protected boolean removeEldestEntry(Entry<String, byte[]> eldest)
				{
					return size() > haCacheSize;
				}
			};

		} else {
			blockCache = null;
		}
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
	 * @return the replicationCompressedBytes
	 */
	public long getReplicationCroppedBytes()
	{
		return replicationCroppedBytes;
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
	 * @param b
	 * @param off
	 * @param len
	 */
	public synchronized void cacheRead(FilePathHa filePath, long filePointer, byte[] b, int off,
		int len)
	{
		if (blockCache == null) {
			return;
		}

		if (blocksizeLearningState == BlocksizeLearningState.INITIAL) {
			blocksizeLearningState = BlocksizeLearningState.LEARNING;
			learnedBlocksize = len;
			blocksizeOccurenceCount = 1;
			log.debug("initial blocksize hypothesis: " + len);

		} else if (blocksizeLearningState == BlocksizeLearningState.LEARNING) {
			long blockno = filePointer / learnedBlocksize;
			if (learnedBlocksize == len && blockno * learnedBlocksize == filePointer) {
				blocksizeOccurenceCount++;
				if (blocksizeOccurenceCount > REQUIRED_LEARN_COUNT) {
					blocksizeLearningState = BlocksizeLearningState.LEARNED;
					log.debug("learned blocksize: " + len);
				} else {
					log.debug(blocksizeOccurenceCount + "th blocksize repetition: " + len);
				}

			} else {
				learnedBlocksize = len;
				blocksizeOccurenceCount = 1;
				log.debug("blocksize hypothesis mismatch: " + len);

			}

		} else {
			// We have already learned a blocksize. We will put any read
			// for whole blocks into a LRU cache. to allow computing deltas
			// in subsequent writes.
			// There is no need to make sure that all data read are but
			// into the cache - we are only interested in reads of whole
			// blocks. Other read sizes or stream reads will be ignored.
			// In the worst case this will one result in worse compression
			// be computing deltas.
			long blockno = filePointer / learnedBlocksize;
			if (learnedBlocksize == len && blockno * learnedBlocksize == filePointer) {
				// incrementing blocksizeOccurenceCount after learning has
				// completed
				// is basically pointless. However, we may want to add some
				// statistics
				// at a later time, so we keep incrementing the count
				blocksizeOccurenceCount++;

				// b may be changed by the caller upon return, so
				// we need to copy the data before placing it in the cache
				byte[] dupData = new byte[len];
				System.arraycopy(b, off, dupData, 0, len);
				String cacheKey = cacheKey(filePath, blockno);
				blockCache.put(cacheKey, dupData);
				blockCacheAdds++;
				log.debug("read: adding to cache: '" + cacheKey + "'");
			}
		}
	}

	/**
	 * @param fileSystem
	 * @param b
	 * @param off
	 * @param len
	 */
	public void compressAndSendWrite(FilePathHa filePath, long filePointer, byte[] b, int off,
		int len)
	{
		if (!filePath.mustReplicate()) {
			return;
		}
		
		// All writing traffic will pass through this point. So we
		// may use the block cache to limit the amount of data sent
		// to the peer system to only the parts that have changed.
		// The current implementation assumes that the block will
		// usually have leading and trailing unchanged bytes and
		// cuts them off.
		byte[] dupData;
		synchronized (this) {
			boolean addToCache = false;
			replicationRawBytes += len;
			if (blockCache != null && blocksizeLearningState == BlocksizeLearningState.LEARNED) {
				long blockno = filePointer / learnedBlocksize;
				if (learnedBlocksize == len && blockno * learnedBlocksize == filePointer) {
					// blocked write
					String cacheKey = cacheKey(filePath, blockno);
					byte[] oldData = blockCache.get(cacheKey);
					blockCacheLookups++;
					if (oldData != null) {
						log.debug("found in cache: '" + cacheKey + "'");
						blockCacheHits++;
						// we found a cached entry - modifying offset and length
						int ofirst = 0;
						for (; ofirst < oldData.length; ofirst++) {
							if (b[off] == oldData[ofirst]) {
								off++;
								filePointer++;
								len--;
							} else {
								break;
							}
						}

						int blast = off + len - 1;
						int olast = oldData.length - 1;
						for (; olast >= ofirst; olast--) {
							if (b[blast] == oldData[olast]) {
								blast--;
								len--;
							} else {
								break;
							}
						}
						log.debug("adjusted bounds to " + ofirst + " - " + olast + ", len=" + len);

						if (len == 0) {
							return;
						}

						// we need to remember the update in case this block
						// gets updated again
						System.arraycopy(b, off, oldData, ofirst, len);

					} else {
						// block sized write but no cached entry - we will add
						// the block to the cache after we have created a copy
						// of the data
						log.debug("not in cache: '" + cacheKey + "'");
						addToCache = true;
					}

				} else {
					// not a block sized write. We need to flush
					// all entries from the cache that may have been invalidated
					// by this write
					log.debug("non-blocked: " + filePath.getNormalizedHaName() + " - p=" +
						filePointer + ", l=" + len);
					long lastBlockModified = (filePointer + len - 1) / learnedBlocksize;
					while (blockno <= lastBlockModified) {
						blockCache.remove(cacheKey(filePath, blockno));
						blockno++;
					}
				}

			} else {
				log.debug("blocksize not yet learned");
				// we don't have a blocksize yet - we can assume that
				// the cache is still empty so that we don't need to flush
				// any entries
			}

			// b may be changed by the caller upon return, so
			// we need to copy the data before placing it into the queue
			dupData = new byte[len];
			System.arraycopy(b, off, dupData, 0, len);
			replicationCroppedBytes += len;

			if (addToCache) {
				long blockno = filePointer / learnedBlocksize;
				String cacheKey = cacheKey(filePath, blockno);
				blockCache.put(cacheKey, dupData);
				blockCacheAdds++;
				log.debug("write: adding to cache: '" + cacheKey + "'");
			}

		}

		sendToReplicators(new WriteMessage(filePath.getNormalizedHaName(), filePointer, dupData));
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
	public void sendMoveTo(FilePathHa from, FilePathHa to)
	{
		if (from.mustReplicate() && to.mustReplicate()) {
			sendToReplicators(new MoveToMessage(from.getNormalizedHaName(),
				to.getNormalizedHaName()));
	
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
     * 
     */
	private String cacheKey(FilePathHa filePath, long blockno)
	{
		return filePath.getNormalizedHaName() + ":" + blockno;
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

		WriteMessage(String haName, long filePointer, byte[] data)
		{
			this.haName = haName;
			this.filePointer = filePointer;
			this.data = data;
		}

		@Override
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
		{
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

		MoveToMessage(String oldName, String newName)
		{
			this.oldName = oldName;
			this.newName = newName;
		}

		@Override
		protected void processMessageToClient(ReplicationClientInstance instance)
			throws Exception
		{
			instance.processMoveToMessage(oldName, newName);
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
