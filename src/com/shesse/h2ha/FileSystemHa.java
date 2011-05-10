/**
 * (c) St. Hesse,   2009
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.h2.message.DbException;
import org.h2.store.fs.FileObject;
import org.h2.store.fs.FileSystem;
import org.h2.store.fs.FileSystemDisk;

/**
 * 
 * @author sth
 */
public class FileSystemHa
    extends FileSystem
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(FileSystemHa.class);

    /** */
    private FileSystem baseFileSystem = FileSystemDisk.getInstance();

    /** */
    private String haBaseDir = null;

    /** */
    private String haBaseDirAbsoluteNormalized = null;

    /** */
    private Map<String, FileInfo> fileByHaName = new HashMap<String, FileInfo>();

    /** */
    private Map<String, FileInfo> fileByLocalName = new HashMap<String, FileInfo>();

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
    private static final String haProtocol = "ha:";

    /** */
    private static final int DEFAULT_CACHE_SIZE = 1000;


    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public FileSystemHa(String[] args)
    {
	log.debug("HaFileSystem()");

	for (int i = 0; i < args.length - 1; i++) {
	    if (args[i].equals("-haBaseDir")) {
		haBaseDir = args[i + 1];
		
	    } else if (args[i].equals("-haCacheSize")) {
                try {
                    haCacheSize = Integer.parseInt(args[++i]);
                } catch (NumberFormatException x) {
                    log.error("inhalid haCacheSize: "+x);
                }
            }
	}

	if (haBaseDir != null) {
	    haBaseDirAbsoluteNormalized = baseFileSystem.getCanonicalPath(haBaseDir);
	    register(this);
	}

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
     * Guarantees a non-null return value
     */
    public synchronized FileInfo getFileInfoForHaName(String haName)
    {
	FileInfo fi = fileByHaName.get(haName);

	if (fi == null) {
	    if (isHaName(haName)) {
		String localName = haName.substring(3);
		while (localName.startsWith("/"))
		    localName = localName.substring(1);

		// don't allow .. references within HA names
		localName = localName.replaceAll("/\\.\\./", "/");
		if (localName.startsWith("../"))
		    localName = localName.substring(3);
		if (localName.endsWith("/.."))
		    localName = localName.substring(0, localName.length() - 3);
		if (localName.equals(".."))
		    localName = "";

		if (localName.equals(""))
		    localName = haBaseDirAbsoluteNormalized;
		else
		    localName = haBaseDirAbsoluteNormalized + "/" + localName;

		localName = baseFileSystem.getCanonicalPath(localName);

		// there may be cases where multiple haName refer to the same
		// file. However, localName is normalized and must therefore be
		// unique. We try a lookup using this unique key to resolve such
		// cases of ambiguity.
		fi = fileByLocalName.get(localName);

		// no ambiguity - we need a new FileInfo.
		if (fi == null) {
		    log.debug("new FileInfo for HA file: " + haName + " -> " + localName);
		    fi = new FileInfo(haName, localName, true);

		} else {
		    log.debug("alternative HA name: " + haName + " for local=" + localName);
		    fi.addAlternativeHaName(haName);
		}

	    } else {
		// not a ha: URL - we create a non-ha file entry
		// log.debug("new FileInfo for non HA url: "+haName);
		// fi = new FileInfo(haName, haName, false);

		throw new IllegalStateException("attempt to pass non-HA URL to FileSystemHa: " +
		    haName);
	    }

	    addFileInfo(fi);
	}

	return fi;
    }

    /**
     * @param localName
     *            must already be normalized
     */
    public synchronized FileInfo getFileInfoForLocalName(String localName)
    {
	FileInfo fi = fileByLocalName.get(localName);
	if (fi == null) {
	    // not yet known - we build a HA name algorithmically

	    if (localName.startsWith(haBaseDirAbsoluteNormalized + "/")) {
		String haName = localName.substring(haBaseDirAbsoluteNormalized.length() + 1);
		while (haName.startsWith("/"))
		    haName = haName.substring(1);
		haName = haProtocol + "///" + haName;
		log.debug("new FileInfo for local file within HA directory: " + haName + " -> " +
		    localName);
		fi = new FileInfo(haName, localName, true);

	    } else if (localName.equals(haBaseDirAbsoluteNormalized)) {
		String haName = haProtocol + "///";
		log.debug("new FileInfo for HA base directory: " + haName + " -> " + localName);
		fi = new FileInfo(haName, localName, true);

	    } else {
		// not a HA file - we create a non-ha file entry
		log.debug("new FileInfo for local file outside of : " + localName);
		fi = new FileInfo(localName, localName, false);
	    }

	    addFileInfo(fi);
	}
	return fi;
    }

    /**
     * 
     */
    private String haNameToLocal(String haName)
    {
	FileInfo fi = getFileInfoForHaName(haName);

	return fi.getLocalName();
    }

    /**
     * 
     */
    private String localNameToHa(String localName)
    {
	FileInfo fi = getFileInfoForLocalName(localName);
	return fi.getHaName();
    }

    /**
     * 
     */
    private synchronized void addFileInfo(FileInfo fileInfo)
    {
	log.debug("addFileInfo " + fileInfo.getHaName() + " -> " + fileInfo.getLocalName() +
	    ", aliases=" + fileInfo.getAllHaNames());
	fileByLocalName.put(fileInfo.getLocalName(), fileInfo);
	for (String haName : fileInfo.getAllHaNames()) {
	    fileByHaName.put(haName, fileInfo);
	}
    }

    /**
     * 
     */
    private synchronized void removeFileInfo(FileInfo fileInfo)
    {
	fileByLocalName.remove(fileInfo.getLocalName());
	for (String haName : fileInfo.getAllHaNames()) {
	    fileByHaName.remove(haName);
	}
    }

    /**
     * 
     */
    private boolean isHaName(String name)
    {
	return name.startsWith(haProtocol);
    }

    /**
     * 
     */
    public static String getRoot()
    {
	return haProtocol + "///";
    }

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
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#accepts(java.lang.String)
     */
    @Override
    protected boolean accepts(String fileName)
    {
	if (fileName.startsWith(haProtocol)) {
	    log.debug("accepted file name for HA: " + fileName);
	    return true;
	} else {
	    return false;
	}
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#canWrite(java.lang.String)
     */
    @Override
    public boolean canWrite(String fileName)
    {
	log.debug("canWrite " + fileName);
	return baseFileSystem.canWrite(haNameToLocal(fileName));
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#createDirs(java.lang.String)
     */
    @Override
    public void createDirs(String fileName)
    {
	log.debug("createDirs " + fileName);
	FileInfo fi = getFileInfoForHaName(fileName);
	baseFileSystem.createDirs(fi.getLocalName());

	if (fi.mustReplicate()) {
	    sendToReplicators(new CreateDirsMessage(fileName));
	}
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#createNewFile(java.lang.String)
     */
    @Override
    public boolean createNewFile(String fileName)
    {
	log.debug("createNewFile " + fileName);
	FileInfo fi = getFileInfoForHaName(fileName);
	if (baseFileSystem.createNewFile(fi.getLocalName())) {
	    if (fi.mustReplicate()) {
		sendToReplicators(new CreateNewFileMessage(fileName));
	    }

	    return true;

	} else {
	    return false;
	}
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#createTempFile(java.lang.String,
     *      java.lang.String, boolean, boolean)
     */
    @Override
    public String createTempFile(String prefix, String suffix, boolean deleteOnExit,
				 boolean inTempDir)
	throws IOException
    {
	log.debug("createTempFile " + prefix + "*" + suffix);
	return baseFileSystem.createTempFile(haNameToLocal(prefix), suffix, deleteOnExit, inTempDir);
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#tryDelete(java.lang.String)
     */
    @Override
    public boolean tryDelete(String fileName)
    {
	log.debug("tryDelete " + fileName);
	FileInfo fileInfo = getFileInfoForHaName(fileName);
	if (baseFileSystem.tryDelete(fileInfo.getLocalName())) {
	    removeFileInfo(fileInfo);
	    if (fileInfo.mustReplicate()) {
		sendToReplicators(new TryDeleteMessage(fileName));
	    }
	    return true;

	} else {
	    return false;
	}
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#delete(java.lang.String)
     */
    @Override
    public void delete(String fileName)
    {
	log.debug("delete " + fileName);
	FileInfo fileInfo = getFileInfoForHaName(fileName);
	baseFileSystem.delete(fileInfo.getLocalName());
	if (fileInfo.mustReplicate()) {
	    sendToReplicators(new DeleteMessage(fileName));
	}
	removeFileInfo(fileInfo);
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#deleteRecursive(java.lang.String, boolean
     *      tryOnly)
     */
    @Override
    public void deleteRecursive(String directory, boolean tryOnly)
    {
	log.debug("deleteRecursive " + directory);
	FileInfo dirInfo = getFileInfoForHaName(directory);

	String localName = dirInfo.getLocalName();
	baseFileSystem.deleteRecursive(localName, tryOnly);

	List<FileInfo> toBeDeleted = new ArrayList<FileInfo>();
	for (Iterator<FileInfo> it = fileByHaName.values().iterator(); it.hasNext();) {
	    FileInfo f = it.next();
	    if (f.getLocalName().startsWith(localName + "/") || f.getLocalName().equals(localName)) {
		toBeDeleted.add(f);
	    }
	}

	for (FileInfo fileInfo : toBeDeleted) {
	    removeFileInfo(fileInfo);
	}

	if (dirInfo.isWithinHaTree()) {
	    sendToReplicators(new DeleteRecursiveMessage(directory, tryOnly));
	}
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#exists(java.lang.String)
     */
    @Override
    public boolean exists(String fileName)
    {
	log.debug("exists " + fileName);
	return baseFileSystem.exists(haNameToLocal(fileName));
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#fileStartsWith(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public boolean fileStartsWith(String fileName, String prefix)
    {
	log.debug("fileStartsWith " + fileName + " pfx=" + prefix);
	return baseFileSystem.fileStartsWith(haNameToLocal(fileName), prefix);
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#getCanonicalPath(java.lang.String)
     */
    @Override
    public String getCanonicalPath(String fileName)
    {
	log.debug("getCanonicalPath " + fileName + " - local=" + haNameToLocal(fileName));
	String ret = localNameToHa(baseFileSystem.getCanonicalPath(haNameToLocal(fileName)));
	log.debug("getCanonicalPath " + fileName + " -> " + ret);
	return ret;
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#getFileName(java.lang.String)
     */
    @Override
    public String getFileName(String name)
    {
	String ret = baseFileSystem.getFileName(haNameToLocal(name));
	log.debug("getFileName " + name + " -> " + ret);
	return ret;
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#getLastModified(java.lang.String)
     */
    @Override
    public long getLastModified(String fileName)
    {
	log.debug("getLastModified " + fileName);
	return baseFileSystem.getLastModified(haNameToLocal(fileName));
    }

    /**
     * 
     */
    public void setLastModified(String fileName, long millis)
    {
	log.debug("setLastModified " + fileName);
	File file = new File(haNameToLocal(fileName));
	file.setLastModified(millis);
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#getParent(java.lang.String)
     */
    @Override
    public String getParent(String fileName)
    {
	String ret = localNameToHa(baseFileSystem.getParent(haNameToLocal(fileName)));
	log.debug("getParent " + fileName + " -> " + ret);
	return ret;
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#isAbsolute(java.lang.String)
     */
    @Override
    public boolean isAbsolute(String fileName)
    {
	log.debug("isAbsolute " + fileName);
	return baseFileSystem.isAbsolute(haNameToLocal(fileName));
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#isDirectory(java.lang.String)
     */
    @Override
    public boolean isDirectory(String fileName)
    {
	log.debug("isDirectory " + fileName);
	return baseFileSystem.isDirectory(haNameToLocal(fileName));
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#isReadOnly(java.lang.String)
     */
    @Override
    public boolean isReadOnly(String fileName)
    {
	log.debug("isReadOnly " + fileName);
	return baseFileSystem.isReadOnly(haNameToLocal(fileName));
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#length(java.lang.String)
     */
    @Override
    public long length(String fileName)
    {
	log.debug("length " + fileName);
	return baseFileSystem.length(haNameToLocal(fileName));
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#listFiles(java.lang.String)
     */
    @Override
    public String[] listFiles(String directory)
    {
	log.debug("listFiles " + directory);
	String[] localNames = baseFileSystem.listFiles(haNameToLocal(directory));
	for (int i = 0; i < localNames.length; i++) {
	    localNames[i] = localNameToHa(localNames[i]);
	}
	return localNames;
    }

    /**
     * @throws SQLException
     * 
     */
    public FileInfo[] listFileInfos(String directory)
    {
	String[] localNames = baseFileSystem.listFiles(haNameToLocal(directory));
	FileInfo[] fileInfos = new FileInfo[localNames.length];
	for (int i = 0; i < localNames.length; i++) {
	    fileInfos[i] = getFileInfoForLocalName(localNames[i]);
	}
	return fileInfos;
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#openFileInputStream(java.lang.String)
     */
    @Override
    public InputStream openFileInputStream(String fileName)
	throws IOException
    {
	log.debug("openFileInputStream " + fileName);
	return baseFileSystem.openFileInputStream(haNameToLocal(fileName));
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#openFileObject(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public FileObject openFileObject(String fileName, String mode)
	throws IOException
    {
	log.debug("openFileObject " + fileName);
	FileInfo fi = getFileInfoForHaName(fileName);
	FileObject baseFileObject = baseFileSystem.openFileObject(fi.getLocalName(), mode);

	if (fi.mustReplicate()) {
	    return new FileObjectHa(this, fi, baseFileObject);
	} else {
	    return baseFileObject;
	}
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#openFileOutputStream(java.lang.String,
     *      boolean)
     */
    @Override
    public OutputStream openFileOutputStream(String fileName, boolean append)
    {
	log.debug("openFileOutputStream " + fileName);
	FileInfo fi = getFileInfoForHaName(fileName);
	if (fi.mustReplicate()) {
	    try {
		FileObject baseFileObject = openFileObject(fileName, "rw");
		return new OutputStreamHa(this, fileName, baseFileObject, append);
	    } catch (IOException x) {
		throw DbException.convertIOException(x, "cannot open file '" + fileName);
	    }

	} else {
	    return baseFileSystem.openFileOutputStream(fi.getLocalName(), append);
	}

	/*
	 * if (isHaName(fileName)) { } else { return
	 * baseFileSystem.openFileOutputStream(fileName, append); }
	 */
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#rename(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public void rename(String oldName, String newName)
    {
	log.debug("rename " + oldName + " to " + newName);
	FileInfo oldFile = getFileInfoForHaName(oldName);
	FileInfo newFile = getFileInfoForHaName(newName);

	baseFileSystem.rename(oldFile.getLocalName(), newFile.getLocalName());

	newFile.setStatusInfo(oldFile);
	removeFileInfo(oldFile);

	if (oldFile.mustReplicate() && newFile.mustReplicate()) {
	    sendToReplicators(new RenameMessage(oldName, newName));

	} else if (oldFile.mustReplicate() || newFile.mustReplicate()) {
	    log.error("attempt to rename with differing replication requirements: " + oldFile +
		" to " + newFile);
	}
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#setReadOnly(java.lang.String)
     */
    @Override
    public boolean setReadOnly(String fileName)
    {
	log.debug("setReadOnly " + fileName);
	FileInfo fileInfo = getFileInfoForHaName(fileName);
	if (baseFileSystem.setReadOnly(fileInfo.getLocalName())) {
	    if (fileInfo.mustReplicate()) {
		sendToReplicators(new SetReadOnlyMessage(fileName));
	    }
	    return true;

	} else {
	    return false;
	}
    }


    /**
     * {@inheritDoc}
     * 
     * @see org.h2.store.fs.FileSystem#unwrap(java.lang.String)
     */
    @Override
    public String unwrap(String fileName)
    {
	return fileName;
    }

    /**
     * @param b
     * @param off
     * @param len
     */
    public synchronized void cacheRead(FileInfo fileInfo, long filePointer, byte[] b, int off,
				       int len)
    {
	if (blockCache == null) {
	    return;
	}
	
	if (blocksizeLearningState == BlocksizeLearningState.INITIAL) {
	    blocksizeLearningState = BlocksizeLearningState.LEARNING;
	    learnedBlocksize = len;
	    blocksizeOccurenceCount = 1;
	    log.debug("initial blocksize hypothesis: "+len);

	} else if (blocksizeLearningState == BlocksizeLearningState.LEARNING) {
	    long blockno = filePointer / learnedBlocksize;
	    if (learnedBlocksize == len && blockno * learnedBlocksize == filePointer) {
		blocksizeOccurenceCount++;
		if (blocksizeOccurenceCount > REQUIRED_LEARN_COUNT) {
		    blocksizeLearningState = BlocksizeLearningState.LEARNED;
		    log.debug("learned blocksize: "+len);
		} else {
		    log.debug(blocksizeOccurenceCount+"th blocksize repetition: "+len);
		}
		
	    } else {
		learnedBlocksize = len;
		blocksizeOccurenceCount = 1;
		log.debug("blocksize hypothesis mismatch: "+len);

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
		String cacheKey = cacheKey(fileInfo, blockno);
		blockCache.put(cacheKey, dupData);
		log.debug("read: adding to cache: '"+cacheKey+"'");
	    }
	}
    }

    /**
     * @param fileSystem
     * @param b
     * @param off
     * @param len
     */
    public synchronized void compressAndSendWrite(FileInfo fileInfo, long filePointer, byte[] b,
						  int off, int len)
    {
	// All writing traffic will pass through this point. So we
	// may use the block cache to limit the amount of data sent
	// to the peer system to only the parts that have changed.
	// The current implementation assumes that the block will
	// usually have leading and trailing unchanged bytes and
	// cuts them off.
	boolean addToCache = false;
	if (blockCache != null && blocksizeLearningState == BlocksizeLearningState.LEARNED) {
	    long blockno = filePointer / learnedBlocksize;
	    if (learnedBlocksize == len && blockno * learnedBlocksize == filePointer) {
		// blocked write
		String cacheKey = cacheKey(fileInfo, blockno);
		byte[] oldData = blockCache.get(cacheKey);
		if (oldData != null) {
		    log.debug("found in cache: '"+cacheKey+"'");
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
		    log.debug("adjusted bounds to "+ofirst+" - "+olast+", len="+len);

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
		    log.debug("not in cache: '"+cacheKey+"'");
		    addToCache = true;
		}

	    } else {
		// not a block sized write. We need to flush
		// all entries from the cache that may have been invalidated
		// by this write
		log.debug("non-blocked: "+fileInfo.getHaName()+" - p="+filePointer+", l="+len);
		long lastBlockModified = (filePointer + len - 1) / learnedBlocksize;
		while (blockno <= lastBlockModified) {
		    blockCache.remove(cacheKey(fileInfo, blockno));
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
	byte[] dupData = new byte[len];
	System.arraycopy(b, off, dupData, 0, len);
	sendToReplicators(new WriteMessage(fileInfo.getHaName(), filePointer, dupData));

	if (addToCache) {
	    long blockno = filePointer / learnedBlocksize;
	    String cacheKey = cacheKey(fileInfo, blockno);
	    blockCache.put(cacheKey, dupData);
	    log.debug("write: adding to cache: '"+cacheKey+"'");
	}
    }

    /**
     * 
     */
    private String cacheKey(FileInfo fileInfo, long blockno)
    {
	return fileInfo.getHaName() + ":" + blockno;
    }


    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////
    /** */
    private static class CreateDirsMessage
	extends MessageToClient
    {
	private static final long serialVersionUID = 1L;
	String fileName;

	CreateDirsMessage(String fileName)
	{
	    this.fileName = fileName;
	}

	@Override
	protected void processMessageToClient(ReplicationClientInstance instance)
	    throws Exception
	{
	    instance.processCreateDirsMessage(fileName);
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
    private static class CreateNewFileMessage
	extends MessageToClient
    {
	private static final long serialVersionUID = 1L;
	String fileName;

	CreateNewFileMessage(String fileName)
	{
	    this.fileName = fileName;
	}

	@Override
	protected void processMessageToClient(ReplicationClientInstance instance)
	    throws Exception
	{
	    instance.processCreateNewFileMessage(fileName);
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
            instance.processFoWriteMessage(haName, filePointer, data);
        }

	@Override
	public int getSizeEstimate()
	{
	    return 30 + data.length;
	}
	
	@Override
	public String toString()
	{
	    return "write message "+haName+", offs="+filePointer+", len="+data.length;
	}
   }




    /** */
    private static class TryDeleteMessage
	extends MessageToClient
    {
	private static final long serialVersionUID = 1L;
	String fileName;

	TryDeleteMessage(String fileName)
	{
	    this.fileName = fileName;
	}

	@Override
	protected void processMessageToClient(ReplicationClientInstance instance)
	    throws Exception
	{
	    instance.processTryDeleteMessage(fileName);
	}

	@Override
	public int getSizeEstimate()
	{
	    return 20;
	}

	@Override
	public String toString()
	{
	    return "try delete " + fileName;
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
    private static class DeleteRecursiveMessage
	extends MessageToClient
    {
	private static final long serialVersionUID = 1L;
	String directory;
	boolean tryOnly;

	DeleteRecursiveMessage(String directory, boolean tryOnly)
	{
	    this.directory = directory;
	    this.tryOnly = tryOnly;
	}

	@Override
	protected void processMessageToClient(ReplicationClientInstance instance)
	    throws Exception
	{
	    instance.processDeleteRecursiveMessage(directory, tryOnly);
	}

	@Override
	public int getSizeEstimate()
	{
	    return 24;
	}

	@Override
	public String toString()
	{
	    return "delete recursive " + directory + ", tryOnly=" + tryOnly;
	}
    }

    /** */
    private static class RenameMessage
	extends MessageToClient
    {
	private static final long serialVersionUID = 1L;
	String oldName;
	String newName;

	RenameMessage(String oldName, String newName)
	{
	    this.oldName = oldName;
	    this.newName = newName;
	}

	@Override
	protected void processMessageToClient(ReplicationClientInstance instance)
	    throws Exception
	{
	    instance.processRenameMessage(oldName, newName);
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
