/**
 * (c) Stephan Hesse, 2013
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.h2.engine.Constants;
import org.h2.store.fs.FilePath;


/**
 *
 * @author sth
 */
public class FilePathHa
	extends FilePath
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	private static Logger log = Logger.getLogger(FilePathHa.class.getName());
	
	/** */
	private FileSystemHa fileSystem;
	
    /** */
    private FilePath basePath;

    /**
     * See normalizeHaName() for rules on ths value 
     */
    private String normalizedHaName;
    
    /** */
    private boolean isDatabaseFile;
    
    /** */
    private boolean needsReplication;
    

	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @param fileSystem
     * @param isAlreadyNormalized should only be true if the caller knows
     * that the passed haName is already normalized. Otherwise it will
     * be normalized using normalizeHaName, which is always be possible
     * but may consume some additional CPU cycles
	 */
	public FilePathHa(FileSystemHa fileSystem, String haName, boolean isAlreadyNormalized)
	{
		this.fileSystem = fileSystem;
		if (isAlreadyNormalized) {
			this.normalizedHaName = haName; // ha:///path/to/file
		} else {
			this.normalizedHaName = normalizeHaName(haName);
		}
		
        FilePath haDir = fileSystem.getLocalBaseDir();
        basePath = haDir.getPath(haDir.toString()+normalizedHaName.substring(5));
        
        if (normalizedHaName.endsWith(Constants.SUFFIX_PAGE_FILE)) {
            log.debug("file "+normalizedHaName+" is a page file");
            isDatabaseFile = true;
            needsReplication = true;
        } else if (normalizedHaName.endsWith(Constants.SUFFIX_LOCK_FILE)) {
            log.debug("file "+normalizedHaName+" is a lock file");
            isDatabaseFile = true;
            needsReplication = false;
        } else if (normalizedHaName.endsWith(Constants.SUFFIX_LOB_FILE)) {
            log.debug("file "+normalizedHaName+" is a LOB file");
            isDatabaseFile = true;
            needsReplication = true;
        } else if (normalizedHaName.endsWith(Constants.SUFFIX_LOBS_DIRECTORY)) {
            log.debug("file "+normalizedHaName+" is a LOBs directory");
            isDatabaseFile = true;
            needsReplication = true;
        } else if (normalizedHaName.endsWith(Constants.SUFFIX_TEMP_FILE)) {
            log.debug("file "+normalizedHaName+" is a temp file");
            isDatabaseFile = true;
            needsReplication = false;
        } else if (normalizedHaName.endsWith(Constants.SUFFIX_TRACE_FILE)) {
            log.debug("file "+normalizedHaName+" is an trace file");
            isDatabaseFile = true;
            needsReplication = false;
        } else if (normalizedHaName.endsWith(Constants.SUFFIX_DB_FILE)) {
            log.debug("file "+normalizedHaName+" is an unrecognized DB file");
            isDatabaseFile = true;
            needsReplication = true;
        } else {
            log.debug("file "+normalizedHaName+" does not have a relevant suffix -- ignored");
            isDatabaseFile = false;
            needsReplication = false;
        }

	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * returns a normalized form of the HA name.
	 * A normalized HA name has the following form:
	 * <ul>
	 * <li>starts with ha:///
	 * <li>uses '/' as separator on all platforms
	 * <li>Only the root node (ha:///)  ends with a '/'.
	 * <li>Does not contain duplicate appearances of '/'
	 * <li>Does not contain /../ nor end with /..  
	 * <li>Does not contain /./ nor end with /.  
	 * <ul>
	 * 
	 * @return the normalizedHaName
	 */
	public String getNormalizedHaName()
	{
	    return normalizedHaName;
	}

	/**
	 * @return the basePath
	 */
	public FilePath getBasePath()
	{
		
	    return basePath;
	}

	/**
	 * 
	 */
	public boolean isDatabaseFile()
	{
	    return isDatabaseFile;
	}

	/**
	 * @return
	 */
	public boolean mustReplicate()
	{
	    return needsReplication;
	}

	/**
	 * @return the fileSystem
	 */
	public FileSystemHa getFileSystem()
	{
		return fileSystem;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#getName()
	 */
	@Override
	public String getName()
	{
		int lsep = normalizedHaName.lastIndexOf('/');
		if (lsep < 0) {
			// should not happen
			return normalizedHaName;
		} else {
			return normalizedHaName.substring(lsep+1);
		}
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#createTempFile(java.lang.String, boolean, boolean)
	 */
	@Override
	public FilePath createTempFile(String suffix, boolean deleteOnExit, boolean inTempDir)
		throws IOException
	{
		// we leave temp files always on the local host and never replicate
		// them
		return basePath.createTempFile(suffix, deleteOnExit, inTempDir);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#unwrap()
	 */
	@Override
	public FilePath unwrap()
	{
		// it is not clear for what H2 uses this information -
		// we want to completely hide the underlying implementation.
		return this;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#canWrite()
	 */
	@Override
	public boolean canWrite()
	{
		return getBasePath().canWrite();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#toRealPath()
	 */
	@Override
	public FilePath toRealPath()
	{
		return this;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#createDirectory()
	 */
	@Override
	public void createDirectory()
	{
		getBasePath().createDirectory();

		if (mustReplicate()) {
			fileSystem.sendCreateDirectory(this);
		}
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#createFile()
	 */
	@Override
	public boolean createFile()
	{
		boolean success = getBasePath().createFile();

		if (success && mustReplicate()) {
			fileSystem.sendCreateFile(this);
		}
		
		return success;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#delete()
	 */
	@Override
	public void delete()
	{
		getBasePath().delete();

		if (mustReplicate()) {
			fileSystem.sendDelete(this);
		}
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#exists()
	 */
	@Override
	public boolean exists()
	{
		return getBasePath().exists();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#getParent()
	 */
	@Override
	public FilePath getParent()
	{
		String haName = getNormalizedHaName();
		int lsep = haName.lastIndexOf('/');
		if (lsep <= 5) { // ha:///
			haName = "ha:///";
		} else {
			haName = haName.substring(0, lsep);
		}
		
		return new FilePathHa(fileSystem, haName, true);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#getPath(java.lang.String)
	 */
	@Override
	public FilePath getPath(String haName)
	{
		return new FilePathHa(fileSystem, haName, false);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#getScheme()
	 */
	@Override
	public String getScheme()
	{
		return "ha";
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#isAbsolute()
	 */
	@Override
	public boolean isAbsolute()
	{
		return true;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#isDirectory()
	 */
	@Override
	public boolean isDirectory()
	{
		return getBasePath().isDirectory();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#lastModified()
	 */
	@Override
	public long lastModified()
	{
		return getBasePath().lastModified();
	}
	
	/**
	 * Is not a part of the FilePath interface. However, it is 
	 * useful for the replication mechanism and we try
	 * to implement it on a best effort base (assuming the unterlying
	 * basePath is a real file system accessbale be the File() class
	 */
	public void lastModified(long timestamp)
	{
		File baseFile = new File(basePath.toString());
		if (baseFile.exists()) {
			baseFile.setLastModified(timestamp);
		}
	}
	

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#moveTo(org.h2.store.fs.FilePath)
	 */
	@Override
	public void moveTo(FilePath newPath)
	{
		if (newPath instanceof FilePathHa) {
			FilePathHa newPathHa = (FilePathHa)newPath;
			getBasePath().moveTo(newPathHa.getBasePath());
			fileSystem.sendMoveTo(this, newPathHa);
		}
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#newDirectoryStream()
	 */
	@Override
	public List<FilePath> newDirectoryStream()
	{
		List<FilePath> haEntries = new ArrayList<FilePath>();
		for (FilePath baseEntry: getBasePath().newDirectoryStream()) {
			String baseName = baseEntry.getName();
			if (baseName.equals(".") || baseName.equals("..")) {
				// contradicts normalization
				continue;
			} else {
				haEntries.add(new FilePathHa(fileSystem, normalizedHaName+"/"+baseName, true));

			}
		}
		
		return haEntries;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#newInputStream()
	 */
	@Override
	public InputStream newInputStream()
		throws IOException
	{
		return getBasePath().newInputStream();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#newOutputStream(boolean)
	 */
	@Override
	public OutputStream newOutputStream(boolean append)
		throws IOException
	{
		return fileSystem.newOutputStream(this, append);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#open(java.lang.String)
	 */
	@Override
	public FileChannel open(String accessMode)
		throws IOException
	{
		return fileSystem.open(this, accessMode);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#setReadOnly()
	 */
	@Override
	public boolean setReadOnly()
	{
		if (basePath.setReadOnly()) {
			if (mustReplicate()) {
				fileSystem.sendSetReadOnly(this);
			}
			return true;
		} else {
			return false;
		}
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.h2.store.fs.FilePath#size()
	 */
	@Override
	public long size()
	{
		return basePath.size();
	}

	/**
     * Normalizes the argument.  See getNormalizedHaName() for
     * a description of the normalized form.
     */
    public static String normalizeHaName(String value)
    {
    	// cut off protocol prefix - we will add at again at the end
    	if (value.startsWith("ha:")) {
    		// ok
    		value = value.substring(3);
    	}
    	
     	// prevent double-slash
    	value = value.replace('\\', '/');
    	value = value.replaceAll("//+", "/");

       	if (value.startsWith("/")) {
    		value = value.substring(1);
    	}
    	
       	if (value.endsWith("/")) {
    		value = value.substring(0, value.length()-1);
    	}
    	
    	for (;;) {
    	   	int ddot;
    	   	if (value.endsWith("/.")) {
     	   		// single dot at end
    			value = value.substring(0, value.length() -2);

    		} else if (value.endsWith("/..")) {
	    		// double-dot at end
    			int lseg = value.lastIndexOf('/', value.length() -4);
    			if (lseg < 0) {
        			// name/..
        			value = value.substring(0, value.length()-3);
        		} else {
        			// cut off last part of path 
        			value = value.substring(0, lseg);
        		}

    		} else if (value.startsWith("../")) {
	    		// double-dot at beginning
    			value = value.substring(3);

    		} else if ((ddot = value.indexOf("/../")) >= 0) {
    			// double dot within path
    			int lseg = value.lastIndexOf('/', ddot-1);
        		if (lseg < 0) {
        			// double dot after first element
        			value = value.substring(ddot+4);
        		} else {
        			// double dot within path
        			value = value.substring(0, lseg)+value.substring(ddot+3);
        		}
        		
    		} else {
    			// nothing special - path is already normalized
    			break;
    		}
     	}
    	
    	// prepend ha:///, because it was stripped off above
    	value = "ha:///"+value;
    	
    	return value;
    }

    
    /**
	 * {@inheritDoc}
	 *
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return normalizedHaName.hashCode();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof FilePathHa) {
			return normalizedHaName.equals(((FilePathHa)obj).normalizedHaName);
		} else {
			return false;
		}
	}

	/**
     * 
     */
    public String toString()
    {
    	if (normalizedHaName.equals("ha:///")) {
    		// work around H2' check procedure that tries to enforce
    		// the database being within the base dir. As of 1.3.174. 
    		// it will not accept ha:///test to lie within
    		// ha:///, but it will accept it to lie within
    		// ha:// ....
    		// See org.h2.engine.ConnectionInfo.setBaseDir(String)
    		return "ha://";
    	} else {
    		return normalizedHaName;
    	}
    }




	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
