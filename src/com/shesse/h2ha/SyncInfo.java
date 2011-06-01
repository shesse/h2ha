/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import org.apache.log4j.Logger;

/**
 *
 * @author sth
 */
public class SyncInfo
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(SyncInfo.class);

    /** */
    private FileInfo fileInfo;
    
    /** */
    private volatile long beginIgnore = 0L;
    
    /** */
    private volatile long endIgnore = 0L;

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public SyncInfo(FileInfo fileInfo)
    {
        log.debug("SyncInfo()");
        
        this.fileInfo = fileInfo;
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @return the fileInfo
     */
    public FileInfo getFileInfo()
    {
        return fileInfo;
    }

    /**
     * @return the beginIgnore
     */
    public long getBeginIgnore()
    {
        return beginIgnore;
    }

    /**
     * @param beginIgnore the beginIgnore to set
     */
    public void setBeginIgnore(long beginIgnore)
    {
        this.beginIgnore = beginIgnore;
    }

    /**
     * @return the endIgnore
     */
    public long getEndIgnore()
    {
        return endIgnore;
    }

    /**
     * @param endIgnore the endIgnore to set
     */
    public void setEndIgnore(long endIgnore)
    {
        this.endIgnore = endIgnore;
    }


    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////
}
