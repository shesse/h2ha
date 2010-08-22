/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.h2.engine.Constants;

/**
 *
 * @author sth
 */
public class FileInfo
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(FileInfo.class);

    /** */
    private String haName;
    
    /** */
    private static Set<String> allHaNames = null;
    
    /** */
    private String localName;
    
    /** */
    private boolean withinHaTree;
    
    /** */
    private boolean isDatabaseFile;
    
    
    /** */
    private boolean needsReplication;
    
  

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public FileInfo(String haName, String localName, boolean withinHaTree)
    {
        log.debug("HaFile()");
        
        this.haName = haName;
        this.localName = localName;
        this.withinHaTree = withinHaTree;
        
        if (haName.endsWith(Constants.SUFFIX_PAGE_FILE)) {
            log.debug("file "+haName+" is a page file");
            isDatabaseFile = true;
            needsReplication = true;
        } else if (haName.endsWith(Constants.SUFFIX_LOCK_FILE)) {
            log.debug("file "+haName+" is a lock file");
            isDatabaseFile = true;
            needsReplication = true;
        } else if (haName.endsWith(Constants.SUFFIX_LOB_FILE)) {
            log.debug("file "+haName+" is a LOB file");
            isDatabaseFile = true;
            needsReplication = true;
        } else if (haName.endsWith(Constants.SUFFIX_LOBS_DIRECTORY)) {
            log.debug("file "+haName+" is a LOBs directory");
            isDatabaseFile = true;
            needsReplication = true;
        } else if (haName.endsWith(Constants.SUFFIX_TEMP_FILE)) {
            log.debug("file "+haName+" is a temp file");
            isDatabaseFile = true;
            needsReplication = false;
        } else if (haName.endsWith(Constants.SUFFIX_TRACE_FILE)) {
            log.debug("file "+haName+" is an trace file");
            isDatabaseFile = true;
            needsReplication = false;
        } else if (haName.endsWith(Constants.SUFFIX_DB_FILE)) {
            log.debug("file "+haName+" is an unrecognized DB file");
            isDatabaseFile = true;
            needsReplication = true;
        } else {
            log.debug("file "+haName+" does not have a relevant suffix -- ignored");
            isDatabaseFile = false;
            needsReplication = false;
        }
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @return the haName
     */
    public String getHaName()
    {
        return haName;
    }

    /**
     * 
     */
    public void addAlternativeHaName(String haName)
    {
        if (haName.equals(this.haName)) return;
        if (allHaNames == null) {
            allHaNames = new HashSet<String>();
            allHaNames.add(this.haName);
        }
        
        allHaNames.add(haName);
    }
    
    /**
     * 
     */
    public Set<String> getAllHaNames()
    {
        if (allHaNames == null) {
            return Collections.singleton(haName);
        } else {
            return allHaNames;
        }
    }
    
    /**
     * @return the haEnabled
     */
    public boolean isWithinHaTree()
    {
        return withinHaTree;
    }

    /**
     * @return the localName
     */
    public String getLocalName()
    {
        return localName;
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
        return needsReplication && withinHaTree;
    }

    /**
     * Copies all the status information from oldFile to the
     * current object. HA and local names don't get modified.
     * This is done, when renaming the old the the new file.
     *  
     * @param oldFile
     */
    public void setStatusInfo(FileInfo oldFile)
    {
        withinHaTree = oldFile.withinHaTree;
    }

    /**
     * 
     */
    public String toString()
    {
        return haName;
        //return "(ha="+haName+", local="+localName+")";
    }

    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
