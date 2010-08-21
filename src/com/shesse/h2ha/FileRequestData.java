/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.Serializable;
import org.apache.log4j.Logger;

/**
 *
 * @author sth
 */
public class FileRequestData
    implements Serializable
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(FileRequestData.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    /** */
    private String haName;
    
    /** */
    public enum TransmissionMethod { FULL, DELTA };
    
    /** */
    private TransmissionMethod transmissionMethod;
    
    /**
     * ist -1 wenn nicht bekannt
     */
    private long existingFileLength = -1;
    
    /**
     * Angabe in Millis -- -1 wenn nicht bekannt
     */
    private long existingLastModified = -1;
   
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public FileRequestData(String haName, TransmissionMethod transmissionMethod, 
                           long existingFileLength, long existingLastModified)
    {
        log.debug("FileOnServer()");
        
        this.haName = haName;
        this.transmissionMethod = transmissionMethod;
        this.existingFileLength = existingFileLength;
        this.existingLastModified = existingLastModified;
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
     * @return the fileLength
     */
    public TransmissionMethod getTransmissionMethod()
    {
        return transmissionMethod;
    }


    /**
     * @return the existingFileLength
     */
    public long getExistingFileLength()
    {
        return existingFileLength;
    }


    /**
     * @return the existingLastModified
     */
    public long getExistingLastModified()
    {
        return existingLastModified;
    }


    /**
     * 
     */
    @Override
    public String toString()
    {
        return haName+": "+transmissionMethod;
    }

    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
