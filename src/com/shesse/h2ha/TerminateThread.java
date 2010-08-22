/**
 * (c) DICOS GmbH, 2010
 *
 * $Id$
 */

package com.shesse.h2ha;

import org.apache.log4j.Logger;

/**
 *
 * @author sth
 */
public class TerminateThread
    extends Exception
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static final long serialVersionUID = 1L;
    
    /** */
    private static Logger log = Logger.getLogger(TerminateThread.class);


    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public TerminateThread()
    {
	log.debug("TerminateThread()");
    }

    /**
     * @param message
     */
    public TerminateThread(String message)
    {
	super(message);
    }

    /**
     * @param cause
     */
    public TerminateThread(Throwable cause)
    {
	super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public TerminateThread(String message, Throwable cause)
    {
	super(message, cause);
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * 
     */
    public void logError(Logger logger, String prefix)
    {
	if (prefix == null) {
	    prefix = "";
	} else {
	    prefix += ": ";
	}
	
	if (getCause() == null) {
	    logger.error(prefix+getMessage());
	} else {
	    logger.error(prefix+getMessage(), getCause());
	}
    }

    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
