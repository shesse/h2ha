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
	//private static Logger log = Logger.getLogger(TerminateThread.class);

	/** */
	private boolean isError = true;
	
	
	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @param message
	 */
	public TerminateThread(String message)
	{
		super(message);
	}

	/**
	 * @param message
	 */
	public TerminateThread(boolean isError, String message)
	{
		super(message);
		this.isError = isError;
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
	 * @return the isError
	 */
	public boolean isError()
	{
		return isError;
	}
	
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
			logger.error(prefix + getMessage());
		} else {
			logger.error(prefix + getMessage(), getCause());
		}
	}

	

	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
