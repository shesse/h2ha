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
public abstract class ReplicationMessage
	implements Serializable
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	private static Logger log = Logger.getLogger(ReplicationMessage.class);

	/**
     * 
     */
	private static final long serialVersionUID = 1L;

	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
     */
	public ReplicationMessage()
	{
		log.debug("ReplicationMessage()");
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * @throws Exception
	 * 
	 */
	protected abstract void process(ReplicationProtocolInstance instance)
		throws Exception;

	/**
     * 
     */
	protected boolean callOnlyIfConnected()
	{
		return true;
	}

	/**
     * 
     */
	public boolean needToSend(ReplicationProtocolInstance instance)
	{
		return true;
	}

	/**
	 * @return estimated size in bytes of this object when serialized. This value is only
	 *         used for statistical purposes and need not be exact.
	 */
	public abstract int getSizeEstimate();

	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
