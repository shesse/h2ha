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
public abstract class MessageToClient
	extends ReplicationMessage
	implements Serializable
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	private static Logger log = Logger.getLogger(MessageToClient.class);

	/**
     * 
     */
	private static final long serialVersionUID = 1L;

	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
     */
	public MessageToClient()
	{
		log.debug("MessageToClient()");
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * @throws Exception
	 * 
	 */
	@Override
	protected void process(ReplicationProtocolInstance instance)
		throws Exception
	{
		if (instance instanceof ReplicationClientInstance) {
			processMessageToClient((ReplicationClientInstance) instance);
		} else {
			throw new IllegalArgumentException("expeced a ReplicationClientInstance and got " +
				instance.getClass().getName());
		}
	}

	/**
	 * @throws Exception
	 * 
	 */
	protected abstract void processMessageToClient(ReplicationClientInstance instance)
		throws Exception;

	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
