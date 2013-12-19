/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import org.apache.log4j.Logger;


/**
 * May be used to request the status from a H2HA server via it's control
 * connection. Even though this may be helpful in other contexts, it is
 * currently used for testing only.
 * 
 * @author sth
 */
public class ClientCommandProtocolInstance
	extends ReplicationProtocolInstance
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	private static Logger log = Logger.getLogger(ClientCommandProtocolInstance.class);

	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @param receiver
	 */
	public ClientCommandProtocolInstance(String instanceName)
	{
		super(instanceName, 0, 0, 0);

		setParameters(0);
		log.debug("ClientCommandProtocolInstance()");

	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////

	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////
}
