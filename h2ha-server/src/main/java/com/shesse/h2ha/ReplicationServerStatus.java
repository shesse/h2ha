/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;

import org.apache.log4j.Logger;


/**
 * May be used to request the status from a H2HA server via it's control
 * connection. Even though this may be helpful in other contexts, it is
 * currently used for testing only.
 * 
 * @author sth
 */
public class ReplicationServerStatus
	extends ClientCommandProtocolInstance
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	private static Logger log = Logger.getLogger(ReplicationServerStatus.class);

	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @param receiver
	 */
	public ReplicationServerStatus()
	{
		super("serverStatus");
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 */
	public boolean isActive()
		throws IOException, InterruptedException
	{
		log.debug("querying server status");
		WaitingOperation<Boolean> wo = new WaitingOperation<Boolean>(new IsActiveRequest());

		log.debug("sending is active request to peer");
		boolean result = wo.sendAndGetResult();
		log.debug("is active has been confirmed - result = " + result);

		return result;
	}

	/**
	 * @return
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public boolean isMaster()
		throws InterruptedException, IOException
	{
		log.debug("querying server status");
		WaitingOperation<Boolean> wo = new WaitingOperation<Boolean>(new IsMasterRequest());

		log.debug("sending is master request to peer");
		boolean result = wo.sendAndGetResult();
		log.debug("is master has been confirmed - result = " + result);

		return result;
	}


	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////
	/**
     * 
     */
	private static class IsActiveRequest
		extends OperationRequestMessage<Boolean>
	{
		private static final long serialVersionUID = 1L;

		IsActiveRequest()
		{
			super(Boolean.class);
		}

		@Override
		protected Boolean performOperation(ReplicationProtocolInstance instance)
		{
			if (instance instanceof ReplicationServerInstance) {
				return ((ReplicationServerInstance) instance).isActive();
			} else {
				return false;
			}
		}

		@Override
		public int getSizeEstimate()
		{
			return 4;
		}

		@Override
		public String toString()
		{
			return "is active req";
		}
	}

	/**
     * 
     */
	private static class IsMasterRequest
		extends OperationRequestMessage<Boolean>
	{
		private static final long serialVersionUID = 1L;

		IsMasterRequest()
		{
			super(Boolean.class);
		}

		@Override
		protected Boolean performOperation(ReplicationProtocolInstance instance)
		{
			if (instance instanceof ReplicationServerInstance) {
				return ((ReplicationServerInstance) instance).isMaster();
			} else {
				return false;
			}
		}

		@Override
		public int getSizeEstimate()
		{
			return 4;
		}

		@Override
		public String toString()
		{
			return "is master req";
		}
	}

}
