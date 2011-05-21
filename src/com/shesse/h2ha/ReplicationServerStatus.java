/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;

import org.apache.log4j.Logger;


/**
 * 
 * @author sth
 */
public class ReplicationServerStatus
extends ReplicationProtocolInstance
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
	super("serverStatus", 0);
	
        log.debug("ReplicationServerStatus()");

    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @throws IOException 
     * @throws InterruptedException 
     * 
     */
    public boolean isActive() throws IOException, InterruptedException
    {
        log.debug("querying server status");
        WaitingOperation<Boolean> wo = new WaitingOperation<Boolean>(new IsActiveRequest());

        log.debug("sending is active request to peer");
        boolean result = wo.sendAndGetResult();
        log.debug("is active has been confirmed - result = "+result);
        
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
                return ((ReplicationServerInstance)instance).isActive();
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

 }
