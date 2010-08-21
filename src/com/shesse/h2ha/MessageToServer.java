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
public abstract class MessageToServer
extends ReplicationMessage
    implements Serializable
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(MessageToServer.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public MessageToServer()
    {
        log.debug("MessageToServer()");
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
        if (instance instanceof ReplicationServerInstance) {
             processMessageToServer((ReplicationServerInstance)instance);
        } else {
            throw new IllegalArgumentException("expected a ReplicationServerInstance and got "+instance.getClass().getName());
        }
    }
    
    /**
     * 
     */
    protected abstract void processMessageToServer(ReplicationServerInstance instance)
    throws Exception;

    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
