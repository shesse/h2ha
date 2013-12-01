/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;
import java.sql.SQLException;

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
	super("serverStatus", 0, 0, 0);
	
	setParameters(0);
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

    /**
     * @param dbName
     * @param adminUser
     * @param adminPassword
     * @throws IOException 
     * @throws InterruptedException 
     * @throws SQLException 
     */
    public void createDatabase(String dbName, String adminUser, String adminPassword)
    throws InterruptedException, IOException, SQLException
    {
	log.debug("creating DB "+dbName);
        WaitingOperation<SQLException> wo = new WaitingOperation<SQLException>(new CreateDatabaseRequest(dbName, adminUser, adminPassword));

        SQLException result = wo.sendAndGetResult();
        if (result != null) throw result;
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

    /**
     * 
     */
    private static class CreateDatabaseRequest
    extends OperationRequestMessage<SQLException>
    {
        private static final long serialVersionUID = 1L;
        
        private String dbName;
        private String adminUser;
        private String adminPassword;
        
        CreateDatabaseRequest(String dbName, String adminUser, String adminPassword)
        {
            super(SQLException.class);
            
            this.dbName = dbName;
            this.adminUser = adminUser;
            this.adminPassword = adminPassword;
        }

        @Override
        protected SQLException performOperation(ReplicationProtocolInstance instance)
        {
            if (instance instanceof ReplicationServerInstance) {
        	try {
        	    ((ReplicationServerInstance)instance).createDatabase(dbName, adminUser, adminPassword);
        	    return null;
        	    
        	} catch (SQLException x) {
        	    return x;
        	}
        	
            } else {
                return null;
            }
        }

	@Override
	public int getSizeEstimate()
	{
	    return 16+dbName.length()+adminUser.length()+adminPassword.length();
	}
	
	@Override
	public String toString()
	{
	    return "create db req";
	}
    }

}
