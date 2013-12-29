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
 * Client for the H2HA server that executes
 * commands on the server via the control channel.
 * 
 * @author sth
 */
public class ControlCommandClient
	extends ReplicationProtocolInstance
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	private static Logger log = Logger.getLogger(ControlCommandClient.class);

	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @param receiver
	 */
	public ControlCommandClient()
	{
		super("command", 0);

		setParameters(0, 0, 0, 30000);
		log.debug("ControlCommandClient()");

	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * May be used to request the creation of a new database within the
	 * HA directory tree.
	 * @param dbNameAndParameters
	 * @param adminUser
	 * @param adminPassword
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws SQLException
	 */
	public void createDatabase(String dbName, String adminUser, String adminPassword)
		throws InterruptedException, IOException, SQLException
	{
		log.debug("creating DB " + dbName);
		WaitingOperation<SQLException> wo =
			new WaitingOperation<SQLException>(new CreateDatabaseRequest(dbName, adminUser,
				adminPassword));

		SQLException result = wo.sendAndGetResult();
		if (result != null)
			throw result;
	}

	/**
	 * May be used to request the status from a H2HA server via it's control
	 * connection.
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
	private static class CreateDatabaseRequest
		extends OperationRequestMessage<SQLException>
	{
		private static final long serialVersionUID = 1L;

		private String dbNameAndParameters;
		private String adminUser;
		private String adminPassword;

		CreateDatabaseRequest(String dbNameAndParameters, String adminUser, String adminPassword)
		{
			super(SQLException.class);

			this.dbNameAndParameters = dbNameAndParameters;
			this.adminUser = adminUser;
			this.adminPassword = adminPassword;
		}

		@Override
		protected SQLException performOperation(ReplicationProtocolInstance instance)
		{
			if (instance instanceof ReplicationServerInstance) {
				try {
					((ReplicationServerInstance) instance).createDatabase(dbNameAndParameters, adminUser,
						adminPassword);
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
			return 16 + dbNameAndParameters.length() + adminUser.length() + adminPassword.length();
		}

		@Override
		public String toString()
		{
			return "create db req";
		}
	}

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
