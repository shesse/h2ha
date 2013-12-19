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
 * May be used to request the creation of a new database within the
 * HA directory tree.
 * 
 * @author sth
 */
public class CreateDatabaseCommand
	extends ClientCommandProtocolInstance
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	private static Logger log = Logger.getLogger(CreateDatabaseCommand.class);

	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @param receiver
	 */
	public CreateDatabaseCommand()
	{
		super("createDbCommand");

	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
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
		log.debug("creating DB " + dbName);
		WaitingOperation<SQLException> wo =
			new WaitingOperation<SQLException>(new CreateDatabaseRequest(dbName, adminUser,
				adminPassword));

		SQLException result = wo.sendAndGetResult();
		if (result != null)
			throw result;
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
					((ReplicationServerInstance) instance).createDatabase(dbName, adminUser,
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
			return 16 + dbName.length() + adminUser.length() + adminPassword.length();
		}

		@Override
		public String toString()
		{
			return "create db req";
		}
	}

}
