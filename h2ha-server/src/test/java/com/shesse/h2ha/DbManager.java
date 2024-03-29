/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.h2.api.ErrorCode;
import org.h2.jdbcx.JdbcConnectionPool;

import com.shesse.jdbcproxy.HaDataSource;

/**
 * 
 * @author sth
 */
public class DbManager
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	private static Logger log = Logger.getLogger(DbManager.class);
	
	/** */
	private String addresses;
	
	/** */
	private String user;
	
	/** */
	private String password;

	/** */
	private JdbcConnectionPool cp = null;

	/** */
	private boolean mvStore;


	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
     */
	public DbManager(boolean mvStore)
	{
		this(mvStore, "localhost:9092,localhost:9093", "sa", "sa");
	}
	
	/**
     */
	public DbManager(boolean mvStore, String addresses, String user, String password)
	{
		log.debug("DbManager()");
		this.addresses = addresses;
		this.user = user;
		this.password = password;
		this.mvStore = mvStore;
	}


	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * @throws SQLException
	 * 
	 */
	public void shutdown()
		throws SQLException
	{
		if (cp != null) {
			log.info("Shutting down DB");
			Connection conn = createConnection();
			try {
				String sql = "shutdown";

				Statement stmnt = conn.createStatement();
				stmnt.executeUpdate(sql);

			} finally {
				conn.close();
			}

			cp.dispose();
			cp = null;
		}
	}

	/**
     * 
     */
	public void cleanup()
		throws SQLException
	{
		shutdown();
	}

	/**
	 * @throws SQLException
	 * 
	 */
	public Connection createConnection()
		throws SQLException
	{
		if (cp == null) {
			String url = "jdbc:h2ha:tcp://"+addresses+"/test";
			if (!mvStore) {
				url += ";MV_STORE=FALSE";
			}
			
			Properties props = new Properties();
			props.setProperty("user", user);
			props.setProperty("password", password);
			log.info("using JDBC URL="+url);
			HaDataSource ds = new HaDataSource(url, props);
			cp = JdbcConnectionPool.create(ds);
		}
		
		Connection conn = null;
		int attempts = 0;
		while (conn == null) {
			
			// check if this connection still can be used
			try {
				attempts++;
				conn = cp.getConnection();
				
				Statement stmnt = conn.createStatement();
				stmnt.executeQuery("select 1");
				stmnt.close();
				
			} catch (SQLException x) {
				if (conn != null) {
					conn.close();
				}
				
				if (x.getErrorCode() == ErrorCode.CONNECTION_BROKEN_1 && attempts <= 4) {
					// connection is broken - probably due to HA takeover
					log.info("connection returned from pool is broken - trying again");
					conn = null;
					
				} else if (x.getErrorCode() == ErrorCode.OBJECT_CLOSED && attempts <= 4) {
					// connection is broken - probably due to HA takeover
					log.info("connection has been closed - trying again");
					conn = null;
					
				//}  else if (x.getErrorCode() == ErrorCode.DATABASE_CALLED_AT_SHUTDOWN && attempts <= 4) {
				//	// connection is broken - probably due to HA takeover
				//	log.info("connection has been closed - trying again");
				//	conn = null;
					
				} else {
					// too many retries - give up!
					throw x;
				}
			}
		}
		
		conn.setAutoCommit(false);
		return conn;
	}

	/**
	 * Calls syncWithAllReplicators on the master db server
	 * 
	 * @throws SQLException
	 */
	public void syncWithAllReplicators()
		throws SQLException
	{
		Connection conn = createConnection();
		try {
			Statement stmnt = conn.createStatement();
			try {
				stmnt.executeUpdate("create alias SYNC_ALL_REPLICATORS for \"com.shesse.h2ha.H2HaServer.syncWithAllReplicators\"");
			} catch (SQLException x) {
			}
			stmnt.executeUpdate("call SYNC_ALL_REPLICATORS()");

		} finally {
			conn.close();
		}
		
		// we will wait for the MVStrore Background Writer to do its
		// last commit. This is usually done within 1 second
		try {
			Thread.sleep(2000);
		} catch (InterruptedException x1) {
		}
		

	}


	/**
	 * Calls transferMasterRole on the master db server
	 * 
	 * @throws SQLException
	 */
	public void transferMasterRole()
		throws SQLException
	{
		Connection conn = createConnection();
		try {
			Statement stmnt = conn.createStatement();
			try {
				stmnt.executeUpdate("create alias TRANSFER_MASTER for \"com.shesse.h2ha.H2HaServer.transferMasterRole\"");
			} catch (SQLException x) {
			}
			stmnt.executeUpdate("call TRANSFER_MASTER()");

		} finally {
			try {
				conn.close();
			} catch (SQLException x) {
				// ignore
			}
		}
	}


	/**
	 * @return the mvStore
	 */
	public boolean isMvStore()
	{
		return mvStore;
	}
	
	
	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////
	
	/**
	 * @return
	 */
	public boolean isActive()
	{
		return cp != null;
	}

	/**
	 * for testing only
	 */
	public static void main(String[] args)
		throws Exception
	{
		log.info("connecting ...");
		new DbManager(true).createConnection();
		log.info("connected");
	}
	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////

}
