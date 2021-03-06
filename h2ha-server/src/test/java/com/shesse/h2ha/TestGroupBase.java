/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.LogManager;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * 
 * @author sth
 */
public class TestGroupBase
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	static private Logger log = Logger.getLogger(FailoverTests.class);

	/** */
	protected ServerProcessPair servers;

	/** */
	protected DbManager dbManager;

	/** */
	protected TableRegistry tr;
	
	static {
		try {
			InputStream is = TestGroupBase.class.getResourceAsStream("/logging.properties");
			LogManager.getLogManager().readConfiguration(is);
			is.close();
		} catch (IOException x) {
			x.printStackTrace();
		}
	}

	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
     * 
     */
	public TestGroupBase(boolean mvStore)
	{
		this(new DbManager(mvStore));
		
	}
	/**
     * 
     */
	public TestGroupBase(DbManager dbManager)
	{
		super();
		
		this.dbManager = dbManager;
		this.servers = new ServerProcessPair(dbManager);
		
		this.tr = new TableRegistry(dbManager);
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
     * 
     */
	@BeforeClass
	public static void oneTimeSetUp()
		throws IOException, InterruptedException
	{
		log.info("ensuring that no old test instances are running");
		boolean stillActive;
		do {
			stillActive = false;
			
			File jhome = new File(System.getProperty("java.home"));
			
			File jps = null;
			for (String rel: new String[]{"bin/jps", "bin/jps.exe", "../bin/jps", "../bin/jps.exe"}) {
				File check = new File(jhome, rel);
				if (check.exists()) {
					jps = check;
					break;
				}
			}
			
			if (jps == null) {
				throw new IllegalStateException("cannot find jps command - please start the test from a JDK");
			}
			
			ProcessBuilder pb = new ProcessBuilder(jps.getPath(), "-v");
			pb.redirectInput(Redirect.INHERIT);
			pb.redirectErrorStream(true);
			
			Process proc = pb.start();
			BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String line;
			while ((line = br.readLine()) != null) {
				if (line.contains("DhaTestProc=")) {
					String[] fields = line.split("\\s+");
					log.info("terminating old ha test process " + fields[1]);
					String killer;
					if (System.getProperty("os.name").toLowerCase().contains("windows")) {
						killer = "taskkill /PID "+fields[0]+" /F";
					} else {
						killer = "kill -9 " + fields[0];
					}
					ProcessUtils.exec(killer);
					stillActive = true;
				}
			}

			if (stillActive) {
				Thread.sleep(2000L);
			}
		} while (stillActive);

		log.info("cleaning up old DB dirs");
		FileUtils.deleteRecursive(ServerProcessPair.getDbBaseDir());
	}


	/**
     * 
     */
	@AfterClass
	public static void oneTimeTearDown()
		throws InterruptedException
	{
	}

	/**
	 * @throws SQLException
	 * 
	 */
	public void executeTransaction(TransactionBody transBody)
		throws SQLException
	{
		SQLException lastException = null;
		for (int i = 0; i < 5; i++) {
			try {
				Connection conn;
				try {
					conn = dbManager.createConnection();
				} catch (SQLException x) {
					log.info("could not connect to DB server: " + x.getMessage());
					throw x;
				}

				try {
					Statement stmnt = conn.createStatement();

					transBody.run(stmnt);

					conn.commit();

					return;

				} catch (SQLException x) {
					log.info("exception during transaction - rollback: " + x.getMessage());
					conn.rollback();
					throw x;

				} finally {
					conn.close();
				}

			} catch (SQLException x) {
				lastException = x;
			}
		}

		throw lastException;
	}

	/**
	 * @throws SQLException
	 * 
	 */
	protected void logStatistics()
		throws SQLException
	{
		if (!dbManager.isActive()) {
			return;
		}
		
		Connection conn = dbManager.createConnection();
		try {
			Statement stmnt = conn.createStatement();

			try {
				stmnt.executeUpdate("create alias SERVER_INFO for \"com.shesse.h2ha.H2HaServer.getServerInfo\"");

			} catch (SQLException x) {
				// ignore
			}

			try {
				stmnt.executeUpdate("create alias REPLICATION_INFO for \"com.shesse.h2ha.H2HaServer.getReplicationInfo\"");

			} catch (SQLException x) {
				// ignore
			}

			logTable(stmnt, "SERVER_INFO");
			logTable(stmnt, "REPLICATION_INFO");

		} finally {
			conn.close();
		}
	}

	/**
	 * @throws SQLException
	 * 
	 */
	protected String getPeerState()
		throws SQLException
	{
		Connection conn = dbManager.createConnection();
		try {
			Statement stmnt = conn.createStatement();

			try {
				stmnt.executeUpdate("create alias SERVER_INFO for \"com.shesse.h2ha.H2HaServer.getServerInfo\"");

			} catch (SQLException x) {
				// ignore
			}

			ResultSet rset = stmnt.executeQuery("select * from SERVER_INFO()");
			if (rset.next()) {
				return rset.getString("PEER_STATUS");
			}
			return null;

		} finally {
			conn.close();
		}
	}

	/**
	 * @throws SQLException
	 * 
	 */
	private void logTable(Statement stmnt, String aliasName)
		throws SQLException
	{
		ResultSet rset = stmnt.executeQuery("select * from " + aliasName + "()");
		ResultSetMetaData meta = rset.getMetaData();

		while (rset.next()) {
			StringBuilder line = new StringBuilder();
			String delim = "";
			for (int i = 0; i < meta.getColumnCount(); i++) {
				String name = meta.getColumnLabel(i + 1);
				Object value = rset.getObject(i + 1);
				line.append(delim).append(name).append("=").append(value);
				delim = ", ";
			}
			log.info(aliasName + ": " + line);
		}
	}

}