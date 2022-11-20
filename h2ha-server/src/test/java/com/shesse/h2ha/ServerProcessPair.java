/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;

/**
 * 
 * @author sth
 */
public class ServerProcessPair
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	private static Logger log = Logger.getLogger(ServerProcessPair.class);

	/** */
	private static final File dbBaseDir = new File("h2hatest");

	/** */
	private static final File dbDirA = new File(dbBaseDir, "a");

	/** */
	private static final File dbDirB = new File(dbBaseDir, "b");

	/** */
	private static final int tcpPortA = 9092;

	/** */
	private static final int tcpPortB = 9093;

	/** */
	private static final int syncPortA = 8125;

	/** */
	private static final int syncPortB = 8126;

	/** */
	private ServerProcess processA = new ServerProcess("a", dbDirA, tcpPortA, syncPortA, syncPortB);

	/** */
	private ServerProcess processB = new ServerProcess("b", dbDirB, tcpPortB, syncPortB, syncPortA);

	/** */
	private DbManager dbManager;

	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
     */
	public ServerProcessPair(DbManager dbManager)
	{
		log.debug("ServerProcessPair()");
		this.dbManager = dbManager;
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */
	public void start(String... args)
		throws IOException, InterruptedException
	{
		startA(args);
		startB(args);
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */
	public void startA(String... args)
		throws IOException, InterruptedException
	{
		processA.start(true, args);
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */
	public void startB(String... args)
		throws IOException, InterruptedException
	{
		processB.start(false, args);
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */
	public void cleanup()
		throws IOException, InterruptedException
	{
		cleanupA();
		cleanupB();
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */
	public void cleanupA()
		throws IOException, InterruptedException
	{
		processA.cleanup();
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */
	public void cleanupB()
		throws IOException, InterruptedException
	{
		processB.cleanup();
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */
	public void waitUntilActive()
		throws IOException, InterruptedException
	{
		waitUntilAIsActive();
		waitUntilBIsActive();
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */
	public void waitUntilAIsActive()
		throws IOException, InterruptedException
	{
		processA.waitUntilActive();
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */
	public void waitUntilBIsActive()
		throws IOException, InterruptedException
	{
		processB.waitUntilActive();
	}

	/**
	 * @throws InterruptedException 
	 * @throws IOException 
	 * 
	 */
	public void waitUntilAIsMaster()
		throws IOException, InterruptedException
	{
		processA.waitUntilMaster();
	}

	/**
	 * @throws InterruptedException 
	 * @throws IOException 
	 * 
	 */
	public void waitUntilBIsMaster()
		throws IOException, InterruptedException
	{
		processB.waitUntilMaster();
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws SQLException
	 * 
	 */
	public void createDatabaseA()
		throws IOException, InterruptedException, SQLException
	{
		processA.createDatabase("localhost", syncPortA, "test", "sa", "sa");
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */
	public void createDatabaseB()
		throws IOException, InterruptedException, SQLException
	{
		processB.createDatabase("localhost", syncPortB, "test", "sa", "sa");
	}

	/**
	 * @throws InterruptedException
	 * @throws SQLException 
	 * 
	 */
	public void stop()
		throws InterruptedException, SQLException
	{
		dbManager.cleanup();
		
		processA.stop();
		processB.stop();
	}

	/**
	 * @throws InterruptedException
	 * 
	 */
	public void stopA()
		throws InterruptedException
	{
		processA.stop();
	}

	/**
	 * @throws InterruptedException
	 * 
	 */
	public void stopB()
		throws InterruptedException
	{
		processB.stop();
	}

	/**
     * 
     */
	public static File getDbBaseDir()
	{
		return dbBaseDir;
	}

	/**
	 * @throws SQLException 
	 * @throws InterruptedException 
     * 
     */
	public boolean contentEquals() throws InterruptedException, SQLException
	{
		log.info("Stopping DB instances before comparing the files");
		stop();
		return FileUtils.dbDirsEqual(dbDirA, dbDirB);
	}

	/**
     * 
     */
	public File getDirA()
	{
		return dbDirA;
	}

	/**
     * 
     */
	public File getDirB()
	{
		return dbDirB;
	}

	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
