/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Random;

import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author sth
 */
public class SingleServerTests
	extends TestGroupBase
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	static private Logger log = Logger.getLogger(SingleServerTests.class);

	/** */
	private Random rnd = new Random();

	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @throws SQLException
	 */
	public SingleServerTests()
		throws SQLException
	{
		log.debug("FailoverTests()");
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	@Before
	public void setUp()
		throws SQLException, IOException, InterruptedException
	{
		Appender a = Logger.getRootLogger().getAppender("STDERR");
		if (a instanceof AppenderSkeleton) {
			((AppenderSkeleton) a).setThreshold(Level.DEBUG);
		}
		dbManager.cleanup();
		servers.cleanupA();
		servers.startA();
		servers.waitUntilAIsActive();
		servers.createDatabaseA();
		tr.startup();
	}

	@After
	public void tearDown()
		throws InterruptedException, SQLException
	{
		tr.shutdown();
		dbManager.shutdown();
		servers.stop();
	}


	/**
	 * Test Purpose: verify that the database file gets created
	 */
	@Test
	public void createDb()
	{
		Assert.assertTrue(new File(servers.getDirA(), "test.mv.db").exists());
	}

	/**
	 * Test Purpose: verify that data stored during one DB run is accessible in
	 * another run
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@Test
	public void verifyPersistence()
		throws SQLException, InterruptedException, IOException
	{
		TestTable table = tr.createTable();
		table.insertRecord();

		dbManager.shutdown();
		servers.stop();

		servers.startA();
		servers.waitUntilAIsActive();

		Assert.assertTrue(table.getNoOfRecords() == 1);
	}


	/**
	 * Test Purpose: verify that connection loss detection and 
	 * reconnect (in DbManager) is working
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@Test
	public void verifyReconnect()
		throws SQLException, InterruptedException, IOException
	{
		log.info("cleaning up connection pool");
		dbManager.shutdown();

		log.info("creating a table to force a new connection");
		TestTable table = tr.createTable();
		table.insertRecord();

		log.info("restarting DB server");
		servers.stop();

		servers.startA();
		servers.waitUntilAIsActive();

		log.info("getting a new connection and using it");
		Assert.assertTrue(table.getNoOfRecords() == 1);
	}

	@Test
	public void largeDb()
		throws SQLException, IOException, InterruptedException
	{
		log.info("largeDb");

		TestTable[] tables = new TestTable[50];
		for (int i = 0; i < tables.length; i++) {
			tables[i] = tr.createTable();
		}

		Thread[] writers = new Thread[20];
		for (int i = 0; i < writers.length; i++) {
			writers[i] = createWriter(i, tables);
			writers[i].start();
		}

		for (int i = 0; i < writers.length; i++) {
			writers[i].join();
		}
	}

	/**
     */
	private Thread createWriter(final int wi, final TestTable[] tables)
	{
		return new Thread() {
			public void run()
			{
				try {
					body();
				} catch (Throwable x) {
					log.error("unexpected error within writer thread", x);
				}
			}

			public void body()
				throws SQLException
			{
				for (int i = 0; i < 5000; i++) {
					TestTable tab;
					synchronized (rnd) {
						tab = tables[rnd.nextInt(tables.length)];
					}

					synchronized (tab) {
						tab.insertRecord();
					}

					if (i % 1000 == 999) {
						log.info("writer[" + wi + "]: " + (i + 1));
					}
				}
			}
		};
	}


	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
