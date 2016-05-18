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
public class DbLargeSyncTests
	extends TestGroupBase
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	static private Logger log = Logger.getLogger(DbLargeSyncTests.class);

	/** */
	private Random rnd = new Random();

	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @throws SQLException
	 */
	public DbLargeSyncTests()
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
			((AppenderSkeleton)a).setThreshold(Level.DEBUG);
		}
		dbManager.cleanup();
		servers.cleanup();
		servers.start();
		servers.waitUntilActive();
		servers.createDatabaseA();
		tr.startup();
		log.info("######################################################################");
		log.info("######################################################################");
		log.info("setup done");
	}

	@After
	public void tearDown()
		throws InterruptedException, SQLException
	{
		logStatistics();

		log.info("######################################################################");
		log.info("######################################################################");
		log.info("beginning to tear down");
		tr.shutdown();
		dbManager.shutdown();
		servers.stop();
	}


	@Test
	public void largeSyncTest()
		throws SQLException, IOException, InterruptedException
	{
		log.info("######################################################################");
		log.info("######################################################################");
		log.info("start of test largeSyncTest");
		
		//syncTest(2);
		syncTest(60);
	}
	
	private void syncTest(int threads) throws SQLException, IOException, InterruptedException
	{
		TestTable[] tables = new TestTable[100];
		for (int i = 0; i < tables.length; i++) {
			tables[i] = tr.createTable();
		}

		Thread[] writers = new Thread[threads];
		for (int i = 0; i < writers.length; i++) {
			Runnable onHalfway = null;
			if (i == 0) {
				onHalfway = new Runnable() {
					@Override
					public void run()
					{
						try {
							servers.stopB();
						} catch (InterruptedException x) {
							log.error("InterruptedException", x);
						}
					}
				};
			}
			writers[i] = createWriter(i, tables, onHalfway);
			writers[i].start();
		}

		for (int i = 0; i < writers.length; i++) {
			writers[i].join();
		}

		log.info("wait for sync");
		tr.getDbManager().syncWithAllReplicators();
		log.info("sync finished");
		
		log.info("file size A="+new File(servers.getDirA(), "test.mv.db").length());
		log.info("file size B="+new File(servers.getDirB(), "test.mv.db").length());
		
	
		servers.startB();
		
		for (;;) {
			String ps = getPeerState();
			log.info("peer state = "+ps);
			if ("SLAVE".equals(ps)) {
				break;
			}
			Thread.sleep(5000);
		}
		
		log.info("wait for sync");
		tr.getDbManager().syncWithAllReplicators();
		log.info("sync finished");
		
		log.info("file size A="+new File(servers.getDirA(), "test.mv.db").length());
		log.info("file size B="+new File(servers.getDirB(), "test.mv.db").length());
		

		Assert.assertTrue(servers.contentEquals());
		log.info("end of test largeDb");
	}

	/**
     */
	private Thread createWriter(final int wi, final TestTable[] tables, final Runnable onHalfway)
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
						tab.insertRecord(10);
					}

					if (i % 1000 == 999) {
						log.info("writer[" + wi + "]: " + (i + 1));
					}
					
					if (i == 2500 && onHalfway != null) {
						onHalfway.run();
					}
				}
			}
		};
	}

	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
