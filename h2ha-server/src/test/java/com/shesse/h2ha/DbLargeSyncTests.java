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
	
	/** */
	private static String[] serverArgs = {
		"-Dthrottle=10000", 
		"-statisticsInterval", "5000",
		//"-haMaxQueueSize", "50"
	};
	

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
		servers.start(serverArgs);
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
	public void syncAll()
		throws SQLException, IOException, InterruptedException
	{
		log.info("######################################################################");
		log.info("######################################################################");
		log.info("start of test syncAll");
		
		//syncTest(2, 1.);
		syncTest(60, 1.);
	}
	
	@Test
	public void syncNone()
		throws SQLException, IOException, InterruptedException
	{
		log.info("######################################################################");
		log.info("######################################################################");
		log.info("start of test syncNone");
		
		//syncTest(2);
		syncTest(60, 0.);
	}
	
	@Test
	public void syncMiddle()
		throws SQLException, IOException, InterruptedException
	{
		log.info("######################################################################");
		log.info("######################################################################");
		log.info("start of test syncMiddle");
		
		//syncTest(2);
		syncTest(60, 0.5);
	}
	
	@Test
	public void syncFull()
		throws SQLException, IOException, InterruptedException
	{
		log.info("######################################################################");
		log.info("######################################################################");
		log.info("start of test syncFull");
		

		servers.stopB();
		
		log.info("wait for sync");
		tr.getDbManager().syncWithAllReplicators();
		log.info("sync finished");
		
		new File(servers.getDirB(), "test.mv.db").delete();
		

		//syncTest(2, 2.);
		syncTest(60, 2.);
	}
	
	private void syncTest(int threads, double stopReplAt) throws SQLException, IOException, InterruptedException
	{
		TestTable[] tables = new TestTable[100];
		for (int i = 0; i < tables.length; i++) {
			tables[i] = tr.createTable();
		}

		Thread[] writers = new Thread[threads];
		for (int i = 0; i < writers.length; i++) {
			Runnable onStopRepl = null;
			if (i == 0 && stopReplAt <= 1.0) {
				onStopRepl = new Runnable() {
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
			writers[i] = createWriter(i, tables, stopReplAt, onStopRepl);
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
		
	
		servers.startB(serverArgs);
		
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
	private Thread createWriter(final int wi, final TestTable[] tables, 
	                            final double stopReplAt, final Runnable onStopRepl)
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
				final int max = 5000;
				for (int i = 0; i < max; i++) {
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
					
					if (i == (int)(max*stopReplAt) && onStopRepl != null) {
						onStopRepl.run();
					}
				}
				if (max == (int)(max*stopReplAt) && onStopRepl != null) {
					onStopRepl.run();
				}
			}
		};
	}

	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
