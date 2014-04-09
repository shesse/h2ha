/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Random;

import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.h2.api.ErrorCode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author sth
 */
public class PermanentLoadGenerator
	extends TestGroupBase
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	static private Logger log = Logger.getLogger(PermanentLoadGenerator.class);

	/** */
	private Random rnd = new Random();

	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @throws SQLException
	 */
	public PermanentLoadGenerator()
	{
		super(new DbManager("h2ha-a,h2ha-b", "sa", "sa"));

		log.debug("FailoverTests()");
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	@Before
	public void setUp()
		throws SQLException, IOException, InterruptedException
	{
		log.info("######################################################################");
		log.info("######################################################################");
		log.info("setting up");
		Appender a = Logger.getRootLogger().getAppender("STDERR");
		if (a instanceof AppenderSkeleton) {
			((AppenderSkeleton)a).setThreshold(Level.DEBUG);
		}
		log.info("######################################################################");
		log.info("######################################################################");
		log.info("setting up");
		dbManager.cleanup();
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
	}


	/**
	 * Don't try to execute this test as part of the maven tests!
	 * It is intended to never terminate - which is usually
	 * a bad idea for automated tests.
	 * <p>
	 * For a manual test it simply will permamently send requests
	 * to a HA database pair allowing the user to start and stop
	 * instances by hand and observing the consequences.
	 *  
	 * @throws SQLException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void permanentLoad()
		throws SQLException, IOException, InterruptedException
	{
		log.info("######################################################################");
		log.info("######################################################################");
		log.info("start of test permanentLoad");

		for (;;) {
			TestTable[] tables = new TestTable[50];
			for (int i = 0; i < tables.length; i++) {
				for (;;) {
					try {
						tables[i] = tr.createTable();
						break;
					} catch (SQLException x) {
						if (x.getErrorCode() == ErrorCode.CONNECTION_BROKEN_1) {
							log.info("exception on create table - retrying it: "+x.getMessage());
						} else if (x.getErrorCode() == ErrorCode.OBJECT_CLOSED) {
							log.info("exception on create table - retrying it: "+x.getMessage());
						} else {
							throw x;
						}
					}
				}
			}

			Thread[] writers = new Thread[20];
			for (int i = 0; i < writers.length; i++) {
				writers[i] = createWriter(i, tables);
				writers[i].start();
			}

			for (int i = 0; i < writers.length; i++) {
				writers[i].join();
			}

			for (int i = 0; i < tables.length; i++) {
				for (;;) {
					try {
						tr.dropTable(tables[i].getName());
						break;
					} catch (SQLException x) {
						if (x.getErrorCode() == ErrorCode.CONNECTION_BROKEN_1) {
							log.info("exception on drop table - retrying it: "+x.getMessage());
						} else {
							throw x;
						}
					}
				}
			}
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
				} catch (SQLException x) {
					if (x.getErrorCode() == ErrorCode.GENERAL_ERROR_1) {
						log.error("got SQL exception within writer thread", x);
					} else {
						log.error("got SQL exception within writer thread: "+x.getMessage());
					}
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
