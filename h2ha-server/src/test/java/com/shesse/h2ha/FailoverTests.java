/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
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
public class FailoverTests
	extends TestGroupBase
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	static private Logger log = Logger.getLogger(FailoverTests.class);

	/** */
	private Random rnd = new Random();

	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @throws SQLException
	 */
	public FailoverTests()
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
	public void createAndDrop()
		throws SQLException, IOException, InterruptedException
	{
		log.info("######################################################################");
		log.info("######################################################################");
		log.info("start of test createAndDrop");

		TestTable table = tr.createTable();
		tr.dropTable(table.getName());

		tr.getDbManager().syncWithAllReplicators();

		Assert.assertTrue(servers.contentEquals());
		log.info("end of test createAndDrop");
	}


	@Test
	public void transferMaster()
		throws SQLException, IOException, InterruptedException
	{
		log.info("######################################################################");
		log.info("######################################################################");
		log.info("start of test transferMaster");

		final TestTable table = tr.createTable();

		TransactionBody insertRecords = new TransactionBody() {
			public void run(Statement stmnt)
				throws SQLException
			{
				for (int i = 0; i < 200; i++) {
					table.insertRecordWithoutCommit(stmnt);
				}
			}
		};

		log.info("step 1: writing 200 records to redundant DB");
		executeTransaction(insertRecords);

		int nrec = table.getNoOfRecords();
		log.info("step 1 done: wrote 200 records to redundant DB - new count = " + nrec);
		Assert.assertTrue(nrec == 200);


		log.info("step 2: transfering master to DB B");
		tr.getDbManager().transferMasterRole();
		servers.waitUntilBIsMaster();
		servers.waitUntilActive();

		log.info("step 2 done: transfered master role");
		nrec = table.getNoOfRecords();
		Assert.assertTrue(nrec == 200);


		log.info("step 3: writing 200 records to redundant DB");
		executeTransaction(insertRecords);

		nrec = table.getNoOfRecords();
		log.info("step 3 done: wrote 200 records to redundant DB - new count = " + nrec);
		Assert.assertTrue(nrec == 400);


		log.info("step 4: transfering master to DB A");
		tr.getDbManager().transferMasterRole();
		servers.waitUntilAIsMaster();
		servers.waitUntilActive();

		log.info("step 4 done: transfered master role");
		nrec = table.getNoOfRecords();
		Assert.assertTrue(nrec == 400);


		log.info("step 5: writing 200 records to redundant DB");
		executeTransaction(insertRecords);

		nrec = table.getNoOfRecords();
		log.info("step 5 done: wrote 200 records to redundant DB - new count = " + nrec);
		Assert.assertTrue(nrec == 600);


		log.info("step 6: transfering master to DB B");
		tr.getDbManager().transferMasterRole();
		servers.waitUntilActive();

		log.info("step 6 done: transfered master role");
		nrec = table.getNoOfRecords();
		Assert.assertTrue(nrec == 600);

		tr.getDbManager().syncWithAllReplicators();
		log.info("Failover Pair is in sync");


		Assert.assertTrue(servers.contentEquals());
		log.info("end of test failoverOutsideTransaction");
	}


	@Test
	public void multiFailover()
		throws SQLException, IOException, InterruptedException
	{
		for (int i = 0; i < 300; i++) {
			if (i > 0) {
				tearDown();
				setUp();
			}
			log.info("######################################################################");
			log.info("loop "+i);
			failoverDuringTransaction();
		}
	}
	
	@Test
	public void failoverDuringTransaction()
		throws SQLException, IOException, InterruptedException
	{
		log.info("######################################################################");
		log.info("######################################################################");
		log.info("start of test failoverDuringTransaction");

		final TestTable table = tr.createTable();

		TransactionBody insertRecords = new TransactionBody() {
			public void run(Statement stmnt)
				throws SQLException
			{
				for (int i = 0; i < 200; i++) {
					table.insertRecordWithoutCommit(stmnt);
				}
			}
		};

		TransactionBody insertAndTerminateA = new TransactionBody() {
			public void run(Statement stmnt)
				throws SQLException
			{
				for (int i = 0; i < 100; i++) {
					table.insertRecordWithoutCommit(stmnt);
				}

				log.info("terminating DB A");
				try {
					servers.stopA();
				} catch (InterruptedException x) {
					log.error("InterruptedException", x);
				}
				log.info("DB A has been stopped");

				for (int i = 0; i < 100; i++) {
					table.insertRecordWithoutCommit(stmnt);
				}
			}
		};

		log.info("step 1: writing 200 records to redundant DB");
		executeTransaction(insertRecords);

		int nrec = table.getNoOfRecords();
		log.info("step 1 done: wrote 200 records to redundant DB - new count = " + nrec);
		Assert.assertTrue(nrec == 200);


		tr.getDbManager().syncWithAllReplicators();
		log.info("Failover Pair is in sync");


		log.info("step 2: writing 200 records and terminating A during transaction");
		executeTransaction(insertAndTerminateA);


		nrec = table.getNoOfRecords();
		log.info("step 2 done: wrote 200 records to DB - new count = " + nrec);
		Assert.assertTrue(nrec == 400);


		log.info("starting DB A");
		servers.startA();
		servers.waitUntilAIsActive();


		log.info("step 3: writing 200 records to re-established redundant DB");
		executeTransaction(insertRecords);

		nrec = table.getNoOfRecords();
		log.info("step 3 done: wrote 200 records to re-established redundant DB - new count = " +
			nrec);
		Assert.assertTrue(nrec == 600);


		tr.getDbManager().syncWithAllReplicators();
		log.info("Failover Pair is in sync");


		Assert.assertTrue(servers.contentEquals());
		log.info("end of test failoverOutsideTransaction");
	}


	@Test
	public void failoverOutsideTransaction()
		throws SQLException, IOException, InterruptedException
	{
		log.info("######################################################################");
		log.info("######################################################################");
		log.info("start of test failoverOutsideTransaction");

		final TestTable table = tr.createTable();

		TransactionBody insertRecords = new TransactionBody() {
			public void run(Statement stmnt)
				throws SQLException
			{
				for (int i = 0; i < 200; i++) {
					table.insertRecordWithoutCommit(stmnt);
				}
			}
		};

		log.info("step 1: writing 200 records to redundant DB");
		executeTransaction(insertRecords);

		int nrec = table.getNoOfRecords();
		log.info("step 1 done: wrote 200 records to redundant DB - new count = " + nrec);
		Assert.assertTrue(nrec == 200);


		tr.getDbManager().syncWithAllReplicators();
		log.info("Failover Pair is in sync");


		servers.stopA();
		log.info("DB A has been stopped");


		log.info("step 2: writing 200 records to DB B");
		executeTransaction(insertRecords);

		nrec = table.getNoOfRecords();
		log.info("step 2 done: wrote 200 records to DB B - new count = " + nrec);
		Assert.assertTrue(nrec == 400);


		log.info("starting DB A");
		servers.startA();
		servers.waitUntilAIsActive();


		log.info("step 3: writing 200 records to re-established redundant DB");
		executeTransaction(insertRecords);

		nrec = table.getNoOfRecords();
		log.info("step 3 done: wrote 200 records to re-established redundant DB - new count = " +
			nrec);
		Assert.assertTrue(nrec == 600);


		tr.getDbManager().syncWithAllReplicators();
		log.info("Failover Pair is in sync");


		servers.stopB();
		log.info("DB B has been stopped");


		log.info("step 4: writing 200 records to DB A");
		executeTransaction(insertRecords);

		nrec = table.getNoOfRecords();
		log.info("step 4 done: wrote 200 records to DB A - new count = " + nrec);
		Assert.assertTrue(nrec == 800);


		log.info("starting DB B");
		servers.startB();
		servers.waitUntilBIsActive();

		nrec = table.getNoOfRecords();
		log.info("DB B is running again - new count = " + nrec);
		Assert.assertTrue(nrec == 800);

		tr.getDbManager().syncWithAllReplicators();

		Assert.assertTrue(servers.contentEquals());
		log.info("end of test failoverOutsideTransaction");
	}


	@Test
	public void largeDb()
		throws SQLException, IOException, InterruptedException
	{
		log.info("######################################################################");
		log.info("######################################################################");
		log.info("start of test largeDb");

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

		log.info("wait for sync");
		tr.getDbManager().syncWithAllReplicators();
		log.info("sync finished");

		Assert.assertTrue(servers.contentEquals());
		log.info("end of test largeDb");
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
