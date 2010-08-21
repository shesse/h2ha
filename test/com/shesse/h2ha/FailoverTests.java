/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Random;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
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
    public FailoverTests() throws SQLException
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
        servers.start();
        servers.waitUntilActive();
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


    @Ignore @Test
    public void createAndDrop()
    throws SQLException, IOException, InterruptedException {
        log.info("createAndDrop");
        
        TestTable table = tr.createTable();
        tr.dropTable(table.getName());

        tr.getDbManager().syncWithAllReplicators();
         
        Assert.assertTrue(servers.contentEquals());
    }
    
     
    @Test
    public void largeDb()
    throws SQLException, IOException, InterruptedException {
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
        
        log.info("wait for sync");
        tr.getDbManager().syncWithAllReplicators();
        log.info("sync finished");
         
        Assert.assertTrue(servers.contentEquals());
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
            public void body() throws SQLException
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
                        log.info("writer["+wi+"]: "+(i+1));
                    }
                }
            }
        };
    }
    
    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
