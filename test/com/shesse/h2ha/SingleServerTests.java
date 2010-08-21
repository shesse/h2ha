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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
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

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @throws SQLException 
     */
    public SingleServerTests() throws SQLException
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
        servers.startA(true);
        servers.waitUntilAIsActive();
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
        Assert.assertTrue(new File(servers.getDirA(), "test.data.db").exists());
    }
    
    /**
     * Test Purpose: verify that data stored during one DB run is
     * accessible in another run
     * @throws SQLException 
     * @throws InterruptedException 
     * @throws IOException 
     */
    @Test
    public void verifyPersistence() throws SQLException, InterruptedException, IOException
    {
        TestTable table = tr.createTable();
        table.insertRecord();
        
        dbManager.shutdown();
        servers.stop();
        
        servers.startA(true);
        servers.waitUntilAIsActive();
        
        Assert.assertTrue(table.getNoOfRecords() == 1);
    }
    
     
    /**
     * Test Purpose: verify that auto-reconnect is working. Note: standard H2 
     * does not support combining multiple addresses and auto reconnect
     * @throws SQLException 
     * @throws InterruptedException 
     * @throws IOException 
     */
    @Ignore @Test
    public void verifyAutoreconnect() throws SQLException, InterruptedException, IOException
    {
        dbManager.shutdown();
        dbManager.setAutoReconnect(true);
        
        TestTable table = tr.createTable();
        table.insertRecord();
        
        servers.stop();
        
        servers.startA(true);
        servers.waitUntilAIsActive();
        
        Assert.assertTrue(table.getNoOfRecords() == 1);
    }
    
     
    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
