/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;


import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

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
    protected ServerProcessPair servers = new ServerProcessPair();

    /** */
    protected DbManager dbManager = new DbManager();
    
    /** */
    protected TableRegistry tr = new TableRegistry(dbManager);

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * 
     */
    public TestGroupBase()
    {
        super();
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
            ProcessUtils.ExecResult psres = ProcessUtils.exec("ps -ewwwf | grep 'DhaTestProc=' | grep -v grep");
            
            for (String psline: psres.outputLines) {
                String[] fields = psline.split("\\s+");
                log.info("terminating old ha test process "+fields[1]);
                ProcessUtils.exec("kill -9 "+fields[1]);
                stillActive = true;
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
		    log.info("could not connect to DB server: "+x.getMessage());
		    throw x;
		}
		
		try {
		    Statement stmnt = conn.createStatement();

		    transBody.run(stmnt);
		    
		    conn.commit();
		    
		    return;

		} catch (SQLException x) {
		    log.info("exception during transaction - rollback: "+x.getMessage());
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
}