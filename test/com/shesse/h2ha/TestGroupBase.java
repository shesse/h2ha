/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;


import java.io.IOException;

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

}