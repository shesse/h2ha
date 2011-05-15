/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.File;
import java.io.IOException;

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
    private static final File dbBaseDir = new File("/tmp/h2hatest");

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
    private int haCacheSize = -1;

    /** */
    private ServerProcess processA = new ServerProcess(dbDirA, tcpPortA, syncPortA, syncPortB);
    
    /** */
    private ServerProcess processB = new ServerProcess(dbDirB, tcpPortB, syncPortB, syncPortA);
    

    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public ServerProcessPair()
    {
        log.debug("ServerProcessPair()");
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @param haCacheSize
     */
    public void setHaCacheSize(int haCacheSize)
    {
	this.haCacheSize = haCacheSize;
    }

    /**
     * @throws InterruptedException 
     * @throws IOException 
     * 
     */
    public void start() throws IOException, InterruptedException
    {
        startA();
        startB();
    }
    
    /**
     * @throws InterruptedException 
     * @throws IOException 
     * 
     */
    public void startA()
    throws IOException, InterruptedException
    {
        processA.start(haCacheSize, true);
    }
    
    /**
     * @throws InterruptedException 
     * @throws IOException 
     * 
     */
    public void startB()
    throws IOException, InterruptedException
    {
        processB.start(haCacheSize, false);
    }
    
    /**
     * @throws InterruptedException 
     * @throws IOException 
     * 
     */
    public void cleanup() throws IOException, InterruptedException
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
    public void waitUntilActive() throws IOException, InterruptedException
    {
        waitUntilAIsActive();
        waitUntilBIsActive();
    }
    
    /**
     * @throws InterruptedException 
     * @throws IOException 
     * 
     */
    public void waitUntilAIsActive() throws IOException, InterruptedException
    {
        processA.waitUntilActive();
    }
    
    /**
     * @throws InterruptedException 
     * @throws IOException 
     * 
     */
    public void waitUntilBIsActive() throws IOException, InterruptedException
    {
        processB.waitUntilActive();
    }
    
    /**
     * @throws InterruptedException 
     * 
     */
    public void stop() throws InterruptedException
    {
        processA.stop();
        processB.stop();
    }
    
    /**
     * @throws InterruptedException 
     * 
     */
    public void stopA() throws InterruptedException
    {
        processA.stop();
    }
    
    /**
     * @throws InterruptedException 
     * 
     */
    public void stopB() throws InterruptedException
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
     * 
     */
    public boolean contentEquals()
    {
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
