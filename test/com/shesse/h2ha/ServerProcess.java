/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.junit.Assert;

/**
 *
 * @author sth
 */
public class ServerProcess
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(ServerProcess.class);

    /** */
    private File dbDir;
    
    /** */
    private int localTcpPort;
    
    /** */
    private int localSyncPort;
    
    /** */
    private int peerSyncPort;
    
    /** */
    private Process dbProcess = null;
    
    /** */
    private ReplicationServerStatus serverStatus = null;
    
    /** */
    private Thread serverConnectionThread = null;
    
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public ServerProcess(File dbDir, int localTcpPort,
                         int localSyncPort, int peerSyncPort)
    {
        log.debug("ServerProcess()");
        
        this.dbDir = dbDir;
        this.localTcpPort = localTcpPort;
        this.localSyncPort = localSyncPort;
        this.peerSyncPort = peerSyncPort;
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @throws IOException 
     * @throws InterruptedException 
     * 
     */
    public void start(boolean isPrimary)
    throws IOException, InterruptedException
    {
        stop();
        
        log.info("creating "+dbDir);
        dbDir.mkdirs();
        
        String instName = dbDir.getName();
        
        String testcfg = System.getProperty("user.home")+"/config/h2";
        if (isPrimary) {
            testcfg += "/prim";
        } else {
            testcfg += "/sec";
        }
        
        String[] serverCommand = {//
            "-Dlog4j.configuration=file:///"+testcfg+"/log4j.properties",
            "-DhaTestProc="+instName,
            "com.shesse.h2ha.H2HaServer",//
            "-haPeerHost", "localhost",//
            "-haPeerPort", String.valueOf(peerSyncPort), //
            "-haListenPort", String.valueOf(localSyncPort),//
            "-tcpAllowOthers",//
            "-tcpPort", String.valueOf(localTcpPort),//
            "-baseDir", "ha://",//
            "-haBaseDir", dbDir.getPath(),//
            "-masterPriority", (isPrimary ? "20" : "10"),//
        };
        
        log.info("starting up instance "+instName);
        dbProcess = ProcessUtils.startJavaProcess(instName, Arrays.asList(serverCommand));

        log.info("instance "+instName+" has been started");
    }
    
    /**
     * @throws InterruptedException 
     * 
     */
    private ReplicationServerStatus getServerStatus() throws InterruptedException
    {
        if (serverConnectionThread != null && !serverConnectionThread.isAlive()) {
            serverConnectionThread = null;
            serverStatus = null;
        }
        
        if (serverStatus == null) {
            serverStatus = new ReplicationServerStatus();
            for (int i = 20; i >= 0; i--) {
                if (serverStatus.tryToConnect("localhost", localSyncPort, 20000)) {
                    serverConnectionThread = new Thread(serverStatus);
                    serverConnectionThread.start();
                    return serverStatus;
                }
                Thread.sleep(500L);
            }
            
            Assert.fail("cannot connect to DB server process");
        }
        
        return serverStatus;
    }
    /**
     * @throws InterruptedException 
     * @throws IOException 
     * 
     */
    public void waitUntilActive() throws IOException, InterruptedException
    {
        log.info("waiting until server becomes active");
        for (int i = 30; i >= 0; i--) {
            if (getServerStatus().isActive()) return;
            Thread.sleep(500L);
        }
        Assert.fail("instance does not become active");
    }
    
    /**
     * @throws InterruptedException 
     * 
     */
    public void stop() throws InterruptedException
    {
        String instName = dbDir.getName();

        if (dbProcess != null) {
            log.info("stopping db server process "+instName);
            dbProcess.destroy();
            dbProcess.waitFor();
        }

    }

    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
