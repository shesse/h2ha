/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
	private String sideName;
	
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
	public ServerProcess(String sideName, File dbDir, int localTcpPort, int localSyncPort, int peerSyncPort)
	{
		log.debug(sideName+": ServerProcess()");

		this.sideName = sideName;
		this.dbDir = dbDir;
		this.localTcpPort = localTcpPort;
		this.localSyncPort = localSyncPort;
		this.peerSyncPort = peerSyncPort;
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * @param haCacheSize
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 */
	public void start(int haCacheSize, boolean isPrimary)
		throws IOException, InterruptedException
	{
		stop();

		log.info(sideName+": creating " + dbDir);
		dbDir.mkdirs();

		List<String> cmd = new ArrayList<String>();

		/*
		String testcfg = System.getProperty("user.home") + "/config/h2";
		if (isPrimary) {
			testcfg += "/prim";
		} else {
			testcfg += "/sec";
		}

		String l4j = testcfg + "/log4j.properties";
		if (new File(l4j).canRead()) {
			cmd.add("-Dlog4j.configuration=file:///" + l4j);
		}
		*/
		cmd.add("-Dstderr.threshold=DEBUG");

		String[] serverCommand = {//
			"-DhaTestProc=" + sideName, //
				"com.shesse.h2ha.H2HaServer",//
				"server",//
				"-haPeerHost", "localhost",//
				"-haPeerPort", String.valueOf(peerSyncPort), //
				"-haListenPort", String.valueOf(localSyncPort),//
				"-tcpPort", String.valueOf(localTcpPort),//
				"-ifExists",//
				"-haBaseDir", dbDir.getPath(),//
				"-masterPriority", (isPrimary ? "20" : "10"),//
			};

		cmd.addAll(Arrays.asList(serverCommand));

		if (haCacheSize >= 0) {
			cmd.add("-haCacheSize");
			cmd.add(String.valueOf(haCacheSize));
		}

		log.info(sideName+": starting up instance");
		log.info(sideName+": arguments=" + cmd);
		dbProcess = ProcessUtils.startJavaProcess(sideName, cmd);

		log.info(sideName+": instance has been started");
	}

	/**
	 * @throws InterruptedException
	 * 
	 */
	public void cleanup()
		throws InterruptedException
	{
		stop();

		log.info(sideName+": removing " + dbDir);
		FileUtils.deleteRecursive(dbDir);
	}

	/**
	 * @throws InterruptedException
	 * 
	 */
	private ReplicationServerStatus getServerStatus()
		throws InterruptedException
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
	public void waitUntilActive()
		throws IOException, InterruptedException
	{
		log.info(sideName+": waiting until server becomes active");
		for (int i = 30; i >= 0; i--) {
			if (getServerStatus().isActive())
				return;
			Thread.sleep(500L);
		}
		Assert.fail(sideName+": instance did not become active");
	}

	/**
	 * 
	 */
	public void waitUntilMaster()
		throws IOException, InterruptedException
	{
		log.info(sideName+": waiting until server becomes master");
		for (int i = 30; i >= 0; i--) {
			if (getServerStatus().isMaster())
				return;
			Thread.sleep(500L);
		}
		Assert.fail(sideName+": instance did not become master");
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws SQLException
	 * 
	 */
	public void createDatabase(String dbName, String adminUser, String adminPassword)
		throws IOException, InterruptedException, SQLException
	{
		ReplicationServerStatus rs = getServerStatus();
		rs.createDatabase(dbName, adminUser, adminPassword);
	}

	/**
	 * @throws InterruptedException
	 * 
	 */
	public void stop()
		throws InterruptedException
	{
		if (dbProcess != null) {
			log.info(sideName+": stopping db server process");
			dbProcess.destroy();
			dbProcess.waitFor();
		}

	}

	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
