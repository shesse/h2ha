/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.h2.store.fs.FilePath;
import org.h2.tools.RunScript;
import org.h2.tools.Server;
import org.h2.tools.Shell;
import org.h2.tools.SimpleResultSet;

import com.shesse.dbdup.DbDuplicate;

/**
 * 
 * @author sth
 */
public class H2HaServer
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	private static Logger log = Logger.getLogger(H2HaServer.class);

	/** */
	private List<String> args;

	/** */
	private FileSystemHa fileSystem;

	/** */
	private ReplicationServer server;

	/** */
	private List<String> serverArgs = new ArrayList<String>();

	/** */
	private Server tcpDatabaseServer;

	/** */
	private String peerHost = null;

	/** */
	private String haBaseDir = null;

	/** */
	private int masterPriority = 10;

	/** */
	private ReplicationClientInstance client = null;

	/** */
	private ReplicationServerInstance[] replicators = new ReplicationServerInstance[0];

	/** */
	private BlockingQueue<Runnable> controlQueue = new LinkedBlockingQueue<Runnable>();
	
	/** */
	private Timer timer = new Timer(true);

	/** */
	private volatile boolean shutdownRequested = false;

	/** */
	private String uuid = UUID.randomUUID().toString();

	/** */
	private Properties hafsm = new Properties();

	/** */
	public enum FailoverState { //
		INITIAL, //
		STARTING_STANDALONE, //
		MASTER_STANDALONE, //
		IDLE, //
		STARTING, //
		MASTER, //
		STOPPING, //
		TRANSFERING, //
		TRANSFERED, //
		SLAVE_SYNCING, //
		SLAVE, //
		SLAVE_STOPPING, //
	};

	/** */
	public enum Event { //
		HA_STARTUP, //
		NO_PEER, // we don't have a peer
		MASTER_STARTED, //
		MASTER_STOPPED, //
		CONNECTED_TO_PEER, //
		CANNOT_CONNECT, // parameter is: indication if local data is valid for a
						// DB
		SYNC_COMPLETED, //
		PEER_STATE, // parameter is: state of HA peer
		DISCONNECTED, //
		TRANSFER_MASTER, //
		SLAVE_STOPPED, //
	};

	/** */
	private volatile FailoverState failoverState = FailoverState.INITIAL;

	/** */
	private static LockHandle staticBaseLock = null;

	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 */
	public H2HaServer(List<String> args)
	{
		log.debug("H2HaServer()");

		this.args = args;


		InputStream fsmStream = getClass().getResourceAsStream("hafsm.properties");
		if (fsmStream == null) {
			throw new IllegalStateException("cannot find hafsm.properties");
		}

		try {
			hafsm.load(fsmStream);
		} catch (IOException e) {
			throw new IllegalStateException("cannot read hafsm.properties", e);
		}

		for (String key : hafsm.stringPropertyNames()) {
			String[] trans = hafsm.getProperty(key, "").split("\\s+");
			if (trans.length != 2) {
				throw new IllegalStateException("invalid FSM transition for " + key);
			}

			try {
				getAction(trans[0]);
			} catch (NoSuchMethodException x) {
				throw new IllegalStateException("unknown action in FSM transition for " + key);
			}


			try {
				FailoverState.valueOf(trans[1]);
			} catch (IllegalArgumentException x) {
				throw new IllegalStateException("unknown target state in FSM transition for " + key);
			}
		}
	}


	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] argsArray)
		throws InterruptedException
	{
		List<String> args = new ArrayList<String>(Arrays.asList(argsArray));

		try {
			String command = "";
			if (args.size() > 0) {
				command = args.remove(0);
			}

			if ("server".equals(command)) {
				performServerCommand(args);

			} else if ("console".equals(command)) {
				performConsoleCommand(args);

			} else if ("script".equals(command)) {
				performScriptCommand(args);

			} else if ("create".equals(command)) {
				performCreateCommand(args);

			} else if ("dbdup".equals(command)) {
				DbDuplicate.main(args);

			} else if ("version".equals(command)) {
				performVersionCommand(args);

			} else {
				System.err.println("usage: java -jar " + getJarname() + " <command> [option ...]");
				System.err.println("with command:");
				System.err.println("    server");
				System.err.println("        Run as H2HA database server");
				System.err.println("    console");
				System.err.println("        Provide a text console for a H2HA database");
				System.err.println("    script");
				System.err.println("        Submit a SQL script to the database");
				System.err.println("    create");
				System.err.println("        Create a new database");
				System.err.println("    version");
				System.err.println("        print version info");

				System.exit(1);

			}

		} catch (Throwable x) {
			log.fatal("unexpected exception within main thread", x);
			System.exit(1);

		} finally {
			if (staticBaseLock != null) {
				staticBaseLock.release();
			}
		}
	}

	/**
	 * 
	 */
	public void shutdown()
	{
		shutdownRequested = true;
		enqueue(new Runnable() {
			public void run()
			{
				// no content - we simply want the controlQueue.take()
				// in the main loop to return.
			}
		});
	}


	/**
	 * @param args
	 * @throws InterruptedException
	 */
	private static void performServerCommand(List<String> args)
		throws InterruptedException
	{
		new H2HaServer(args).runHaServer();
	}

	/**
	 * 
	 */
	private void showServerUsage()
	{
		System.err.println("usage: java -jar " + getJarname() + " server [option ...]");
		System.err.println("with option:");
		System.err.println("    -help or -?");
		System.err.println("        Print the list of options");
		System.err.println("    -haListenPort");
		System.err.println("        TCP port to listen for HA connections (default = 8234)");
		System.err.println("    -haPeerHost");
		System.err.println("        host name of HA peer");
		System.err.println("    -haPeerPort");
		System.err.println("        peer HA port (default = 8234)");
		System.err.println("    -haRestrictPeer");
		System.err.println("        restrict incoming HA connections to those coming from -haPeerHost");
		System.err.println("    -haBaseDir");
		System.err.println("        base directory for replicated DB files");
		System.err.println("    -masterPriority");
		System.err.println("        priority to become a master. Integer value.");
		System.err.println("        Default is 10. Higher values are higher priorities.");
		System.err.println("    -autoFailback");
		System.err.println("        automatically transfer master role if configured master");
		System.err.println("        comes back again afetr a failure.");
		System.err.println("    -haMaxQueueSize");
		System.err.println("        Queue size for HA replication (default = 5000)");
		System.err.println("    -haMaxEnqueueWait");
		System.err.println("        max millis to wait to enqueue HA data (default = 60000)");
		System.err.println("    -haMaxWaitingMessages");
		System.err.println("        max no of messages in message queue before connection");
		System.err.println("        is considered defect. 0 = default = unlimited");
		System.err.println("    -statisticsInterval");
		System.err.println("        cycle millis for statistics logging, default = 300000");
		System.err.println("    -idleTimeout");
		System.err.println("        max millis waiting for activity on a peer connection, default = 20000");
		System.err.println("    -haConnectTimeout");
		System.err.println("        max millis to wait for HA connection, default = 10000");
		System.err.println("    -connectRetry");
		System.err.println("        max retries for a single connect attempt. Default = 5");
		System.err.println("    -tcpPort");
		System.err.println("        port for database connections (default: 9092)");
		System.err.println("    -trace");
		System.err.println("        print additional trace information");
		System.err.println("");
	
	
		System.exit(1);
	}


	/**
	 * @throws InterruptedException
	 * 
	 */
	private void runHaServer()
		throws InterruptedException
	{
		serverArgs = new ArrayList<String>();
	
		serverArgs.add("-tcpAllowOthers");
		serverArgs.add("-baseDir");
		serverArgs.add("ha:///");
		serverArgs.add("-ifExists");
	
		serverArgs.addAll(args);
	
		peerHost = removeOptionWithValue(serverArgs, "-haPeerHost", null);
		haBaseDir = removeOptionWithValue(serverArgs, "-haBaseDir", null);
		masterPriority = removeOptionWithInt(serverArgs, "-masterPriority", 10);
	
		removeOptionWithValue(serverArgs, "-haPeerPort", null);
		removeOptionWithValue(serverArgs, "-haConnectTimeout", null);
		long statisticsInterval = removeOptionWithInt(serverArgs, "-statisticsInterval", 300000);
		removeOptionWithValue(serverArgs, "-idleTimeout", null);
		removeOptionWithValue(serverArgs, "-connectRetry", null);
		removeOptionWithValue(serverArgs, "-haListenPort", null);
		removeOptionWithValue(serverArgs, "-haMaxQueueSize", null);
		removeOptionWithValue(serverArgs, "-haMaxEnqueueWait", null);
		removeOptionWithValue(serverArgs, "-haMaxWaitingMessages", null);
		removeOption(serverArgs, "-autoFailback");
		removeOption(serverArgs, "-haRestrictPeer");
	
	
		if (findOption(serverArgs, "-?")) {
			showServerUsage();
			System.exit(1);
		}
		if (findOption(serverArgs, "-help")) {
			showServerUsage();
			System.exit(1);
		}
	
	
		if (haBaseDir == null) {
			System.err.println("mandatory flag -haBaseDir is missing");
			showServerUsage();
			System.exit(1);
		}
	
		if (!new File(haBaseDir).exists()) {
			System.err.println("HA base dir " + haBaseDir + " does not exist");
			showServerUsage();
			System.exit(1);
		}
	
		LockHandle baseLock = acquireHaBaseLock(haBaseDir);
		if (baseLock == null) {
			System.err.println("could not get lock for " + haBaseDir +
				" - some other process is probably using it");
			System.exit(1);
		}
	
		try {
			fileSystem = new FileSystemHa(this, args);
			server = new ReplicationServer(this, fileSystem, args);
			server.start();
			
			if (statisticsInterval > 0) {
				timer.schedule(new TimerTask() {
					@Override
					public void run()
					{
						logStatistics();
					}
				}, 2000, statisticsInterval);
			}
	
			if (peerHost == null) {
				log.warn("no haPeerHost specified - running in master only mode!");
				applyEvent(Event.NO_PEER, null, null);
	
			} else {
				applyEvent(Event.HA_STARTUP, null, null);
			}
	
			while (!shutdownRequested) {
				Runnable queueEntry = controlQueue.take();
				queueEntry.run();
			}
	
		} catch (TerminateThread x) {
			System.err.println(x.getMessage());
	
		} finally {
			baseLock.release();
		}
	}


	/**
	 * 
	 */
	private static void showConsoleUsage()
	{
		System.err.println("usage: java -jar " + getJarname() + " console [option ...]");
		System.err.println("with option:");
		System.err.println("    -help or -?");
		System.err.println("        Print the list of options");
		System.err.println("    -server hostname[:port][,hostname[:port]]");
		System.err.println("        The database server address(es)");
		System.err.println("    -haBaseDir <directory>");
		System.err.println("        Base directory of HA database");
		System.err.println("    -database <database-name>");
		System.err.println("        The database name");
		System.err.println("    -user <user>");
		System.err.println("        The user name (default: sa)");
		System.err.println("    -password <pwd>");
		System.err.println("        The password");
		System.err.println("    -sql <statements>");
		System.err.println("        Execute the SQL statements and exit");
		System.err.println("    -url <url>");
		System.err.println("        Explicit spec of database URL (jdbc:...)");
		System.err.println("    -driver <class>");
		System.err.println("        The JDBC driver class to use (not required in most cases)");
		System.err.println("    -properties <dir>");
		System.err.println("        Load the server properties from this directory");
		System.err.println("");
		System.err.println("If special characters don't work as expected, you may need to use");
		System.err.println("-Dfile.encoding=UTF-8 (Mac OS X) or CP850 (Windows).");

		System.exit(1);

	}

	/**
	 * @param args2
	 */
	private static void performConsoleCommand(List<String> args)
	{
		try {
			createUrl(args);
			
			new Shell() {
				@Override
				protected void showUsage()
				{
					showConsoleUsage();
				}

			}.runTool(args.toArray(new String[0]));
			
		} catch (SQLException x) {
			System.err.println(x.getMessage() + "\n");
			showConsoleUsage();
		}

	}

	/**
	 *
	 */
	private static void showScriptUsage()
	{
		System.err.println("usage: java -jar " + getJarname() + " script [option ...]");
		System.err.println("with option:");
		System.err.println("    -help or -?");
		System.err.println("        Print the list of options");
		System.err.println("    -server hostname[:port][,hostname[:port]]");
		System.err.println("        The database server address(es)");
		System.err.println("    -haBaseDir <directory>");
		System.err.println("        Base directory of HA database");
		System.err.println("    -database <database-name>");
		System.err.println("        The database name");
		System.err.println("    -user <user>");
		System.err.println("        The user name (default: sa)");
		System.err.println("    -password <pwd>");
		System.err.println("        The password");
		System.err.println("    -script <file>;");
		System.err.println("        The script file to run (default: backup.sql)");
		System.err.println("    -showResults");
		System.err.println("        Show the statements and the results of queries");
		System.err.println("    -checkResults");
		System.err.println("        Check if the query results match the expected results");
		System.err.println("    -continueOnError");
		System.err.println("        Continue even if the script contains errors");
		System.err.println("    -url <url>");
		System.err.println("        Explicit spec of database URL (jdbc:...)");
		System.err.println("    -driver <class>");
		System.err.println("        The JDBC driver class to use (not required in most cases)");
		System.err.println("");

		System.exit(1);

	}

	/**
	 * @param args2
	 * @throws SQLException
	 */
	private static void performScriptCommand(List<String> args)
	{
		try {
			createUrl(args);
			
			new RunScript() {
				@Override
				protected void showUsage()
				{
					showScriptUsage();
				}

			}.runTool(args.toArray(new String[0]));
			
		} catch (SQLException x) {
			System.err.println(x.getMessage() + "\n");
			showScriptUsage();
		}

	}


	/**
	 * 
	 */
	private static void showCreateUsage()
	{
		System.err.println("usage: java -jar " + getJarname() + " create [option ...]");
		System.err.println("with option:");
		System.err.println("    -help or -?");
		System.err.println("        Print the list of options");
		System.err.println("    -server hostname[:port][,hostname[:port]]");
		System.err.println("        The database server address(es)");
		System.err.println("    -haControlPort <port>");
		System.err.println("        Control port of master H2HA database server");
		System.err.println("    -haBaseDir <directory>");
		System.err.println("        Base directory of HA database");
		System.err.println("    -database <database-name>");
		System.err.println("        The database name");
		System.err.println("    -user <user>");
		System.err.println("        The user name (default: sa)");
		System.err.println("    -password <pwd>");
		System.err.println("        The password");
		System.err.println("    -script <file>;");
		System.err.println("        A script file to run");
		System.err.println("    -showResults");
		System.err.println("        Show the statements and the results of queries");
		System.err.println("    -checkResults");
		System.err.println("        Check if the query results match the expected results");
		System.err.println("    -continueOnError");
		System.err.println("        Continue even if the script contains errors");
		System.err.println("");
	
		System.exit(1);
	}


	/**
	 * @param args2
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws SQLException
	 */
	private static void performCreateCommand(List<String> args)
		throws InterruptedException
	{
		String script = findOptionWithValue(args, "-script", null);
		String user = findOptionWithValue(args, "-user", null);
		String password = findOptionWithValue(args, "-password", null);
		String haBaseDir = findOptionWithValue(args, "-haBaseDir", null);
		String server = findOptionWithValue(args, "-server", null);
		String database = findOptionWithValue(args, "-database", null);
		int ctlPort = removeOptionWithInt(args, "-haControlPort", 8234);

		removeOptionWithValue(args, "-url", null);

		if (user == null) {
			System.err.println("mandatory parameter -user is missing");
			showCreateUsage();
		}

		if (password == null) {
			System.err.println("mandatory parameter -password is missing");
			showCreateUsage();
		}

		if (database == null) {
			System.err.println("mandatory parameter -database is missing");
			showCreateUsage();
		}
		
		if (haBaseDir != null && server != null) {
			System.err.println("only one of -haBaseDir and -server may be specified");
			showCreateUsage();
		}

		try {
			if (haBaseDir != null) {
				// -haBaseDir was specified - we assume the the H2HA server is not
				// running and create a database file within the HA base directory.
				if (!new File(haBaseDir).exists()) {
					System.err.println("HA base dir " + haBaseDir + " does not exist");
					System.exit(1);
				}

				createUrl(args);
				// we will use the URL created within createUrl to create the DB
				String url = findOptionWithValue(args, "-url", null);
				try {
					Class.forName("org.h2.Driver");
				} catch (ClassNotFoundException x) {
					log.error("ClassNotFoundException", x);
					throw new SQLException("ClassNotFoundException", x);
				}

				// the DB file will be created as a side effect of opening the 
				// DB connection the first time.
				Connection conn = DriverManager.getConnection(url, user, password);
				conn.close();
				
			} else {
				// -haBaseDir was not specified - we will use -server with
				// a default of localhost to locate a h2ha server and send it a
				// create command
				if (server == null) {
					server = "localhost";
				}
				
				// if multiple servers have been specified, we try them in
				// sequence until we find the master
				ControlCommandClient commandClient = null;
				String error = null;
				for (String target: server.split(",")) {
					// if server contains a port number, it refers to the DB port.
					// For DB creation we need to connect to the control port.
					// Therefore, we strip the DB port off.
					target = target.replaceFirst(":.*", "");
					
					commandClient = new ControlCommandClient();
					if (commandClient.tryToConnect(target, ctlPort, 20000)) {
						new Thread(commandClient).start();
						
						// ask the server if it is the master
						if (commandClient.isMaster()) {
							// success - we may use this client
							break;
							
						} else {
							// not the master - close and forget this connection
							commandClient.terminate();
							commandClient = null;
						}
						
					} else {
						// could not connect - prepare for error message and forget
						if (error == null) {
							error = "cannot connect to DB server control port " + target + ":" + ctlPort;
						}
						commandClient = null;
					}
				}
				
				if (commandClient == null) {
					if (error == null) {
						System.err.println("could not find a database instance running in master role");
						System.exit(1);
					} else {
						System.err.println(error);
						System.exit(1);
					}
				}
				
				commandClient.createDatabase(database, user, password);
				commandClient.terminate();
			}

			// if we have got an init script, we will execute it
			// using RunScript
			if (script != null) {
				new RunScript() {
					@Override
					protected void showUsage()
					{
						showCreateUsage();
					}

				}.runTool(args.toArray(new String[0]));
			}
			
		} catch (SQLException x) {
			System.err.println(x.getMessage() + "\n");
			showCreateUsage();
			
		} catch (IOException x) {
			System.err.println("error communicating to " + server + ":" + ctlPort+": "+x.getMessage());
			System.exit(1);
		}

	}

	/**
	 * @param args2
	 */
	private static void performVersionCommand(List<String> args)
	{
		System.err.println("H2HA Server " + getVersionInfo());
	}

	/**
	 * 
	 */
	private static String getVersionInfo()
	{
		InputStream versionStream =
			H2HaServer.class.getClassLoader().getResourceAsStream("version.props");
		if (versionStream == null) {
			return "unknown";
		} else {
			try {
				Properties versionProps = new Properties();
				versionProps.load(versionStream);
				return versionProps.getProperty("h2ha.version", "unknown");
			} catch (IOException x) {
				return "unknown";
			} finally {
				try {
					versionStream.close();
				} catch (IOException x) {
				}
			}
		}
	}

	/**
	 * Function intended for use from an SQL select statement. It returns
	 * a single row result set containing the following columns:
	 * <ul>
	 * <li>SERVER_NAME: host name of currently active server
	 * <li>SERVER_PORT: TCP port number of the control port of the currently active server
	 * <li>LOCAL_STATUS: State the currently active server is in - most probably 'MASTER'.
	 * <li>PEER_NAME: host name of the failover peer. May be NULL when no
	 * peer has been configured.
	 * <li>PEER_PORT: TCP port number of the control port of the failover peer. 
	 * May be NULL when no
	 * peer has been configured.
	 * <li>PEER_STATUS: State the failover peer is in. May be NULL when no
	 * peer has been configured.
	 * <li>REPL_BYTES_RAW: cumulative number of bytes replicated since start of server
	 * <li>REPL_BYTES_CROPPED: cumulative number of bytes replicated since start of server
	 * after cropping unchanged initial and final parts of blocks.
	 * <li>BLOCK_CACHE_LOOKUPS: Number of block cache lookups since start of the server.
	 * <li>BLOCK_CACHE_ADDS: Number of blocks added to the block cache since start of the server.
	 * <li>BLOCK_CACHE_HITS: Nunberof block cache hits since start of the server.
	 * </ul>
	 * <p>
	 * For accessing this function from SQL, add the following ALIAS to your session:
	 * <br>
	 * create alias SERVER_INFO for "com.shesse.h2ha.H2HaServer.getServerInfo"
	 * 
	 * @param conn
	 * @param size
	 * @return
	 * @throws SQLException
	 */
	public static ResultSet getServerInfo(Connection conn)
		throws SQLException
	{
		return findFileSystem().getHaServer().getServerInfoImpl(conn);
	}

	/**
	 * 
	 * @param conn
	 * @param size
	 * @return
	 * @throws SQLException
	 */
	private ResultSet getServerInfoImpl(Connection conn)
		throws SQLException
	{
		SimpleResultSet rs = new SimpleResultSet();
		rs.addColumn("SERVER_NAME", Types.VARCHAR, 100, 0);
		rs.addColumn("SERVER_PORT", Types.INTEGER, 5, 0);
		rs.addColumn("LOCAL_STATUS", Types.VARCHAR, 20, 0);
		rs.addColumn("PEER_NAME", Types.VARCHAR, 100, 0);
		rs.addColumn("PEER_PORT", Types.INTEGER, 5, 0);
		rs.addColumn("PEER_STATUS", Types.VARCHAR, 20, 0);
		rs.addColumn("REPL_BYTES_RAW", Types.INTEGER, 20, 0);
		rs.addColumn("BLOCK_CACHE_LOOKUPS", Types.INTEGER, 20, 0);
		rs.addColumn("BLOCK_CACHE_ADDS", Types.INTEGER, 20, 0);
		rs.addColumn("BLOCK_CACHE_HITS", Types.INTEGER, 20, 0);

		String url = conn.getMetaData().getURL();
		if (url.equals("jdbc:columnlist:connection")) {
			return rs;
		}

		try {
			String serverName = InetAddress.getLocalHost().getHostName();
			int listenPort = server.getListenPort();

			rs.addRow(//
				serverName,//
				listenPort,//
				failoverState.toString(),//
				(client == null ? null : client.getPeerHost()),//
				(client == null ? null : client.getPeerPort()),//
				(client == null ? null : client.getPeerState().toString()),//
				fileSystem.getReplicationRawBytes(),//
				fileSystem.getBlockCacheLookups(),//
				fileSystem.getBlockCacheAdds(),//
				fileSystem.getBlockCacheHits()//
			);


		} catch (UnknownHostException x) {
			throw new SQLException("cannot determine local host name", x);
		}

		return rs;
	}


	/**
	 * Function intended for use from from an SQL select statement. It returns
	 * a row for each registered replicator. The row contains the following columns:
	 * <ul>
	 * <li>INSTANCE_NAME: instance name assigned to this replicator.
	 * <li>ACTIVE_SINCE: timestamp when instance became active.
	 * <li>TOTAL_BYTES_SENT: number of bytes sent to this instance
	 * <li>SEND_DELAY: number of milliseconds the last send operation to this 
	 * replicator took until it was sent out through the TCP channel. 
	 * </ul>
	 * <p>
	 * For accessing this function from SQL, add the following ALIAS to your session:
	 * <br>
	 * create alias REPLICATION_INFO for "com.shesse.h2ha.H2HaServer.getReplicationInfo"
	 * 
	 * @param conn
	 * @param size
	 * @return
	 * @throws SQLException
	 */
	public static ResultSet getReplicationInfo(Connection conn)
		throws SQLException
	{
		return findFileSystem().getHaServer().getReplicationInfoImpl(conn);
	}

	/**
	 * 
	 * @param conn
	 * @param size
	 * @return
	 * @throws SQLException
	 */
	private ResultSet getReplicationInfoImpl(Connection conn)
		throws SQLException
	{
		SimpleResultSet rs = new SimpleResultSet();
		rs.addColumn("INSTANCE_NAME", Types.VARCHAR, 100, 0);
		rs.addColumn("ACTIVE_SINCE", Types.TIMESTAMP, 0, 0);
		rs.addColumn("TOTAL_BYTES_SENT", Types.INTEGER, 20, 0);
		rs.addColumn("SEND_DELAY", Types.INTEGER, 10, 0);

		String url = conn.getMetaData().getURL();
		if (url.equals("jdbc:columnlist:connection")) {
			return rs;
		}

		for (ReplicationServerInstance server : replicators) {
			rs.addRow(//
				server.getInstanceName(),//
				server.getStartTime(),//
				server.getTotalBytesTransmitted(),//
				(int) server.getLastSendDelay()//
			);
		}

		return rs;
	}


	/**
	 * Function intended for use from from an SQL select statement. It returns a single row
	 * with a single column containing the name of a directory to 
	 * be used for storing backup files. This
	 * directory can be defined by setting the system property h2ha.backupdir
	 * when starting the H2HA server.
	 * <p>
	 * For accessing this function from SQL, add the following ALIAS to your session:
	 * <br>
	 * create alias BACKUP_DIRECTORY for "com.shesse.h2ha.H2HaServer.getBackupDirectory"
	 * 
	 * @param conn
	 * @return null if no backup directory is defined or if it cannot be used.
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	public static ResultSet getBackupDirectory(Connection conn)
		throws SQLException, IOException
	{
		String backupDir = System.getProperty("h2ha.backupdir");
		if (backupDir != null) {
			File bdf = new File(backupDir);
			backupDir = bdf.getCanonicalPath();
			if (!bdf.isDirectory()) {
				if (!bdf.mkdir()) {
					backupDir = null;
				}
			}
		}

		SimpleResultSet rs = new SimpleResultSet();
		rs.addColumn("BACKUP_DIRECTORY", Types.VARCHAR, 1000, 0);

		String url = conn.getMetaData().getURL();
		if (url.equals("jdbc:columnlist:connection")) {
			return rs;
		}

		rs.addRow(backupDir);

		return rs;
	}

	/**
	 * Procedure intended for use from from an SQL call statement. 
	 * It cleans up the directory defined as backup directory 
	 * by setting the system property "h2ha.backupdir".
	 * <p>
	 * The procedure will delete all files in this directory
	 * except the noToKeep newest ones.
	 * <p>
	 * For calling this procedure from SQL, add the following ALIAS to your session:
	 * <br>
	 * create alias CLEANUP_BACKUP_DIRECTORY for "com.shesse.h2ha.H2HaServer.cleanupBackupDirectory"
	 * 
	 */
	public static void cleanupBackupDirectory(int noToKeep)
	{
		String backupDir = System.getProperty("h2ha.backupdir");
		if (backupDir == null) {
			return;
		}
	
		File bdf = new File(backupDir);
		File[] backupFiles = bdf.listFiles();
		if (backupFiles == null) {
			return;
		}
	
		Arrays.sort(backupFiles, new Comparator<File>() {
			public int compare(File o1, File o2)
			{
				long lm1 = o1.lastModified();
				long lm2 = o2.lastModified();
	
				if (lm1 < lm2) {
					return 1;
				} else if (lm1 > lm2) {
					return -1;
				} else {
					return 0;
				}
			}
		});
	
		// descending sort -> the youngest ones come first
		for (File backupFile : backupFiles) {
			if (backupFile.isFile()) {
				if (noToKeep > 0) {
					noToKeep--;
				} else {
					backupFile.delete();
				}
			}
		}
	}


	/**
	 * Procedure intended for use from from an SQL call statement. 
	 * It initiates the transfer of the master role to the current peer.
	 * This procedure  must be called on the system that currently
	 * has the master role. If used from SQL, this is always true.
	 * <p>
	 * For calling this procedure from SQL, add the following ALIAS to your session:
	 * <br>
	 * create alias TRANSFER_ROLE for "com.shesse.h2ha.H2HaServer.transferMasterRole"
	 * 
	 * @throws SQLException
	 */
	public static void transferMasterRole()
		throws SQLException
	{
		findFileSystem().getHaServer().transferMasterRoleImpl();
	}


	/**
	 * @throws SQLException
	 * 
	 */
	private void transferMasterRoleImpl()
		throws SQLException
	{
		applyEvent(Event.TRANSFER_MASTER, null, null);
	}


	/**
	 * 
	 */
	public boolean isActive()
	{
		return failoverState == FailoverState.MASTER_STANDALONE ||
			failoverState == FailoverState.MASTER || failoverState == FailoverState.SLAVE;
	}


	/**
	 * @return
	 */
	public Boolean isMaster()
	{
		return failoverState == FailoverState.MASTER_STANDALONE ||
			failoverState == FailoverState.MASTER;
	}


	/**
	 * computes deterministic role assignment depending on current local role,
	 * local and remote master priority and local and remote UUID.
	 * 
	 * @return true if the local system is the configured master system
	 */
	public boolean isConfiguredMaster(int otherMasterPriority, String otherUuid)
	{
		if (!client.isConsistentData()) {
			return false;
		}

		boolean localWins;
		if (masterPriority > otherMasterPriority) {
			localWins = true;

		} else if (masterPriority < otherMasterPriority) {
			localWins = false;

		} else {
			int cmp = uuid.compareTo(otherUuid);
			if (cmp < 0) {
				localWins = true;

			} else {
				// UUIDs should never be equal
				localWins = false;
			}
		}

		return localWins;
	}

	/**
	 * 
	 */
	public int getMasterPriority()
	{
		return masterPriority;
	}

	/**
	 * 
	 */
	public String getUuid()
	{
		return uuid;
	}

	/**
	 * 
	 */
	public FailoverState getFailoverState()
	{
		return failoverState;
	}


	/**
	 * @param server
	 */
	public synchronized void registerReplicationInstance(ReplicationServerInstance server)
	{
		for (ReplicationProtocolInstance r : replicators) {
			if (r == server) {
				return;
			}
		}

		ReplicationServerInstance[] newServers = new ReplicationServerInstance[replicators.length + 1];
		System.arraycopy(replicators, 0, newServers, 0, replicators.length);
		newServers[replicators.length] = server;
		replicators = newServers;
	}


	/**
	 * @param server
	 */
	public void deregisterReplicationInstance(ReplicationProtocolInstance server)
	{
		for (int i = 0; i < replicators.length; i++) {
			if (replicators[i] == server) {
				ReplicationServerInstance[] newServers =
					new ReplicationServerInstance[replicators.length - 1];
				System.arraycopy(replicators, 0, newServers, 0, i);
				System.arraycopy(replicators, i + 1, newServers, i, newServers.length - i);
				replicators = newServers;
				return;
			}
		}
	}


	/**
	 * 
	 */
	public void applyEvent(final Event event, final Object parameter, final Object optParam)
	{
		enqueue(new Runnable() {
			public void run()
			{
				applyEventImpl(event, parameter, optParam);
			}
		});
	}

	/**
	 * 
	 */
	private void applyEventImpl(Event event, Object parameter, Object optParam)
	{
		log.debug("applyEventImpl " + event + ", param=" + parameter);
		String eventKey = String.valueOf(event);

		if (parameter != null) {
			eventKey += "." + parameter;
		}

		String eventKeyOpt = eventKey + "." + optParam;

		String key = failoverState + "." + eventKey;
		String keyOpt = failoverState + "." + eventKeyOpt;


		String transition = hafsm.getProperty(keyOpt);
		if (transition == null) {
			transition = hafsm.getProperty(key);

		} else {
			key = keyOpt;
			eventKey = eventKeyOpt;
		}

		if (transition == null) {
			throw new IllegalStateException("cannot find FSM entry for '" + key + "'");
		}

		log.debug("transition lookup: " + key + " -> " + transition);

		String[] transitionParts = transition.split("\\s+");
		if (transitionParts.length != 2) {
			throw new IllegalStateException("not a valid transition for '" + key + "': " +
				transition);
		}

		String actionName = transitionParts[0];
		String newStateName = transitionParts[1];

		FailoverState newState = FailoverState.valueOf(newStateName);
		FailoverState oldState = failoverState;
		if (newState != oldState) {

			log.info("changing state from " + oldState + " to " + newState + " (event was " +
				eventKey + ")");
			failoverState = newState;

			try {
				Method action = getAction(actionName);
				action.invoke(this, oldState, event, newState, parameter);

			} catch (IllegalArgumentException x) {
				throw new IllegalStateException("illegal argument for FSM action '" + actionName +
					"'", x);

			} catch (IllegalAccessException x) {
				throw new IllegalStateException("illegal access for FSM action '" + actionName +
					"'", x);

			} catch (InvocationTargetException x) {
				throw new IllegalStateException("caught exception within FSM action '" +
					actionName + "'", x.getCause());

			} catch (SecurityException x) {
				throw new IllegalStateException("security exception for FSM action '" + actionName +
					"'", x);

			} catch (NoSuchMethodException x) {
				throw new IllegalStateException("could not find action '" + actionName +
					"' for FSM transition '" + key + "'");

			}

			if (client != null) {
				try {
					client.sendStatus();
				} catch (IOException e) {
				}
			}

			ReplicationServerInstance[] sdup = replicators;
			for (ReplicationServerInstance server : sdup) {
				server.expandAndEnqueue(new ReplicationMessage() {
					private static final long serialVersionUID = 1L;

					@Override
					protected void process(ReplicationProtocolInstance instance)
						throws Exception
					{
						if (instance instanceof ServerSideProtocolInstance) {
							((ServerSideProtocolInstance) instance).sendStatus();
						}
					}

					@Override
					public int getSizeEstimate()
					{
						return 4;
					}

					@Override
					public String toString()
					{
						return "send hb";
					}
				});
			}
		}
	}

	/**
	 * @throws NoSuchMethodException
	 * 
	 */
	private Method getAction(String actionName)
		throws NoSuchMethodException
	{
		try {
			return getClass().getMethod(actionName, FailoverState.class, Event.class,
				FailoverState.class, Object.class);

		} catch (SecurityException x) {
			throw new IllegalStateException("security exception for FSM action '" + actionName +
				"'", x);

		}
	}

	/**
	 * Action routine for FSM that does nothing
	 */
	public void noAction(FailoverState oldState, Event event, FailoverState newState,
		Object parameter)
	{
	}

	/**
	 * Action routine for FSM
	 */
	public void fatalError(FailoverState oldState, Event event, FailoverState newState,
		Object parameter)
	{
		log.fatal("invalid state / event combination: " + oldState + " / " + event);
		System.exit(1);
	}

	/**
	 * Action routine for FSM
	 */
	public void logUnexpected(FailoverState oldState, Event event, FailoverState newState,
		Object parameter)
	{
		if (parameter == null) {
			log.warn("unexpected state / event combination: " + oldState + " / " + event);
		} else {
			log.warn("unexpected state / event combination: " + oldState + " / " + event + "(" +
				parameter + ")");
		}
	}

	/**
	 * Action routine for FSM
	 */
	public void startHaClient(FailoverState oldState, Event event, FailoverState newState,
		Object parameter)
	{
		client = new ReplicationClientInstance(this, fileSystem, args);
		new Thread(client, "ReplicationClient").start();
	}

	/**
	 * Action routine for FSM
	 */
	public void startDbServer(FailoverState oldState, Event event, FailoverState newState,
		Object parameter)
	{
		try {
			log.info("creating H2 TCP server with args: " + serverArgs);
			tcpDatabaseServer =
				Server.createTcpServer(serverArgs.toArray(new String[serverArgs.size()])).start();
			log.info("DB server is ready to accept connections");
			applyEvent(Event.MASTER_STARTED, null, null);

		} catch (SQLException x) {
			log.error("SQLException when starting database server", x);
			System.exit(1);
		}
	}

	/**
	 * Action routine for FSM
	 */
	public void failbackMasterRole(FailoverState oldState, Event event, FailoverState newState,
		Object parameter)
	{
		log.info("configured master is ready again - transfering the master role");
		stopDbServer(oldState, event, newState, parameter);
	}

	/**
	 * Action routine for FSM
	 */
	public void stopDbServer(FailoverState oldState, Event event, FailoverState newState,
		Object parameter)
	{
		log.info("shutting down DB server");
		tcpDatabaseServer.stop();
		applyEvent(Event.MASTER_STOPPED, null, null);
	}

	/**
	 * Action routine for FSM
	 */
	public void sendListFilesRequest(FailoverState oldState, Event event, FailoverState newState,
		Object parameter)
	{
		client.sendListFilesRequest();
	}


	/**
	 * Action routine for FSM
	 */
	public void sendStopReplicationRequest(FailoverState oldState, Event event,
		FailoverState newState, Object parameter)
	{
		client.sendStopReplicationRequest();
	}


	/**
	 * Action routine for FSM
	 */
	public void issueConnEvent(FailoverState oldState, Event event, FailoverState newState,
		Object parameter)
	{
		client.issueConnEvent();
	}


	/**
	 * Action routine for FSM
	 */
	public void issuePeerEvent(FailoverState oldState, Event event, FailoverState newState,
		Object parameter)
	{
		client.issuePeerEvent();
	}

	/**
	 * 
	 */
	protected void logStatistics()
	{
		log.info("H2HA "+getVersionInfo()+" - failoverState = " + getFailoverState());

		Runtime rt = Runtime.getRuntime();
		rt.gc();

		long mmax = rt.maxMemory();
		long mtot = rt.totalMemory();
		long mfree = rt.freeMemory();
		long mused = mtot-mfree;

		mmax /= (1024*1024);
		mtot /= (1024*1024);
		mfree /= (1024*1024);
		mused /= (1024*1024);

		log.info("memory used="+mused+
			"MB, allocated="+mtot+"MB"+", allowed="+mmax+"MB");
		
		if (client != null) {
			client.logStatistics();
		}
		
		fileSystem.logStatistics();
	}



	/**
	 * may be called when running as a master to ensure that all outstanding
	 * changes have been sent to all clients
	 */
	public static void pushToAllReplicators()
	{
		findFileSystem().flushAll();
	}


	/**
	 * may be called when running as a master to ensure that all outstanding
	 * changes have been received by all clients
	 */
	public static void syncWithAllReplicators()
	{
		log.debug("syncWithAllReplicators has been called");
		findFileSystem().syncAll();
	}


	/**
	 * locates the named option within the argument list and removes it from the
	 * list. The option is exepcted to have a value which also will be removed.
	 * 
	 * @return the value or the value of dflt if the option was not present
	 */
	public static String removeOptionWithValue(List<String> args, String optName, String dflt)
	{
		String ret = dflt;
		for (int i = 0; i < args.size() - 1;) {
			if (args.get(i).equals(optName)) {
				args.remove(i);
				ret = args.remove(i);
	
			} else {
				i++;
			}
		}
		return ret;
	}


	/**
	 * locates the named option within the argument list and returns its value.
	 * The list will remain unchanged.
	 * 
	 * @return the value or the value of dflt if the option was not present
	 */
	public static String findOptionWithValue(List<String> args, String optName, String dflt)
	{
		String ret = dflt;
		for (int i = 0; i < args.size() - 1;) {
			if (args.get(i).equals(optName)) {
				ret = args.get(i + 1);
				i += 2;
			} else {
				i++;
			}
		}
		return ret;
	}


	/**
	 * locates the named option within the argument list and removes it from the
	 * list. The option is exepcted to have a value which also will be removed.
	 * 
	 * @return the value or the value of dflt if the option was not present
	 */
	public static int removeOptionWithInt(List<String> args, String optName, int dflt)
	{
		String sval = removeOptionWithValue(args, optName, null);
		if (sval == null) {
			return dflt;
		} else {
			try {
				return Integer.parseInt(sval);
			} catch (NumberFormatException x) {
				return dflt;
			}
		}
	}


	/**
	 * locates the named option within the argument list and returns its value.
	 * The list will remain unchanged.
	 * 
	 * @return the value or the value of dflt if the option was not present
	 */
	public static int findOptionWithInt(List<String> args, String optName, int dflt)
	{
		String sval = findOptionWithValue(args, optName, null);
		if (sval == null) {
			return dflt;
		} else {
			try {
				return Integer.parseInt(sval);
			} catch (NumberFormatException x) {
				return dflt;
			}
		}
	}


	/**
	 * locates the named option within the argument list and removes it from the
	 * list.
	 * 
	 * @return true if the option was found
	 */
	public static boolean removeOption(List<String> args, String optName)
	{
		boolean ret = false;
		for (int i = 0; i < args.size();) {
			if (args.get(i).equals(optName)) {
				args.remove(i);
				ret = true;
			} else {
				i++;
			}
		}
		return ret;
	}


	/**
	 * locates the named option within the argument list and returns true if it
	 * could be found. The list will remain unchanged.
	 * 
	 * @return true if the option was found
	 */
	public static boolean findOption(List<String> args, String optName)
	{
		for (int i = 0; i < args.size(); i++) {
			if (args.get(i).equals(optName)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 
	 */
	private void enqueue(Runnable queueEntry)
	{
		try {
			controlQueue.put(queueEntry);
		} catch (InterruptedException x) {
			log.error("InterruptedException", x);
		}
	}


	/**
	 * ensures that a -url option is present. If it is not, it will be built
	 * based on the -server, -database and -haBaseDir options.
	 * 
	 * @param args
	 * @throws SQLException
	 */
	private static void createUrl(List<String> args)
		throws SQLException
	{
		String server = removeOptionWithValue(args, "-server", null);
		String database = removeOptionWithValue(args, "-database", null);
		String haBaseDir = removeOptionWithValue(args, "-haBaseDir", null);
		String url = removeOptionWithValue(args, "-url", null);
		
		try {
			Class.forName("com.shesse.jdbcproxy.Driver");
		} catch (ClassNotFoundException x) {
			log.error("ClassNotFoundException", x);
			throw new SQLException("ClassNotFoundException", x);
		}
	
		if (url == null) {
			if (server == null && haBaseDir == null) {
				server = "localhost";
			}
	
			if (database == null) {
				throw new SQLException("either -database dbname or -url jdbc-url is needed");
			}
	
			if (server != null) {
				if (server.contains(",")) {
					url = "jdbc:h2ha:tcp://" + server + "/" + database;
				} else {
					
					url = "jdbc:h2:tcp://" + server + "/" + database;
				}
	
			} else if (haBaseDir != null) {
				url = "jdbc:h2:file:" + new File(haBaseDir).getAbsolutePath() + "/" + database;
				if (!new File(haBaseDir).exists()) {
					System.err.println("HA base dir " + haBaseDir + " does not exist");
					System.exit(1);
				}
	
				staticBaseLock = acquireHaBaseLock(haBaseDir);
				if (staticBaseLock == null) {
					System.err.println("could not get lock for " + haBaseDir +
						" - some other process is probably using it");
					System.exit(1);
				}
	
			} else {
				url = "jdbc:h2:tcp://localhost/" + database;
			}
		}
	
		args.add(0, "-url");
		args.add(1, url);
	}


	/**
	 * Acquires a lock for accessing haBaseDir. Returns a handle for this lock
	 * which can be used to release the lock. Returns null if it was not
	 * possible to acquire the lock.
	 */
	private static LockHandle acquireHaBaseLock(String haBaseDir)
	{
		FilePath basePath = FilePath.get(haBaseDir + "/h2ha.lock");
	
		FileChannel channel;
		try {
			channel = basePath.open("rw");
		} catch (IOException x) {
			log.error("could not create HA lock file: " + x);
			return null;
		}
	
		try {
			FileLock lock = channel.tryLock();
			if (lock == null) {
				log.error("could not acquire HA lock: it is locked by another process");
				return null;
			} else {
				return new LockHandle(channel, lock);
			}
	
		} catch (IOException x) {
			log.error("could not acquire HA lock: " + x);
			return null;
		}
	}


	/**
	 * 
	 */
	private static FileSystemHa findFileSystem()
	{
		FilePath fp = FilePath.get("ha:///");
		if (fp instanceof FilePathHa) {
			return ((FilePathHa) fp).getFileSystem();
		} else {
			throw new IllegalStateException("did not get a FilePathHa for url ha:///");
		}
	
	}


	/**
	 * 
	 */
	private static String getJarname()
	{
		String url = String.valueOf(H2HaServer.class.getResource("H2HaServer.class"));
		int bang = url.indexOf('!');
		if (bang >= 0) {
			url = url.substring(0, bang);
		}
	
		if (url.startsWith("jar:")) {
			url = url.substring(4);
		}
	
		if (url.startsWith("file:")) {
			url = url.substring(5);
		}
	
		int pdel = url.lastIndexOf('/');
		if (pdel >= 0) {
			url = url.substring(pdel + 1);
		}
		return url;
	}

	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////
	/**
	 * 
	 */
	private static class LockHandle
	{
		private FileLock lock;
		private FileChannel channel;

		public LockHandle(FileChannel channel, FileLock lock)
		{
			this.channel = channel;
			this.lock = lock;
		}

		public void release()
		{
			try {
				lock.release();
			} catch (IOException x) {
			}

			try {
				channel.close();
			} catch (IOException x) {
			}
		}
	}
	
}
