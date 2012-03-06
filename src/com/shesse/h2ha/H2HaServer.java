/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.h2.store.fs.FileSystem;
import org.h2.tools.RunScript;
import org.h2.tools.Server;
import org.h2.tools.Shell;
import org.h2.tools.SimpleResultSet;

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
	private String[] args;

	/** */
	private List<String> serverArgs = new ArrayList<String>();

	/** */
	private FileSystemHa fileSystem;

	/** */
	private ReplicationServer server;

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
	private ReplicationServerInstance[] servers = new ReplicationServerInstance[0];

	/** */
	private BlockingQueue<Runnable> controlQueue = new LinkedBlockingQueue<Runnable>();

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
		CANNOT_CONNECT, // parameter is: indication if local data is valid for a DB
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
	public H2HaServer(String[] args)
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

		for (String key: hafsm.stringPropertyNames()) {
			String[] trans = hafsm.getProperty(key, "").split("\\s+");
			if (trans.length != 2) {
				throw new IllegalStateException("invalid FSM transition for "+key);
			}

			try {
				getAction(trans[0]);
			} catch (NoSuchMethodException x) {
				throw new IllegalStateException("unknown action in FSM transition for "+key);
			}


			try {
				FailoverState.valueOf(trans[1]);
			} catch (IllegalArgumentException x) {
				throw new IllegalStateException("unknown target state in FSM transition for "+key);
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
	public static void main(String[] args) 
	throws InterruptedException
	{
		try {
			String command = "";
			if (args.length > 0) {
				command = args[0];
				args = Arrays.copyOfRange(args, 1, args.length);
			}

			if ("server".equals(command)) {
				startServer(args);

			} else if ("console".equals(command)) {
				startShell(args);

			} else if ("script".equals(command)) {
				startScript(args);

			} else if ("create".equals(command)) {
				createDatabase(args);

			} else {
				System.err.println("usage: java -jar "+getJarname()+" <command> [option ...]");
				System.err.println("with command:");
				System.err.println("    server");
				System.err.println("        Run as H2HA database server");
				System.err.println("    console");
				System.err.println("        Provide a text console for a H2HA database");
				System.err.println("    script");
				System.err.println("        Submit a SQL script to the database");
				System.err.println("    create");
				System.err.println("        Create a new database");

				System.exit(1);

			}

		} catch (SQLException x) {
			System.err.println("SQL error: "+x.getMessage());
			System.exit(1);

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
	 * @param args
	 * @throws InterruptedException 
	 */
	private static void startServer(String[] args)
	throws InterruptedException
	{
		new H2HaServer(args).runHaServer();
	}

	/**
	 * 
	 */
	private static void showConsoleUsage()
	{
		System.err.println("usage: java -jar "+getJarname()+" console [option ...]");
		System.err.println("with option:");
		System.err.println("    -help or -?");
		System.err.println("        Print the list of options");
		System.err.println("    -server hostname[:port][hostname[:port]]");
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
		System.err.println("        Expliciat spec of database URL (jdbc:...)");
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
	private static void startShell(String[] args)
	throws SQLException
	{
		try {
			args = createUrl(args);
		} catch (SQLException x) {
			System.err.println(x.getMessage()+"\n");
			showConsoleUsage();
		}

		new Shell(){
			@Override
			protected void showUsage()
			{
				showConsoleUsage();
			}

		}.runTool(args);
	}

	/**
	 *
	 */
	private static void showScriptUsage()
	{
		System.err.println("usage: java -jar "+getJarname()+" script [option ...]");
		System.err.println("with option:");
		System.err.println("    -help or -?");
		System.err.println("        Print the list of options");
		System.err.println("    -server hostname[:port][hostname[:port]]");
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
		System.err.println("        Expliciat spec of database URL (jdbc:...)");
		System.err.println("    -driver <class>");
		System.err.println("        The JDBC driver class to use (not required in most cases)");
		System.err.println("");

		System.exit(1);

	}
	
	/**
	 * @param args2
	 * @throws SQLException 
	 */
	private static void startScript(String[] args)
	throws SQLException
	{
		try {
			args = createUrl(args);
		} catch (SQLException x) {
			System.err.println(x.getMessage()+"\n");
			showScriptUsage();
		}

		new RunScript(){
			@Override
			protected void showUsage()
			{
				showScriptUsage();
			}

		}.runTool(args);
	}


	/**
	 * @param args2
	 * @throws SQLException 
	 */
	private static void createDatabase(String[] args)
	throws SQLException
	{
		String script = null;
		String user = null;
		String password = null;
		String haBaseDir = null;
		String database = null;

		List<String> creArgs = new ArrayList<String>();

		for (int i = 0; i < args.length-1; i++) {
			if (args[i].equals("-script")) {
				creArgs.add(args[i]);
				script = args[++i];
				creArgs.add(args[i]);

			} else if (args[i].equals("-user")) {
				creArgs.add(args[i]);
				user = args[++i];
				creArgs.add(args[i]);

			} else if (args[i].equals("-password")) {
				creArgs.add(args[i]);
				password = args[++i];
				creArgs.add(args[i]);

			} else if (args[i].equals("-database")) {
				creArgs.add(args[i]);
				database = args[++i];
				creArgs.add(args[i]);

			} else if (args[i].equals("-haBaseDir")) {
				creArgs.add(args[i]);
				haBaseDir = args[++i];
				creArgs.add(args[i]);

			} else if (args[i].equals("-url")) {
				++i;

			} else if (args[i].equals("-server")) {
				++i;

			} else {
				creArgs.add(args[i]);
				if (args[i].startsWith("-") && !args[i+1].startsWith("-")) {
					creArgs.add(args[++i]);
				}
			}
		}

		args = creArgs.toArray(new String[creArgs.size()]);

		if (script == null) {
			System.err.println("mandatory parameter -script is missing");
			showCreateUsage();
		}

		if (user == null) {
			System.err.println("mandatory parameter -user is missing");
			showCreateUsage();
		}

		if (password == null) {
			System.err.println("mandatory parameter -password is missing");
			showCreateUsage();
		}

		if (haBaseDir == null) {
			System.err.println("mandatory parameter -haBaseDir is missing");
			showCreateUsage();
		}

		if (database == null) {
			System.err.println("mandatory parameter -database is missing");
			showCreateUsage();
		}

		if (!new File(haBaseDir).exists()) {
			System.err.println("HA base dir "+haBaseDir+" does not exist");
			System.exit(1);
		}

		try {
			args = createUrl(args);
		} catch (SQLException x) {
			System.err.println(x.getMessage()+"\n");
			showCreateUsage();
		}
		
		new RunScript(){
			@Override
			protected void showUsage()
			{
				showCreateUsage();
			}

		}.runTool(args);
	}

	/**
	 * 
	 */
	private static void showCreateUsage()
	{
		System.err.println("usage: java -jar "+getJarname()+" create [option ...]");
		System.err.println("with option:");
		System.err.println("    -help or -?");
		System.err.println("        Print the list of options");
		System.err.println("    -server hostname[:port][hostname[:port]]");
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
		System.err.println("        Expliciat spec of database URL (jdbc:...)");
		System.err.println("    -driver <class>");
		System.err.println("        The JDBC driver class to use (not required in most cases)");
		System.err.println("");

		System.exit(1);
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
			url = url.substring(pdel+1);
		}
		return url;
	}


	private static String[] createUrl(String[] args)
	throws SQLException
	{
		String server = null;
		String database = null;
		String haBaseDir = null;
		String url = null;

		List<String> resArgs = new ArrayList<String>();

		for (int i = 0; i < args.length-1; i++) {
			if (args[i].equals("-server")) {
				// angegeben als hostname[:port][hostname[:port]]
				server = args[++i];

			} else if (args[i].equals("-database")) {
				database = args[++i];

			} else if (args[i].equals("-haBaseDir")) {
				haBaseDir = args[++i];

			} else if (args[i].equals("-url")) {
				url = args[++i];

			} else {
				resArgs.add(args[i]);
				if (args[i].startsWith("-") && !args[i+1].startsWith("-")) {
					resArgs.add(args[++i]);
				}
			}
		}   

		if (url == null) {
			if (server == null && haBaseDir == null) {
				server = "localhost";
			}
			
			if (database == null) {
				throw new SQLException("either -database dbname or -url jdbc-url is needed");
			}

			if (server != null) {
				url = "jdbc:h2:tcp://"+server+"/"+database;

			} else if (haBaseDir != null) {
				url = "jdbc:h2:file:"+new File(haBaseDir).getAbsolutePath()+"/"+database;
				if (!new File(haBaseDir).exists()) {
					System.err.println("HA base dir "+haBaseDir+" does not exist");
					System.exit(1);
				}

				staticBaseLock = acquireHaBaseLock(haBaseDir);
				if (staticBaseLock == null) {
					System.err.println("could not get lock for "+haBaseDir+" - some other process is probably using it");
					System.exit(1);
				}

			} else {
				url = "jdbc:h2:tcp://localhost/"+database;
			}
		}

		resArgs.add(0, "-url");
		resArgs.add(1, url);

		return resArgs.toArray(new String[resArgs.size()]);
	}


	/**
	 * @throws InterruptedException 
	 * 
	 */
	private void runHaServer() 
	throws InterruptedException
	{
		serverArgs.add("-tcpAllowOthers");
		serverArgs.add("-baseDir");
		serverArgs.add("ha://");
		serverArgs.add("-ifExists");

		for (int i = 0; i < args.length-1; i++) {
			if (args[i].equals("-haPeerHost")) {
				peerHost = args[++i];

			} else if (args[i].equals("-haBaseDir")) {
				haBaseDir = args[++i];

			} else if (args[i].equals("-masterPriority")) {
				try {
					masterPriority = Integer.parseInt(args[++i]);
				} catch (NumberFormatException x) {
					log.error("inhalid masterPriority: "+x);
				}

			} else if (args[i].equals("-help")) {
				showServerUsage();
				System.exit(1);

			} else if (args[i].equals("-?")) {
				showServerUsage();
				System.exit(1);

			} else {
				serverArgs.add(args[i]);
				if (args[i].startsWith("-") && !args[i+1].startsWith("-")) {
					serverArgs.add(args[++i]);
				}
			}
		}        

		if (haBaseDir == null) {
			System.err.println("mandatory flag -haBaseDir is missing");
			showServerUsage();
			System.exit(1);
		}

		if (!new File(haBaseDir).exists()) {
			System.err.println("HA base dir "+haBaseDir+" does not exist");
			showServerUsage();
			System.exit(1);
		}

		LockHandle baseLock = acquireHaBaseLock(haBaseDir);
		if (baseLock == null) {
			System.err.println("could not get lock for "+haBaseDir+" - some other process is probably using it");
			System.exit(1);
		}

		try {
			fileSystem = new FileSystemHa(this, args);
			server = new ReplicationServer(this, fileSystem, args);
			server.start();

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

		} finally {
			baseLock.release();
		}
	}

	/**
	 * 
	 */
	private void showServerUsage()
	{
		System.err.println("usage: java -jar "+getJarname()+" server [option ...]");
		System.err.println("with option:");
		System.err.println("    -help or -?");
		System.err.println("        Print the list of options");
		System.err.println("    -haListenPort");
		System.err.println("        TCP port to listen for HA connections (default = 8234)");
		System.err.println("    -haPeerHost");
		System.err.println("        host name of HA peer");
		System.err.println("    -haPeerPort");
		System.err.println("        peer HA port (default = 8234)");
		System.err.println("    -haBaseDir");
		System.err.println("        base directory for replicated DB files");
		System.err.println("    -masterPriority");
		System.err.println("        priority to become a master. Integer value.");
		System.err.println("        Default is 10. Higher values are higher priorities.");
		System.err.println("    -haMaxQueueSize");
		System.err.println("        Queue size for HA replication (default = 5000)");
		System.err.println("    -haMaxEnqueueWait");
		System.err.println("        max millis to wait to enqueue HA data (default = 60000)");
		System.err.println("    -haMaxWaitingMessages");
		System.err.println("        max no of messages in message quere before connection");
		System.err.println("        is considered defect. 0 = default = unlimited");
		System.err.println("    -statisticsInterval");
		System.err.println("        cycle millis for statistics logging, default = 300000");
		System.err.println("    -haCacheSize");
		System.err.println("        size of HA block cache (default = 1000)");
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
	 * Fordert eine Sperre für den Zugriff auf die haBaseDir
	 * an. Liefert ein Handle für die Sperre, über das sie
	 * wieder freigegeben werden kann oder null, wenn das Sperren
	 * nicht möglich war
	 */
	private static LockHandle acquireHaBaseLock(String haBaseDir)
	{
		File dir = new File(haBaseDir);
		File lockfile = new File(dir, "h2ha.lock");

		FileChannel channel;
		try {
			channel = new RandomAccessFile(lockfile, "rw").getChannel();
		} catch (FileNotFoundException x) {
			log.error("could not create HA lock file: "+x);
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
			log.error("could not acquire HA lock: "+x);
			return null;
		}
	}

	/**
	 * 
	 */
	public void shutdown()
	{
		shutdownRequested = true;
		enqueue(new Runnable(){
			public void run()
			{
				// no content - we simply want the controlQueue.take()
				// in the main loop to return.
			}
		});
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
	 * may be called when running as a master to ensure that
	 * all outstanding changes have been sent to all clients
	 */
	public static void pushToAllReplicators()
	{
		FileSystem fs = FileSystem.getInstance("ha://");
		if (fs instanceof FileSystemHa) {
			((FileSystemHa)fs).flushAll();
		} else {
			throw new IllegalStateException("did not get a FileSystemHa for url ha://");
		}
	}

	/**
	 * may be called when running as a master to ensure that
	 * all outstanding changes have been received by all clients
	 */
	public static void syncWithAllReplicators()
	{
		log.debug("syncWithAllReplicators has been called");
		FileSystem fs = FileSystem.getInstance("ha://");
		if (fs instanceof FileSystemHa) {
			((FileSystemHa)fs).syncAll();
		} else {
			throw new IllegalStateException("did not get a FileSystemHa for url ha://");
		}
	}

	/**
	 * must be called on the actual master system
	 * @throws SQLException 
	 */
	public static void transferMasterRole()
	throws SQLException
	{
		FileSystem fs = FileSystem.getInstance("ha://");
		if (fs instanceof FileSystemHa) {
			((FileSystemHa)fs).getHaServer().transferMasterRoleImpl();
		} else {
			throw new IllegalStateException("did not get a FileSystemHa for url ha://");
		}
	}

	/**
	 * @throws SQLException 
	 * 
	 */
	private void transferMasterRoleImpl()
	throws SQLException
	{
		if (failoverState != FailoverState.MASTER) {
			throw new SQLException("master role can only be transferred from an active master");
		}

		if (client == null) {
			throw new SQLException("master role can only be transferred from within a failover configuration");
		}

		applyEvent(Event.TRANSFER_MASTER, null, null);
	}

	/**
	 * 
	 * @param conn
	 * @param size
	 * @return
	 * @throws SQLException
	 */
	public static ResultSet getServerInfo(Connection conn)
	throws SQLException
	{
		FileSystem fs = FileSystem.getInstance("ha://");
		H2HaServer server;
		if (fs instanceof FileSystemHa) {
			server = ((FileSystemHa)fs).getHaServer();
		} else {
			throw new IllegalStateException("did not get a FileSystemHa for url ha://");
		}

		return server.getServerInfoImpl(conn);
	}
	/**
	 * 
	 * @param conn
	 * @param size
	 * @return
	 * @throws SQLException
	 */
	public ResultSet getServerInfoImpl(Connection conn)
	throws SQLException
	{
		SimpleResultSet rs = new SimpleResultSet();
		rs.addColumn("SERVER_NAME", Types.VARCHAR, 100, 0);
		rs.addColumn("SERVER_PORT", Types.INTEGER, 5, 0);
		rs.addColumn("LOCAL_STATUS", Types.VARCHAR, 20, 0);
		rs.addColumn("PEER_STATUS", Types.VARCHAR, 20, 0);
		rs.addColumn("REPL_BYTES_RAW", Types.INTEGER, 20, 0);
		rs.addColumn("REPL_BYTES_CROPPED", Types.INTEGER, 20, 0);
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
				(client == null ? null : client.getPeerState().toString()),//
				fileSystem.getReplicationRawBytes(),//
				fileSystem.getReplicationCroppedBytes(),//
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
	 * 
	 * @param conn
	 * @param size
	 * @return
	 * @throws SQLException
	 */
	public static ResultSet getReplicationInfo(Connection conn)
	throws SQLException
	{
		FileSystem fs = FileSystem.getInstance("ha://");
		H2HaServer server;
		if (fs instanceof FileSystemHa) {
			server = ((FileSystemHa)fs).getHaServer();
		} else {
			throw new IllegalStateException("did not get a FileSystemHa for url ha://");
		}

		return server.getReplicationInfoImpl(conn);
	}
	/**
	 * 
	 * @param conn
	 * @param size
	 * @return
	 * @throws SQLException
	 */
	public ResultSet getReplicationInfoImpl(Connection conn)
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

		for (ReplicationServerInstance server: servers) {
			rs.addRow(//
				server.getInstanceName(),//
				server.getStartTime(),//
				server.getTotalBytesTransmitted(),//
				(int)server.getLastSendDelay()//
			);
		}

		return rs;
	}


	/**
	 * computes deterministic role assignment depending on
	 * current local role, local and remote master priority and
	 * local and remote UUID.
	 * @return true if the local system is the configured master system
	 */
	public boolean weAreConfiguredMaster(int otherMasterPriority, String otherUuid)
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
	public synchronized void registerServer(ReplicationServerInstance server)
	{
		for (ReplicationProtocolInstance r : servers) {
			if (r == server) {
				return;
			}
		}

		ReplicationServerInstance[] newServers =
			new ReplicationServerInstance[servers.length + 1];
		System.arraycopy(servers, 0, newServers, 0, servers.length);
		newServers[servers.length] = server;
		servers = newServers;
	}


	/**
	 * @param server
	 */
	public void deregisterServer(ReplicationProtocolInstance server)
	{
		for (int i = 0; i < servers.length; i++) {
			if (servers[i] == server) {
				ReplicationServerInstance[] newServers =
					new ReplicationServerInstance[servers.length - 1];
				System.arraycopy(servers, 0, newServers, 0, i);
				System.arraycopy(servers, i + 1, newServers, i, newServers.length - i);
				servers = newServers;
				return;
			}
		}
	}


	/**
	 * 
	 */
	public void applyEvent(final Event event, final Object parameter, final Object optParam)
	{
		enqueue(new Runnable(){
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
		log.debug("applyEventImpl "+event+", param="+parameter);
		String eventKey = String.valueOf(event);

		if (parameter != null) {
			eventKey += "."+parameter;
		}

		String eventKeyOpt = eventKey + "." +optParam;

		String key = failoverState+"."+eventKey;
		String keyOpt = failoverState+"."+eventKeyOpt;


		String transition = hafsm.getProperty(keyOpt);
		if (transition == null) {
			transition = hafsm.getProperty(key);

		} else {
			key = keyOpt;
			eventKey = eventKeyOpt;
		}

		if (transition == null) {
			throw new IllegalStateException("cannot find FSM entry for '"+key+"'");
		}

		log.debug("transition lookup: "+key+" -> "+transition);

		String[] transitionParts = transition.split("\\s+");
		if (transitionParts.length != 2) {
			throw new IllegalStateException("not a valid transition for '"+key+"': "+transition);
		}

		String actionName = transitionParts[0];
		String newStateName = transitionParts[1];

		FailoverState newState = FailoverState.valueOf(newStateName);
		FailoverState oldState = failoverState;
		if (newState != oldState) {

			log.info("changing state from "+oldState+" to "+newState+" (event was "+eventKey+")");
			failoverState = newState;

			try {
				Method action = getAction(actionName);
				action.invoke(this, oldState, event, newState, parameter);

			} catch (IllegalArgumentException x) {
				throw new IllegalStateException("illegal argument for FSM action '"+actionName+"'", x);

			} catch (IllegalAccessException x) {
				throw new IllegalStateException("illegal access for FSM action '"+actionName+"'", x);

			} catch (InvocationTargetException x) {
				throw new IllegalStateException("caught exception within FSM action '"+actionName+"'", x.getCause());

			} catch (SecurityException x) {
				throw new IllegalStateException("security exception for FSM action '"+actionName+"'", x);

			} catch (NoSuchMethodException x) {
				throw new IllegalStateException("could not find action '"+actionName+"' for FSM transition '"+key+"'");

			}

			if (client != null) {
				try {
					client.sendStatus();
				} catch (IOException e) {
				}
			}

			ReplicationServerInstance[] sdup = servers;
			for (ReplicationServerInstance server: sdup) {
				server.enqueue(new ReplicationMessage() {
					private static final long serialVersionUID = 1L;

					@Override
					protected void process(ReplicationProtocolInstance instance)
					throws Exception
					{
						if (instance instanceof ServerSideProtocolInstance) {
							((ServerSideProtocolInstance)instance).sendStatus();
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
				}
				);
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
			return getClass().getMethod(actionName, FailoverState.class, Event.class, FailoverState.class, Object.class);

		} catch (SecurityException x) {
			throw new IllegalStateException("security exception for FSM action '"+actionName+"'", x);

		}
	}

	/**
	 * 
	 */
	public void noAction(FailoverState oldState, Event event, FailoverState newState, Object parameter)
	{
	}

	/**
	 * 
	 */
	public void fatalError(FailoverState oldState, Event event, FailoverState newState, Object parameter)
	{
		log.fatal("invalid state / event combination: "+oldState+" / "+event);
		System.exit(1);
	}

	/**
	 * 
	 */
	public void logUnexpected(FailoverState oldState, Event event, FailoverState newState, Object parameter)
	{
		if (parameter == null) {
			log.warn("unexpected state / event combination: "+oldState+" / "+event);
		} else {
			log.warn("unexpected state / event combination: "+oldState+" / "+event+"("+parameter+")");
		}
	}

	/**
	 * 
	 */
	public void startHaClient(FailoverState oldState, Event event, FailoverState newState, Object parameter)
	{
		client = new ReplicationClientInstance(this, fileSystem, args);
		new Thread(client, "ReplicationClient").start(); 
	}

	/**
	 * 
	 */
	public void startDbServer(FailoverState oldState, Event event, FailoverState newState, Object parameter)
	{
		try {
			log.info("creating H2 TCP server with args: "+serverArgs);
			tcpDatabaseServer = Server.createTcpServer(serverArgs.toArray(new String[serverArgs.size()])).start();
			log.info("DB server is ready to accept connections");
			applyEvent(Event.MASTER_STARTED, null, null);

		} catch (SQLException x) {
			log.error("SQLException when starting database server", x);
			System.exit(1);
		}
	}

	/**
	 * 
	 */
	public void stopDbServer(FailoverState oldState, Event event, FailoverState newState, Object parameter)
	{
		log.info("shutting down DB server");
		tcpDatabaseServer.stop();
		applyEvent(Event.MASTER_STOPPED, null, null);
	}

	/**
	 * 
	 */
	public void sendListFilesRequest(FailoverState oldState, Event event, FailoverState newState, Object parameter)
	{
		client.sendListFilesRequest();
	}


	/**
	 * 
	 */
	public void sendStopReplicationRequest(FailoverState oldState, Event event, FailoverState newState, Object parameter)
	{
		client.sendStopReplicationRequest();
	}


	/**
	 * 
	 */
	public void issueConnEvent(FailoverState oldState, Event event, FailoverState newState, Object parameter)
	{
		client.issueConnEvent();
	}


	/**
	 * 
	 */
	public void issuePeerEvent(FailoverState oldState, Event event, FailoverState newState, Object parameter)
	{
		client.issuePeerEvent();
	}


	/**
	 * 
	 */
	public boolean isActive()
	{
		return failoverState == FailoverState.MASTER_STANDALONE ||
		failoverState == FailoverState.MASTER || failoverState == FailoverState.SLAVE;
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
