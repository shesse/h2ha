/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

import org.h2.jdbcx.JdbcDataSource;

import com.shesse.jdbcproxy.ServerMonitor.Status;

/**
 * In instance of this class acts as a connection factory for
 * a HA pair of H2 database servers. It maintains DataSources
 * for both database servers and tries to connect to both of 
 * them.
 * <p>
 * This class presumes that only one of the pair of servers
 * will accept database connections at any time. 
 * 
 * 
 * @author sth
 */
public class AlternatingConnectionFactory
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	private static Logger log = Logger.getLogger(AlternatingConnectionFactory.class.getName());

	/** */
	private String haUrl;

	/** */
	private Properties info;
	
	/** */
	private JdbcDataSource[] dataSources;
	
	/** */
	private ServerMonitor[] monitors = null;

	/** */
	private volatile ServerMonitor activeMonitor;
	
	/** */
	private Map<RegisteredHaConnection, Void> activeConnections =
		new WeakHashMap<RegisteredHaConnection, Void>();

	/** */
	private static ExecutorService threadPool = null;
	
	/** timer marked as daemon thread */
	private static Timer timer = new Timer(true);

	/** */
	private static Map<String, AlternatingConnectionFactory> factories =
		new HashMap<String, AlternatingConnectionFactory>();
	
	/** */
	private static final long maxWaitForAvailable = 30000;


	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @param info 
	 * @param haUrl 
     */
	private AlternatingConnectionFactory(String haUrl, Properties info, JdbcDataSource... dataSources)
	{
		this.haUrl = haUrl;
		this.info = info;
		this.dataSources = dataSources;
		
		log.fine("conn factory url="+haUrl);
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * Return a connection factory for the given URL, username
	 * and password. 
	 * @throws SQLException
	 * 
	 */
	public static synchronized AlternatingConnectionFactory getFactory(String url, Properties info)
		throws SQLException
	{
		AlternatingConnectionFactory factory = factories.get(getKey(url, info));
		if (factory == null) {
			factory = AlternatingConnectionFactory.create(url, info);
			registerFactory(factory);
		}
		return factory;
	}

	/**
	 * Returns a new database connection connected to the currently active
	 * H2 instance.
	 *  
	 * @throws SQLException 
	 * 
	 */
	public HaConnection getConnection()
		throws SQLException
	{
		// There is a possible race condition where a
		// call to deregister() from another thread between
		// getActiveMonitor() and the the constructor call
		// my stop the monitor we are using. To prevent this we
		// temporarily register a dummy connection
		RegisteredHaConnection dc = new DummyRegisteredConnection();
		register(dc);
		
		try {
			ServerMonitor m = getActiveMonitor();
			return new HaConnection(this, m, m.createH2XaConnection());
			
		} finally {
			deregister(dc);
		}
	}

	/**
	 * Returns a new XA Connection connected to the currently active
	 * H2 instance.
	 *
	 * @throws SQLException
	 */
	public HaXaConnection getXaConnection()
		throws SQLException
	{
		// There is a possible race condition where a
		// call to deregister() from another thread between
		// getActiveMonitor() and the the constructor call
		// my stop the monitor we are using. To prevent this we
		// temporarily register a dummy connection
		RegisteredHaConnection dc = new DummyRegisteredConnection();
		register(dc);

		try {
			ServerMonitor m = getActiveMonitor();
			return new HaXaConnection(this, m, m.createH2XaConnection());

		} finally {
			deregister(dc);
		}
	}

	/**
	 * @param haConnection
	 */
	public synchronized void register(RegisteredHaConnection haConnection)
	{
		log.finer("registering a connection: "+haConnection.getClass().getName());
		activeConnections.put(haConnection, null);
	}

	/**
	 * @param haConnection
	 * @param monitoredBy 
	 */
	public synchronized void deregister(RegisteredHaConnection haConnection)
	{
		log.finer("deregistering a connection: "+haConnection.getClass().getName());
		activeConnections.remove(haConnection);
		if (activeConnections.isEmpty()) {
			log.fine("all connections are closed for "+haUrl+" - releasing monitors and factories");
			// the last connection using this factory has
			// ended. We stop the monitors and deregister the factory
			stopMonitors();
			deregisterFactory(this);
		}
	}

	/**
	 * @param serverMonitor
	 */
	public void submit(Runnable job)
	{
		getThreadPool().submit(job);
	}

	/**
	 * @param timerTask
	 * @param refreshcycle
	 */
	public void schedule(TimerTask task, long delay)
	{
		getTimer().schedule(task, delay);
	}

	/**
	 * @param serverMonitor
	 * @param status
	 */
	public void updateOfMonitorStatus(ServerMonitor monitor, ServerMonitor.Status status)
	{
		log.fine("update monitor status: "+monitor+": "+status);
		ServerMonitor toBeCleaned = null;
		synchronized (this) {
			if (status == Status.AVAILABLE || activeMonitor == null) {
				if (activeMonitor != monitor) {
					toBeCleaned = activeMonitor;
					activeMonitor = monitor;
				}
			}
			notifyAll();
		}
		
		if (toBeCleaned != null) {
			cleanupForMonitor(toBeCleaned);
		}
	}

	/**
	 * @param toBeCleaned
	 */
	private void cleanupForMonitor(ServerMonitor toBeCleaned)
	{
		log.fine("cleanup for "+toBeCleaned);
		// collect the connections and clean them up outside the synchronized
		// section because cleanup() may be complex and should not
		// be carried out with a lock being held.
		List<RegisteredHaConnection> cleanList = new ArrayList<RegisteredHaConnection>();
		synchronized (this) {
			cleanList = new ArrayList<RegisteredHaConnection>(activeConnections.keySet());
		}
		
		for (RegisteredHaConnection conn: cleanList) {
			if (conn != null && conn.getMonitoredBy() == toBeCleaned) {
				log.fine("cleaning "+conn.getClass().getName()+" to "+toBeCleaned);
				conn.cleanup();
			}
		}
	}

	/**
	 * @throws SQLException
	 * 
	 */
	private static AlternatingConnectionFactory create(String haUrl, Properties info)
		throws SQLException
	{
		URI[] haUris;
		try {
			haUris = splitUris(haUrl);
		} catch (URISyntaxException x) {
			throw new SQLException("cannot parse URL " + haUrl, x);
		}
		JdbcDataSource[] dataSources = new JdbcDataSource[haUris.length];

		for (int i = 0; i < dataSources.length; i++) {
			String user = info.getProperty("user");
			String password = info.getProperty("password");
			String description = info.getProperty("description");
			String loginTimeout = info.getProperty("loginTimeout");

			JdbcDataSource ds = new JdbcDataSource();
			ds.setURL(haUris[i].toString());
			ds.setUser(user);
			ds.setPassword(password);
			if (description != null) {
				ds.setDescription(description);
			}
			if (loginTimeout != null) {
				try {
					ds.setLoginTimeout(Integer.parseInt(loginTimeout));
				} catch (NumberFormatException x) {
				}
			}

			dataSources[i] = ds;
		}

		return new AlternatingConnectionFactory(haUrl, info, dataSources);
	}

	/**
	 * @param value
	 * @return
	 * @throws URISyntaxException
	 * @throws Exception
	 */
	private static URI[] splitUris(String value)
		throws URISyntaxException
	{
		URI uri = new URI(value);

		if (uri.getScheme() == null) {
			return new URI[] { uri };

		} else if (uri.isOpaque()) {
			String scheme = uri.getScheme();
			if ("h2ha".equals(scheme)) {
				scheme = "h2";
			}

			URI[] subs = splitUris(uri.getSchemeSpecificPart());
			for (int i = 0; i < subs.length; i++) {
				subs[i] = new URI(scheme, subs[i].toString(), uri.getFragment());
			}
			return subs;

		} else {
			String auth = uri.getAuthority();
			if (auth == null) {
				return new URI[] { uri };
			}

			String[] authParts = auth.split(",");
			URI[] uris = new URI[authParts.length];
			for (int i = 0; i < uris.length; i++) {
				uris[i] =
					new URI(uri.getScheme(), authParts[i], uri.getPath(), uri.getQuery(),
						uri.getFragment());
			}
			return uris;
		}
	}
	
	/**
	 * @param url
	 * @param info
	 * @return
	 */
	private static String getKey(String url, Properties info)
	{
		return url + "\n" + info.getProperty("user") + "\n" + info.getProperty("password");
	}

	/**
	 * @return
	 */
	private String getKey()
	{
		return getKey(haUrl, info);
	}

	/**
	 * @param alternatingConnectionFactory
	 */
	private static synchronized void registerFactory(AlternatingConnectionFactory factory)
	{
		log.fine("registering factory "+factory);
		factories.put(factory.getKey(), factory);
	}

	/**
	 * @param alternatingConnectionFactory
	 */
	private static synchronized void deregisterFactory(AlternatingConnectionFactory factory)
	{
		log.fine("deregistering factory "+factory);
		factories.remove(factory.getKey());
		if (factories.isEmpty()) {
			log.fine("all factories are gone - cleaning up the threads");
			cleanupThreads();
		}
	}

	/**
	 * 
	 */
	private static synchronized ExecutorService getThreadPool()
	{
		// create thread pool with threads that are marked as daemon threads 
		if (threadPool == null) {
			log.fine("creating thread pool");
			threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
				int nextThreadNo = 1;
				@Override
				public Thread newThread(Runnable r)
				{
					int n;
					synchronized (this) {
						n = nextThreadNo++;
					}

					Thread t = new Thread(r, "H2Ha-pool-"+n);
					t.setDaemon(true);
					return t;
				}
			});
		}
		
		return threadPool;
	}
	
	/**
	 * 
	 */
	private static synchronized Timer getTimer()
	{
		// timer marked as daemon thread
		if (timer == null) {
			log.fine("creating timer");
			timer = new Timer(true);
		}
		return timer;
	}
	
	/**
	 * 
	 */
	private static synchronized void cleanupThreads()
	{
		if (timer != null) {
			log.fine("releasing timer");
			timer.cancel();
			timer = null;
		}
		
		if (threadPool != null) {
			log.fine("releasing thread pool");
			threadPool.shutdown();
			threadPool = null;
		}
	}
	/**
	 * @return
	 */
	private synchronized ServerMonitor getActiveMonitor()
		throws SQLException
	{
		startMonitors();

		if (activeMonitor != null && activeMonitor.getStatus() != Status.AVAILABLE) {
			log.fine("active monitor is not available - scheduling immediate connect attempts");
			for (ServerMonitor cmon: monitors) {
				cmon.checkAgain();
			}
		}
		
		synchronized (this) {
			// we will wait at least until one of the monitors has
			// a result ... AVAILABLE or UNAVAILABLE
			log.fine("waiting for monitors");
			while (activeMonitor == null) {
				try {
					log.fine("waiting because activeMonitor == null");
					wait();
				} catch (InterruptedException x) {
					throw new SQLException("unexpected interrupt", x);
				}
			}
			
			// we will wait at most maxWaitForAvailable for a
			// monitor with status 'AVAILABLE'. 
			long waitUntil = System.currentTimeMillis() + maxWaitForAvailable;
			while (activeMonitor.getStatus() != ServerMonitor.Status.AVAILABLE) {
				long delay = waitUntil - System.currentTimeMillis();
				if (delay <= 0) {
					log.fine("tineout waiting for an AVAILABLE monitor");
					break;
				}
				
				try {
					log.fine("waiting because activeMonitor is not AVAILABLE");
					wait(delay);
				} catch (InterruptedException x) {
					throw new SQLException("unexpected interrupt", x);
				}
			}
			log.fine("found a monitor - "+activeMonitor+", status = "+activeMonitor.getStatus());

		}

		return activeMonitor;
	}
	
	/**
	 * 
	 */
	private synchronized void startMonitors()
	{
		if (monitors == null) {
			log.fine("starting monitors");
			monitors = new ServerMonitor[dataSources.length];
			for (int i = 0; i < monitors.length; i++) {
				monitors[i] = new ServerMonitor(this, dataSources[i]);
			}
		}

	}

	/**
	 * 
	 */
	private synchronized void stopMonitors()
	{
		if (monitors != null) {
			log.fine("stopping monitors");
			for (ServerMonitor monitor: monitors) {
				if (monitor != null) {
					monitor.stop();
				}
			}
			monitors = null;
			activeMonitor = null;
		}

	}

	/**
	 * 
	 */
	public String toString()
	{
		return haUrl;
	}

	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////
	/**
	 * 
	 */
	public interface RegisteredHaConnection
	{
		public ServerMonitor getMonitoredBy();

		public void cleanup();
	}

	/**
	 */
	private static class DummyRegisteredConnection
	implements RegisteredHaConnection
	{
		@Override
		public ServerMonitor getMonitoredBy()
		{
			return null;
		}

		@Override
		public void cleanup()
		{
		}
		
	}

}
