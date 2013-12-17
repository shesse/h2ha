/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;

import org.h2.jdbcx.JdbcDataSource;
import org.h2.jdbcx.JdbcXAConnection;

/**
 * 
 * @author sth
 */
public class AlternatingConnectionFactory
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	// private static Logger log =
	// Logger.getLogger(AlternatingConnectionFactory.class.getName());

	/** */
	private JdbcDataSource[] dataSources;

	/**
	 * -2: unknown, -1: currently trying,  >= 0: this is the index
	 */
	private volatile int currentDataSourceIndex = -2;

	/** */
	private static ExecutorService threadPool = Executors.newCachedThreadPool();

	/** */
	private static Map<String, AlternatingConnectionFactory> factories =
		new HashMap<String, AlternatingConnectionFactory>();


	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
     */
	public AlternatingConnectionFactory(JdbcDataSource... dataSources)
	{
		this.dataSources = dataSources;
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * @throws SQLException
	 * 
	 */
	public static AlternatingConnectionFactory create(String haUrl, Properties info)
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

		return new AlternatingConnectionFactory(dataSources);
	}

	/**
	 * @throws SQLException
	 * 
	 */
	public static AlternatingConnectionFactory getFactory(String url, Properties info)
		throws SQLException
	{
		String key = url + "\n" + info.getProperty("user") + "\n" + info.getProperty("password");
		synchronized (factories) {
			AlternatingConnectionFactory factory = factories.get(key);
			if (factory == null) {
				factory = AlternatingConnectionFactory.create(url, info);
				factories.put(key, factory);
			}
			return factory;
		}
	}


	/**
	 * @param value
	 * @return
	 * @throws URISyntaxException
	 * @throws Exception
	 */
	public static URI[] splitUris(String value)
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
	 * @throws SQLException 
	 * 
	 */
	public HaConnection getConnection()
		throws SQLException
	{
		return new HaConnection(getH2XaConnection().getConnection());
	
	}

	public HaXaConnection getXaConnection()
		throws SQLException
	{
		return new HaXaConnection(getH2XaConnection());
	}

	/**
	 * @return
	 */
	private JdbcXAConnection getH2XaConnection()
		throws SQLException
	{
		int dataSourceIndex;

		synchronized (this) {
			while (currentDataSourceIndex == -1) {
				// a connect ist currently being tried - we wait for 
				// the outcome
				try {
					wait();
				} catch (InterruptedException x) {
					throw new SQLException("wait for connection has been interrupted");
				}
			}

			if (currentDataSourceIndex == -2) {
				// no index is cuerrntly known - we need to try
				currentDataSourceIndex = -1;
			}

			dataSourceIndex = currentDataSourceIndex;
		}

		final JdbcXAConnection pc;
		if (dataSourceIndex < 0) {
			// it must be -1 - the above code will change -2 to -1
			try {
				// try to connect on both data sources in parallel
				pc = simultaneousConnectAttempt();

			} finally {
				synchronized (this) {
					// tell the world that we are finished and that
					// we were not successful
					if (currentDataSourceIndex == -1) {
						currentDataSourceIndex = -2;
					}
					notifyAll();
				}
			}

		} else {
			// we already have an index - and we try to connect on it
			try {
				pc = directConnectAttempt(dataSourceIndex);

			} catch (SQLException x) {
				// the direct connect did not work - it seems or knwon
				// currentDataSourceIndex does not work any longer
				synchronized (this) {
					// set index to unknown
					if (currentDataSourceIndex >= 0) {
						currentDataSourceIndex = -2;
					}
				}

				// another attempt - this time with undefined currentDataSourceIndex
				return getH2XaConnection();
			}
		}

		// we have a connection in pc and have remembered it's index
		// in currentDataSourceIndex to simplify future connect attempts
		
		// we add a listener to pc to get notified when an error
		// occurs. We will use this information to invalidate
		// our currentDataSourceIndex.
		pc.addConnectionEventListener(new ConnectionEventListener() {
			public void connectionErrorOccurred(ConnectionEvent event)
			{
				synchronized (AlternatingConnectionFactory.this) {
					if (currentDataSourceIndex >= 0) {
						currentDataSourceIndex = -2;
					}
				}
			}

			public void connectionClosed(ConnectionEvent event)
			{
			}
		});

		return pc;
	}
	
	/**
	 * Index is not yet known. We try connecting in parallel to both
	 * servers. The first successful wins.
	 */
	private JdbcXAConnection simultaneousConnectAttempt()
		throws SQLException
	{
		ConnectResultContainer rc = new ConnectResultContainer();
		for (int i = 0; i < dataSources.length; i++) {
			threadPool.submit(new ConnectAttemptTask(i, rc));
		}

		synchronized (rc) {
			while (rc.firstConnection == null && rc.noOfFinishedTasks < dataSources.length) {
				try {
					rc.wait();
				} catch (InterruptedException x) {
					throw new SQLException("waiting for connection has been interrupted");
				}
			}
		}

		if (rc.firstConnection == null) {
			// None of the possibilities worked. Therefore, we must throw
			// an SQLException
			SQLException cause = rc.firstException;
			if (cause == null) {
				// none of the exceptions were SQLException
				throw new SQLException("cannot connect to any of the possible database servers");

			} else {
				// we pass on the original SQLException
				throw new SQLException(cause.getMessage(), cause.getSQLState(),
					cause.getErrorCode(), cause);
			}
		} else {
			// at least one of the connections worked - we use it!
			synchronized (this) {
				if (currentDataSourceIndex < 0) {
					currentDataSourceIndex = rc.firstSuccessIndex;
				}
			}

			return rc.firstConnection;
		}
	}


	/**
     * 
     */
	private JdbcXAConnection directConnectAttempt(int dataSourceIndex)
		throws SQLException
	{
		return (JdbcXAConnection) dataSources[dataSourceIndex].getXAConnection();
	}


	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////
	/**
     * 
     */
	private static class ConnectResultContainer
	{
		int noOfFinishedTasks = 0;
		JdbcXAConnection firstConnection = null;
		int firstSuccessIndex = -1;
		SQLException firstException = null;
	}

	/**
     * 
     */
	private class ConnectAttemptTask
		implements Runnable
	{
		private int dataSourceIndex;
		private ConnectResultContainer resultContainer;

		/**
	 * 
	 */
		public ConnectAttemptTask(int dataSourceIndex, ConnectResultContainer resultContainer)
		{
			this.dataSourceIndex = dataSourceIndex;
			this.resultContainer = resultContainer;
		}

		/**
		 * {@inheritDoc}
		 * 
		 * @see java.lang.Runnable#run()
		 */
		public void run()
		{
			JdbcXAConnection pc = null;
			SQLException exception = null;

			try {
				pc = (JdbcXAConnection) dataSources[dataSourceIndex].getXAConnection();

			} catch (SQLException x) {
				exception = x;
			}

			synchronized (resultContainer) {
				resultContainer.noOfFinishedTasks++;

				if (resultContainer.firstConnection == null) {
					resultContainer.firstConnection = pc;
					resultContainer.firstSuccessIndex = dataSourceIndex;
					pc = null;
				}

				if (resultContainer.firstException == null) {
					resultContainer.firstException = exception;
				}

				resultContainer.notify();
			}

			if (pc != null) {
				try {
					pc.close();
				} catch (SQLException x) {
				}
			}
		}
	}
}
