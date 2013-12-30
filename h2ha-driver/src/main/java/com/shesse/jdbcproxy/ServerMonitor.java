/**
 * (c) S. Hesse, 2013
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.sql.SQLException;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.h2.jdbcx.JdbcDataSource;
import org.h2.jdbcx.JdbcXAConnection;

/**
 * Every instance o this class is responsible for monitoring
 * the availability of a single server instance. It does
 * so by periodic connect attempts using the underlying H2 driver.
 * <p>
 * Whenever it has successfully established a connection, it will
 * register itself at the AlternatingConnectionFactory, which
 * will in turn use it for creating productive connections.
 * <p>
 * Even though this class performs background activity, it is
 * not a thread of its own. Instead, it uses a thread pool 
 * and a timer maintained by AlternatingConnectionFactory.
 * 
 * @author sth
 */
public class ServerMonitor
implements Runnable
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	private static Logger log = Logger.getLogger(ServerMonitor.class.getName());

	/** */
	private AlternatingConnectionFactory connectionFactory;
	
	/** */
	private JdbcDataSource h2DataSource;
	
	/** */
	public enum Status { UNKNOWN, UNAVAILABLE, AVAILABLE };
	
	/** */
	private Status status = Status.UNKNOWN;
	
	/** */
	private long nextContactAttempt = 0L;
	
	/** */
	private boolean submitted = false;
	
	/** */
	private boolean stopped = false;
	
	/** */
	private volatile TimerTask timerTask = null;
	
	/** */
	private long connectRetryDelay = minConnectRetryDelay;
	
	/** */
	private static final long minConnectRetryDelay = 1000;
	
	/** */
	private static final long maxConnectRetryDelay = 20000;
	
	
	
	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 */
	public ServerMonitor(AlternatingConnectionFactory connectionFactory, JdbcDataSource h2DataSource)
	{
		this.connectionFactory = connectionFactory;
		this.h2DataSource = h2DataSource;
		
		// we schedule ourself to attempt a connection 
		// in background
		submitted = true;
		AlternatingConnectionFactory.submit(this);
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * @return the status
	 */
	public Status getStatus()
	{
		return status;
	}
	

	/**
	 * 
	 */
	public synchronized void stop()
	{
		stopped = true;
		
		if (timerTask != null) {
			timerTask.cancel();
		}
	}

	/**
	 * 
	 */
	public synchronized void checkAgain()
	{
		// we delegate the check to a thread from the
		// thread pool because it may block for some time
		if (!submitted) {
			log.fine("checkAgain submits another check for "+h2DataSource.getURL());
			submitted = true;
			if (timerTask != null) {
				timerTask.cancel();
				timerTask = null;
			}
			AlternatingConnectionFactory.submit(ServerMonitor.this);
			
		} else {
			log.fine("checkAgain does nothing because the check is already submitted for "+h2DataSource.getURL());
		}
	}

	/**
	 * Will be called as main method to execute this object as a 
	 * background job.
	 * 
	 * {@inheritDoc}
	 *
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run()
	{
		try {
			body();
			
		} catch (Throwable x) {
			log.log(Level.SEVERE, "unexepcted exception within monitor thread", x);
			
		} finally {
			submitted = false;
		}
	}
	
	/**
	 * 
	 */
	private void body()
	{
		if (stopped) {
			log.fine("ServerMonitor is giving up for "+h2DataSource.getURL()+" - it has already been closed");
			return;
		}

		try {
			// we connect to see if we can reach the server. If
			// successful, the resulting connection will be closed
			// without doing anything with it.
			log.fine("trying to contact "+h2DataSource.getURL());
			JdbcXAConnection conn = createH2XaConnection();
			conn.close();
			// serverIsAvailable has already been called in createH2XaConnection()
			
		} catch (SQLException x) {
			// serverIsUnavailable has already been called in createH2XaConnection()

		} catch (Throwable x) {
			log.log(Level.SEVERE, "unexpected exception within HA server monitor", x);
			serverIsUnavailable(new SQLException("unexpected exception", x));
		}
		
		long delay = nextContactAttempt - System.currentTimeMillis();
		if (delay <= 0) {
			// just as a safety measure - nextContactAttempt should have
			// been adjusted within serverContacted or cannotContactServer
			delay = 1000;
		}
		
		if (!stopped) {
			// schedule the next connection check. 
			synchronized (this) {
				if (timerTask != null) {
					timerTask.cancel();
				}
				timerTask = new TimerTask() {
					@Override
					public void run()
					{
						timerTask = null;
						checkAgain();
					}
				};
				AlternatingConnectionFactory.schedule(timerTask, delay);
			}
		}
	}

	/**
	 * 
	 */
	private void serverIsAvailable()
	{
		log.fine("server "+h2DataSource.getURL()+" is available");
		setLocalStatus(Status.AVAILABLE);
		connectionFactory.updateOfMonitorStatus(this, status);
	}

	/**
	 * 
	 */
	private void serverIsUnavailable(SQLException x)
	{
		log.fine("server "+h2DataSource.getURL()+" is unavailable: "+x);
		setLocalStatus(Status.UNAVAILABLE);
		connectionFactory.updateOfMonitorStatus(this, status);
	}
	
	/**
	 * 
	 */
	private void setLocalStatus(Status newStatus)
	{
		if (newStatus == Status.AVAILABLE) {
			connectRetryDelay = maxConnectRetryDelay;
			
		} else if (status == Status.AVAILABLE) {
			connectRetryDelay = minConnectRetryDelay;
		}
		
		nextContactAttempt = System.currentTimeMillis() + connectRetryDelay;
		connectRetryDelay *= 2;
		if (connectRetryDelay > maxConnectRetryDelay) {
			connectRetryDelay = maxConnectRetryDelay;
		}
		
		status = newStatus;
	}

	/**
	 * @throws SQLException 
	 * 
	 */
	public JdbcXAConnection createH2XaConnection()
		throws SQLException
	{
		try {
			JdbcXAConnection conn = (JdbcXAConnection) h2DataSource.getXAConnection();
			serverIsAvailable();
			return conn;
			
		} catch (SQLException x) {
			serverIsUnavailable(x);
			throw x;
		}
	}

	/**
	 * 
	 */
	public String toString()
	{
		return h2DataSource.getURL();
	}
	
	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
