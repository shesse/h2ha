/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.h2.constant.ErrorCode;
import org.h2.engine.SessionInterface;
import org.h2.jdbcx.JdbcXAConnection;
import org.h2.message.DbException;

import com.shesse.jdbcproxy.AlternatingConnectionFactory.RegisteredHaConnection;

/**
 * 
 * @author sth
 */
public class HaXaConnection
	implements XAConnection, XAResource, RegisteredHaConnection
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	//private static Logger log = Logger.getLogger(HaXaConnection.class.getName());

	/** */
	private AlternatingConnectionFactory connectionFactory;

	/** */
	private ServerMonitor monitoredBy;

	/** */
	private JdbcXAConnection h2XaConnection;
	
	/** */
	private SessionInterface session = null;
	
	/** */
	private Map<ConnectionEventListener, ConnectionEventListener> connectionListeners =
		new HashMap<ConnectionEventListener, ConnectionEventListener>();

	/** */
	private Map<StatementEventListener, StatementEventListener> statementListeners =
		new HashMap<StatementEventListener, StatementEventListener>();


	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
     */
	public HaXaConnection(AlternatingConnectionFactory connectionFactory, ServerMonitor monitoredBy, JdbcXAConnection h2Connection)
	{
		this.connectionFactory = connectionFactory;
		this.monitoredBy = monitoredBy;
		this.h2XaConnection = h2Connection;
		
		connectionFactory.register(this);
	}


	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * cleanup if the close was not called explicitly
	 * 
	 * {@inheritDoc}
	 *
	 * @see java.lang.Object#finalize()
	 */
	protected void finalize()
	{
		try {
			close();
		} catch (SQLException x) {
		}
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see com.shesse.jdbcproxy.AlternatingConnectionFactory.RegisteredHaConnection#getMonitoredBy()
	 */
	@Override
	public ServerMonitor getMonitoredBy()
	{
		return monitoredBy;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see com.shesse.jdbcproxy.AlternatingConnectionFactory.RegisteredHaConnection#cleanup()
	 */
	@Override
	public void cleanup()
	{
		HaConnection.forceCloseCommunicationSocket(session);
	}

	/**
	 * @return
	 * @see org.h2.jdbcx.JdbcXAConnection#getXAResource()
	 */
	public XAResource getXAResource()
	{
		return this;
	}


	/**
	 * @return
	 * @throws SQLException
	 * @see org.h2.jdbcx.JdbcXAConnection#getConnection()
	 */
	public Connection getConnection()
		throws SQLException
	{
		if (h2XaConnection == null) {
			throw DbException.get(ErrorCode.OBJECT_CLOSED).getSQLException();
		}
		
		try {
			HaConnection conn = new HaConnection(connectionFactory, monitoredBy, h2XaConnection);
			session = conn.getH2Connection().getSession();
			return conn;

		} catch (SQLException x) {
			// if the H2 connection has experienced a "session closed" exception
			// before, the poll still may try to reuse the connection. In this
			// case we get a DATABASE_CALLED_AT_SHUTDOWN, which is misleading.
			// In this case we send another error code instead.
			if (x.getErrorCode() == ErrorCode.DATABASE_CALLED_AT_SHUTDOWN) {
				throw DbException.get(ErrorCode.OBJECT_CLOSED).getSQLException();
			} else {
				throw x;
			}
		}
	}


	/**
	 * @param evl
	 * @see org.h2.jdbcx.JdbcXAConnection#addConnectionEventListener(javax.sql.ConnectionEventListener)
	 */
	public synchronized void addConnectionEventListener(final ConnectionEventListener evl)
	{
		if (h2XaConnection == null) {
			return;
		}
		
		ConnectionEventListener delegatingListener = new ConnectionEventListener() {
			public void connectionClosed(ConnectionEvent pev)
			{
				ConnectionEvent ev =
					new ConnectionEvent(HaXaConnection.this, pev.getSQLException());
				evl.connectionClosed(ev);
			}

			public void connectionErrorOccurred(ConnectionEvent pev)
			{
				ConnectionEvent ev =
					new ConnectionEvent(HaXaConnection.this, pev.getSQLException());
				evl.connectionErrorOccurred(ev);
			}
		};
		
		ConnectionEventListener old = connectionListeners.put(evl, delegatingListener);
		if (old != null) {
			h2XaConnection.removeConnectionEventListener(old);
		}
		
		h2XaConnection.addConnectionEventListener(delegatingListener);
	}


	/**
	 * @param evl
	 * @see org.h2.jdbcx.JdbcXAConnection#removeConnectionEventListener(javax.sql.ConnectionEventListener)
	 */
	public synchronized void removeConnectionEventListener(ConnectionEventListener evl)
	{
		if (h2XaConnection == null) {
			return;
		}
		
		ConnectionEventListener delegatingListener = connectionListeners.remove(evl);
		if (delegatingListener != null) {
			h2XaConnection.removeConnectionEventListener(delegatingListener);
		}
	}


	/**
	 * @param evl
	 * @see javax.sql.PooledConnection#addStatementEventListener(javax.sql.StatementEventListener)
	 */
	public void addStatementEventListener(final StatementEventListener evl)
	{
		if (h2XaConnection == null) {
			return;
		}
		
		StatementEventListener delegatingListener = new StatementEventListener() {
			@Override
			public void statementClosed(StatementEvent pev)
			{
				StatementEvent ev =
					new StatementEvent(HaXaConnection.this, pev.getStatement());
				evl.statementClosed(ev);
			}

			@Override
			public void statementErrorOccurred(StatementEvent pev)
			{
				StatementEvent ev =
					new StatementEvent(HaXaConnection.this, pev.getStatement());
				evl.statementErrorOccurred(ev);
			}
		};
		
		StatementEventListener old = statementListeners.put(evl, delegatingListener);
		if (old != null) {
			h2XaConnection.removeStatementEventListener(old);
		}
		
		h2XaConnection.addStatementEventListener(delegatingListener);
	}


	/**
	 * @param paramStatementEventListener
	 * @see javax.sql.PooledConnection#removeStatementEventListener(javax.sql.StatementEventListener)
	 */
	public void removeStatementEventListener(StatementEventListener evl)
	{
		if (h2XaConnection == null) {
			return;
		}
		
		StatementEventListener delegatingListener = statementListeners.remove(evl);
		if (delegatingListener != null) {
			h2XaConnection.removeStatementEventListener(delegatingListener);
		}
	}


	/**
	 * @throws SQLException
	 * @see org.h2.jdbcx.JdbcXAConnection#close()
	 */
	public void close()
		throws SQLException
	{
		connectionFactory.deregister(this);
		if (h2XaConnection != null) {
			for (ConnectionEventListener l: connectionListeners.values()) {
				h2XaConnection.removeConnectionEventListener(l);
			}
			for (StatementEventListener l: statementListeners.values()) {
				h2XaConnection.removeStatementEventListener(l);
			}
			h2XaConnection.getConnection().close();
			h2XaConnection = null;
		}
	}


	/**
	 * @return
	 * @see org.h2.jdbcx.JdbcXAConnection#getTransactionTimeout()
	 */
	public int getTransactionTimeout()
	{
		if (h2XaConnection == null) {
			throw DbException.get(ErrorCode.OBJECT_CLOSED);
		}
		
		return h2XaConnection.getTransactionTimeout();
	}


	/**
	 * @param paramXAResource
	 * @return
	 * @see org.h2.jdbcx.JdbcXAConnection#isSameRM(javax.transaction.xa.XAResource)
	 */
	public boolean isSameRM(XAResource paramXAResource)
	{
		return paramXAResource == this;
	}


	/**
	 * @param paramInt
	 * @return
	 * @throws XAException
	 * @see org.h2.jdbcx.JdbcXAConnection#recover(int)
	 */
	public Xid[] recover(int paramInt)
		throws XAException
	{
		if (h2XaConnection == null) {
			throw DbException.get(ErrorCode.OBJECT_CLOSED);
		}
		
		return h2XaConnection.recover(paramInt);
	}


	/**
	 * @param paramXid
	 * @return
	 * @throws XAException
	 * @see org.h2.jdbcx.JdbcXAConnection#prepare(javax.transaction.xa.Xid)
	 */
	public int prepare(Xid paramXid)
		throws XAException
	{
		if (h2XaConnection == null) {
			throw DbException.get(ErrorCode.OBJECT_CLOSED);
		}
		
		return h2XaConnection.prepare(paramXid);
	}


	/**
	 * @param paramXid
	 * @see org.h2.jdbcx.JdbcXAConnection#forget(javax.transaction.xa.Xid)
	 */
	public void forget(Xid paramXid)
	{
		if (h2XaConnection == null) {
			return;
		}
		
		h2XaConnection.forget(paramXid);
	}


	/**
	 * @param paramXid
	 * @param paramInt
	 * @throws XAException
	 * @see org.h2.jdbcx.JdbcXAConnection#end(javax.transaction.xa.Xid, int)
	 */
	public void end(Xid paramXid, int paramInt)
		throws XAException
	{
		if (h2XaConnection == null) {
			return;
		}
		
		h2XaConnection.end(paramXid, paramInt);
	}


	/**
	 * @param paramXid
	 * @param paramBoolean
	 * @throws XAException
	 * @see org.h2.jdbcx.JdbcXAConnection#commit(javax.transaction.xa.Xid,
	 *      boolean)
	 */
	public void commit(Xid paramXid, boolean paramBoolean)
		throws XAException
	{
		if (h2XaConnection == null) {
			return;
		}
		
		h2XaConnection.commit(paramXid, paramBoolean);
	}


	/**
	 * @param paramInt
	 * @return
	 * @see org.h2.jdbcx.JdbcXAConnection#setTransactionTimeout(int)
	 */
	public boolean setTransactionTimeout(int paramInt)
	{
		if (h2XaConnection == null) {
			throw DbException.get(ErrorCode.OBJECT_CLOSED);
		}
		
		return h2XaConnection.setTransactionTimeout(paramInt);
	}


	/**
	 * @param paramXid
	 * @throws XAException
	 * @see org.h2.jdbcx.JdbcXAConnection#rollback(javax.transaction.xa.Xid)
	 */
	public void rollback(Xid paramXid)
		throws XAException
	{
		if (h2XaConnection == null) {
			return;
		}
		
		h2XaConnection.rollback(paramXid);
	}


	/**
	 * @param paramXid
	 * @param paramInt
	 * @throws XAException
	 * @see org.h2.jdbcx.JdbcXAConnection#start(javax.transaction.xa.Xid, int)
	 */
	public void start(Xid paramXid, int paramInt)
		throws XAException
	{
		if (h2XaConnection == null) {
			throw DbException.get(ErrorCode.OBJECT_CLOSED);
		}
		
		h2XaConnection.start(paramXid, paramInt);
	}


	/**
	 * @return
	 * @see org.h2.jdbcx.JdbcXAConnection#toString()
	 */
	public String toString()
	{
		if (h2XaConnection == null) {
			return "null";
		}
		
		return h2XaConnection.toString();
	}



	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
