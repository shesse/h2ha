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
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.h2.jdbcx.JdbcXAConnection;

/**
 * 
 * @author sth
 */
public class HaXaConnection
	implements XAConnection, XAResource
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	// private static Logger log = Logger.getLogger(HaXaConnection.class);

	/** */
	private JdbcXAConnection h2XaConnection;
	
	/** */
	private Map<ConnectionEventListener, ConnectionEventListener> delegatingListeners =
		new HashMap<ConnectionEventListener, ConnectionEventListener>();


	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
     */
	public HaXaConnection(JdbcXAConnection h2Connection)
	{
		this.h2XaConnection = h2Connection;
	}


	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
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
		return new HaConnection(h2XaConnection.getConnection());
	}


	/**
	 * @param evl
	 * @see org.h2.jdbcx.JdbcXAConnection#addConnectionEventListener(javax.sql.ConnectionEventListener)
	 */
	public synchronized void addConnectionEventListener(final ConnectionEventListener evl)
	{
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
		
		ConnectionEventListener old = delegatingListeners.put(evl, delegatingListener);
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
		ConnectionEventListener delegatingListener = delegatingListeners.remove(evl);
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
		// TODO: activate this as soon as H2 supports it (H2 version for Java >=
		// 1.6)
		/* !see addConnectionEventListener for modified procedure
		 * h2XaConnection.addStatementEventListener(new StatementEventListener() {
		 * 
		 * public void statementClosed(StatementEvent pev) { StatementEvent ev =
		 * new StatementEvent(HaXaConnection.this, pev.getStatement(),
		 * pev.getSQLException()); evl.statementClosed(ev); }
		 * 
		 * public void statementErrorOccurred(StatementEvent pev) {
		 * StatementEvent ev = new StatementEvent(HaXaConnection.this,
		 * pev.getStatement(), pev.getSQLException());
		 * evl.statementErrorOccurred(ev); }
		 * 
		 * });
		 */
	}


	/**
	 * @param paramStatementEventListener
	 * @see javax.sql.PooledConnection#removeStatementEventListener(javax.sql.StatementEventListener)
	 */
	public void removeStatementEventListener(StatementEventListener paramStatementEventListener)
	{
		// TODO: activate this as soon as H2 supports it (H2 version for Java >=
		// 1.6)
		/* !see addConnectionEventListener for modified procedure
		 * h2XaConnection.removeStatementEventListener(paramStatementEventListener)
		 * ;
		 */
	}


	/**
	 * @throws SQLException
	 * @see org.h2.jdbcx.JdbcXAConnection#close()
	 */
	public void close()
		throws SQLException
	{
		h2XaConnection.close();
	}


	/**
	 * @return
	 * @see org.h2.jdbcx.JdbcXAConnection#getTransactionTimeout()
	 */
	public int getTransactionTimeout()
	{
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
		return h2XaConnection.prepare(paramXid);
	}


	/**
	 * @param paramXid
	 * @see org.h2.jdbcx.JdbcXAConnection#forget(javax.transaction.xa.Xid)
	 */
	public void forget(Xid paramXid)
	{
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
		h2XaConnection.commit(paramXid, paramBoolean);
	}


	/**
	 * @param paramInt
	 * @return
	 * @see org.h2.jdbcx.JdbcXAConnection#setTransactionTimeout(int)
	 */
	public boolean setTransactionTimeout(int paramInt)
	{
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
		h2XaConnection.start(paramXid, paramInt);
	}


	/**
	 * @return
	 * @see org.h2.jdbcx.JdbcXAConnection#toString()
	 */
	public String toString()
	{
		return h2XaConnection.toString();
	}


	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
