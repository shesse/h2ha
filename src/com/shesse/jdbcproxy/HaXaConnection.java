/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEvent;
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
    //private static Logger log = Logger.getLogger(HaXaConnection.class);

    /** */
    private JdbcXAConnection h2Connection;
    

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public HaXaConnection(JdbcXAConnection h2Connection)
    {
	this.h2Connection = h2Connection;
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
	return new HaConnection(h2Connection.getConnection());
    }


    /**
     * @param evl
     * @see org.h2.jdbcx.JdbcXAConnection#addConnectionEventListener(javax.sql.ConnectionEventListener)
     */
    public void addConnectionEventListener(final ConnectionEventListener evl)
    {
	h2Connection.addConnectionEventListener(new ConnectionEventListener() {

	    public void connectionClosed(ConnectionEvent pev)
	    {
		ConnectionEvent ev = new ConnectionEvent(HaXaConnection.this, pev.getSQLException());
		evl.connectionClosed(ev);
	    }

	    public void connectionErrorOccurred(ConnectionEvent pev)
	    {
		ConnectionEvent ev = new ConnectionEvent(HaXaConnection.this, pev.getSQLException());
		evl.connectionErrorOccurred(ev);
	    }
	});
    }


    /**
     * @param evl
     * @see javax.sql.PooledConnection#addStatementEventListener(javax.sql.StatementEventListener)
     */
    public void addStatementEventListener(final StatementEventListener evl)
    {
    	//TODO: aktivieren, wenn H2 Version für Java 1.6 Basis wird
    	/*
    	h2Connection.addStatementEventListener(new StatementEventListener() {

	    public void statementClosed(StatementEvent pev)
	    {
		StatementEvent ev = new StatementEvent(HaXaConnection.this, pev.getStatement(), pev.getSQLException());
		evl.statementClosed(ev);
	    }

	    public void statementErrorOccurred(StatementEvent pev)
	    {
		StatementEvent ev = new StatementEvent(HaXaConnection.this, pev.getStatement(), pev.getSQLException());
		evl.statementErrorOccurred(ev);
	    }
	    
	});
	*/
    }


    /**
     * @throws SQLException
     * @see org.h2.jdbcx.JdbcXAConnection#close()
     */
    public void close()
	throws SQLException
    {
	h2Connection.close();
    }


    /**
     * @param paramConnectionEventListener
     * @see org.h2.jdbcx.JdbcXAConnection#removeConnectionEventListener(javax.sql.ConnectionEventListener)
     */
    public void removeConnectionEventListener(ConnectionEventListener paramConnectionEventListener)
    {
	h2Connection.removeConnectionEventListener(paramConnectionEventListener);
    }


    /**
     * @return
     * @see org.h2.jdbcx.JdbcXAConnection#getTransactionTimeout()
     */
    public int getTransactionTimeout()
    {
	return h2Connection.getTransactionTimeout();
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
	return h2Connection.recover(paramInt);
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
	return h2Connection.prepare(paramXid);
    }


    /**
     * @param paramXid
     * @see org.h2.jdbcx.JdbcXAConnection#forget(javax.transaction.xa.Xid)
     */
    public void forget(Xid paramXid)
    {
	h2Connection.forget(paramXid);
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
	h2Connection.end(paramXid, paramInt);
    }


    /**
     * @param paramXid
     * @param paramBoolean
     * @throws XAException
     * @see org.h2.jdbcx.JdbcXAConnection#commit(javax.transaction.xa.Xid, boolean)
     */
    public void commit(Xid paramXid, boolean paramBoolean)
	throws XAException
    {
	h2Connection.commit(paramXid, paramBoolean);
    }


    /**
     * @param paramStatementEventListener
     * @see javax.sql.PooledConnection#removeStatementEventListener(javax.sql.StatementEventListener)
     */
    public void removeStatementEventListener(StatementEventListener paramStatementEventListener)
    {
    	//TODO: aktivieren, wenn H2 Version für Java 1.6 Basis wird
    	/*
	h2Connection.removeStatementEventListener(paramStatementEventListener);
	*/
    }


    /**
     * @param paramInt
     * @return
     * @see org.h2.jdbcx.JdbcXAConnection#setTransactionTimeout(int)
     */
    public boolean setTransactionTimeout(int paramInt)
    {
	return h2Connection.setTransactionTimeout(paramInt);
    }


    /**
     * @param paramXid
     * @throws XAException
     * @see org.h2.jdbcx.JdbcXAConnection#rollback(javax.transaction.xa.Xid)
     */
    public void rollback(Xid paramXid)
	throws XAException
    {
	h2Connection.rollback(paramXid);
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
	h2Connection.start(paramXid, paramInt);
    }


    /**
     * @return
     * @see org.h2.jdbcx.JdbcXAConnection#toString()
     */
    public String toString()
    {
	return h2Connection.toString();
    }



    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
