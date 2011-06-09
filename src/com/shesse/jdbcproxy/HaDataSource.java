/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import org.apache.log4j.Logger;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.jdbcx.JdbcXAConnection;

/**
 *
 * @author sth
 */
public class HaDataSource
    implements XADataSource, DataSource, ConnectionPoolDataSource, Serializable,
    Referenceable
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static final long serialVersionUID = 1L;

    /** */
    private static Logger log = Logger.getLogger(HaDataSource.class);

    /** */
    private JdbcDataSource h2DataSource;



    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public HaDataSource()
    {
	log.debug("HaDataSource()");
	h2DataSource = new JdbcDataSource();
    }

    /**
     */
    public HaDataSource(JdbcDataSource h2DataSource)
    {
	log.debug("HaDataSource()");
	this.h2DataSource = h2DataSource;
    }


    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * Called when de-serializing the object.
     *
     * @param in the input stream
     */
    private void readObject(ObjectInputStream in)
    throws IOException, ClassNotFoundException
    {
        in.defaultReadObject();
    }

    /**
     * 
     */
    public JdbcDataSource getH2DataSource()
    {
	return h2DataSource;
    }
    
    /**
     * {@inheritDoc}
     *
     * @see javax.sql.CommonDataSource#getLogWriter()
     */
    public PrintWriter getLogWriter()
	throws SQLException
    {
	return h2DataSource.getLogWriter();
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.CommonDataSource#getLoginTimeout()
     */
    public int getLoginTimeout()
	throws SQLException
    {
	return h2DataSource.getLoginTimeout();
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.CommonDataSource#setLogWriter(java.io.PrintWriter)
     */
    public void setLogWriter(PrintWriter writer)
	throws SQLException
    {
	h2DataSource.setLogWriter(writer);
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.CommonDataSource#setLoginTimeout(int)
     */
    public void setLoginTimeout(int timeout)
	throws SQLException
    {
	h2DataSource.setLoginTimeout(timeout);
    }


    /**
     * {@inheritDoc}
     *
     * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
     */
    public boolean isWrapperFor(Class<?> iface)
	throws SQLException
    {
	return h2DataSource.isWrapperFor(iface);
    }


    /**
     * {@inheritDoc}
     *
     * @see java.sql.Wrapper#unwrap(java.lang.Class)
     */
    public <T> T unwrap(Class<T> iface)
	throws SQLException
    {
	return h2DataSource.unwrap(iface);
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.naming.Referenceable#getReference()
     */
    public Reference getReference()
	throws NamingException
    {
        String factoryClassName = HaDataSourceFactory.class.getName();
        Reference ref = new Reference(getClass().getName(), factoryClassName, null);
        ref.add(new StringRefAddr("url", h2DataSource.getURL()));
        ref.add(new StringRefAddr("user", h2DataSource.getUser()));
        ref.add(new StringRefAddr("password", h2DataSource.getPassword()));
        ref.add(new StringRefAddr("loginTimeout", String.valueOf(h2DataSource.getLoginTimeout())));
        ref.add(new StringRefAddr("description", h2DataSource.getDescription()));
        return ref;
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.ConnectionPoolDataSource#getPooledConnection()
     */
    public PooledConnection getPooledConnection()
	throws SQLException
    {
	return new HaXaConnection((JdbcXAConnection)h2DataSource.getPooledConnection());
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.ConnectionPoolDataSource#getPooledConnection(java.lang.String, java.lang.String)
     */
    public PooledConnection getPooledConnection(String user, String password)
	throws SQLException
    {
	return new HaXaConnection((JdbcXAConnection)h2DataSource.getPooledConnection(user, password));
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.DataSource#getConnection()
     */
    public Connection getConnection()
	throws SQLException
    {
	return new HaConnection((JdbcConnection)h2DataSource.getConnection());
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
     */
    public Connection getConnection(String user, String password)
	throws SQLException
    {
	return new HaConnection((JdbcConnection)h2DataSource.getConnection(user, password));
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.XADataSource#getXAConnection()
     */
    public XAConnection getXAConnection()
	throws SQLException
    {
	return new HaXaConnection((JdbcXAConnection)h2DataSource.getXAConnection());
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.XADataSource#getXAConnection(java.lang.String, java.lang.String)
     */
    public XAConnection getXAConnection(String user, String password)
	throws SQLException
    {
	return new HaXaConnection((JdbcXAConnection)h2DataSource.getXAConnection(user, password));
    }



    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
