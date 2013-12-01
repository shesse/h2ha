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
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

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
    //private static Logger log = Logger.getLogger(HaDataSource.class);

    /** */
    private String url;
    
    /** */
    private Properties props;
    
    /** */
    private PrintWriter logWriter = null;
    

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public HaDataSource()
    {
	url = null;
	props = new Properties();
    }

    /**
     */
    public HaDataSource(String url, Properties props)
    {
	this.url = url;
	this.props = props;
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
     * @throws SQLException 
     * 
     */
    public AlternatingConnectionFactory getBaseConnectionFactory()
    throws SQLException
    {
	return AlternatingConnectionFactory.getFactory(url, props);
    }
    
    /**
     * {@inheritDoc}
     *
     * @see javax.sql.CommonDataSource#getLogWriter()
     */
    public PrintWriter getLogWriter()
	throws SQLException
    {
	return logWriter;
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.CommonDataSource#getLoginTimeout()
     */
    public int getLoginTimeout()
	throws SQLException
    {
	try {
	    return Integer.parseInt(props.getProperty("loginTimeout", "0"));
	} catch (NumberFormatException x) {
	    return 0;
	}
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.CommonDataSource#setLogWriter(java.io.PrintWriter)
     */
    public void setLogWriter(PrintWriter writer)
	throws SQLException
    {
	this.logWriter = writer;
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.CommonDataSource#setLoginTimeout(int)
     */
    public void setLoginTimeout(int timeout)
	throws SQLException
    {
	props.setProperty("loginTimeout", String.valueOf(timeout));
    }


    /**
     * {@inheritDoc}
     *
     * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
     */
    public boolean isWrapperFor(Class<?> iface)
	throws SQLException
    {
	return false;
    }


    /**
     * {@inheritDoc}
     *
     * @see java.sql.Wrapper#unwrap(java.lang.Class)
     */
    public <T> T unwrap(Class<T> iface)
	throws SQLException
    {
	throw new SQLException("cannot unwrap a "+getClass().getName()+" to a "+iface.getName());
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
        ref.add(new StringRefAddr("url", url));
        ref.add(new StringRefAddr("user", props.getProperty("user")));
        ref.add(new StringRefAddr("password", props.getProperty("password")));
        ref.add(new StringRefAddr("loginTimeout", props.getProperty("loginTimeout")));
        ref.add(new StringRefAddr("description", props.getProperty("description")));
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
	JdbcXAConnection h2xac = AlternatingConnectionFactory.getFactory(url, props).getXaConnection();
	return new HaXaConnection(h2xac);
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.ConnectionPoolDataSource#getPooledConnection(java.lang.String, java.lang.String)
     */
    public PooledConnection getPooledConnection(String user, String password)
	throws SQLException
    {
	Properties modprops = new Properties(props);
	modprops.setProperty("user", user);
	modprops.setProperty("password", password);
	JdbcXAConnection h2xac = AlternatingConnectionFactory.getFactory(url, modprops).getXaConnection();
	return new HaXaConnection(h2xac);
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.DataSource#getConnection()
     */
    public Connection getConnection()
	throws SQLException
    {
	Connection h2c = AlternatingConnectionFactory.getFactory(url, props).getConnection();
	return new HaConnection(h2c);
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
     */
    public Connection getConnection(String user, String password)
	throws SQLException
    {
	Properties modprops = new Properties(props);
	modprops.setProperty("user", user);
	modprops.setProperty("password", password);
	Connection h2c = AlternatingConnectionFactory.getFactory(url, modprops).getConnection();
	return new HaConnection(h2c);    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.XADataSource#getXAConnection()
     */
    public XAConnection getXAConnection()
	throws SQLException
    {
	JdbcXAConnection h2xac = AlternatingConnectionFactory.getFactory(url, props).getXaConnection();
	return new HaXaConnection(h2xac);
    }


    /**
     * {@inheritDoc}
     *
     * @see javax.sql.XADataSource#getXAConnection(java.lang.String, java.lang.String)
     */
    public XAConnection getXAConnection(String user, String password)
	throws SQLException
    {
	Properties modprops = new Properties(props);
	modprops.setProperty("user", user);
	modprops.setProperty("password", password);
	JdbcXAConnection h2xac = AlternatingConnectionFactory.getFactory(url, modprops).getXaConnection();
	return new HaXaConnection(h2xac);
    }

	/**
	 * {@inheritDoc}
	 *
	 * @see javax.sql.CommonDataSource#getParentLogger()
	 */
	public Logger getParentLogger()
		throws SQLFeatureNotSupportedException
	{
		throw new SQLFeatureNotSupportedException("");
	}



    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
