/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.apache.log4j.Logger;

/**
 *
 * @author sth
 */
public class HaStatement
    implements Statement
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(HaStatement.class);

    /** */
    protected HaConnection haConnection;
    
    /** */
    private Statement base;
    

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @param statement 
     * @param haConnection 
     */
    public HaStatement(HaConnection haConnection, Statement base)
    {
	log.debug("HaStatement()");
	
	this.haConnection = haConnection;
	this.base = base;
    }


    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @param sql
     * @return
     * @throws SQLException
     * @see java.sql.Statement#executeQuery(java.lang.String)
     */
    public ResultSet executeQuery(String sql)
	throws SQLException
    {
	return new HaResultSet(haConnection, this, base.executeQuery(sql));
    }


    /**
     * @param <T>
     * @param iface
     * @return
     * @throws SQLException
     * @see java.sql.Wrapper#unwrap(java.lang.Class)
     */
    public <T> T unwrap(Class<T> iface)
	throws SQLException
    {
	return base.unwrap(iface);
    }


    /**
     * @param sql
     * @return
     * @throws SQLException
     * @see java.sql.Statement#executeUpdate(java.lang.String)
     */
    public int executeUpdate(String sql)
	throws SQLException
    {
	return base.executeUpdate(sql);
    }


    /**
     * @param iface
     * @return
     * @throws SQLException
     * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
     */
    public boolean isWrapperFor(Class<?> iface)
	throws SQLException
    {
	return base.isWrapperFor(iface);
    }


    /**
     * @throws SQLException
     * @see java.sql.Statement#close()
     */
    public void close()
	throws SQLException
    {
	base.close();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#getMaxFieldSize()
     */
    public int getMaxFieldSize()
	throws SQLException
    {
	return base.getMaxFieldSize();
    }


    /**
     * @param max
     * @throws SQLException
     * @see java.sql.Statement#setMaxFieldSize(int)
     */
    public void setMaxFieldSize(int max)
	throws SQLException
    {
	base.setMaxFieldSize(max);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#getMaxRows()
     */
    public int getMaxRows()
	throws SQLException
    {
	return base.getMaxRows();
    }


    /**
     * @param max
     * @throws SQLException
     * @see java.sql.Statement#setMaxRows(int)
     */
    public void setMaxRows(int max)
	throws SQLException
    {
	base.setMaxRows(max);
    }


    /**
     * @param enable
     * @throws SQLException
     * @see java.sql.Statement#setEscapeProcessing(boolean)
     */
    public void setEscapeProcessing(boolean enable)
	throws SQLException
    {
	base.setEscapeProcessing(enable);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#getQueryTimeout()
     */
    public int getQueryTimeout()
	throws SQLException
    {
	return base.getQueryTimeout();
    }


    /**
     * @param seconds
     * @throws SQLException
     * @see java.sql.Statement#setQueryTimeout(int)
     */
    public void setQueryTimeout(int seconds)
	throws SQLException
    {
	base.setQueryTimeout(seconds);
    }


    /**
     * @throws SQLException
     * @see java.sql.Statement#cancel()
     */
    public void cancel()
	throws SQLException
    {
	base.cancel();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#getWarnings()
     */
    public SQLWarning getWarnings()
	throws SQLException
    {
	return base.getWarnings();
    }


    /**
     * @throws SQLException
     * @see java.sql.Statement#clearWarnings()
     */
    public void clearWarnings()
	throws SQLException
    {
	base.clearWarnings();
    }


    /**
     * @param name
     * @throws SQLException
     * @see java.sql.Statement#setCursorName(java.lang.String)
     */
    public void setCursorName(String name)
	throws SQLException
    {
	base.setCursorName(name);
    }


    /**
     * @param sql
     * @return
     * @throws SQLException
     * @see java.sql.Statement#execute(java.lang.String)
     */
    public boolean execute(String sql)
	throws SQLException
    {
	return base.execute(sql);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#getResultSet()
     */
    public ResultSet getResultSet()
	throws SQLException
    {
	return new HaResultSet(haConnection, this, base.getResultSet());
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#getUpdateCount()
     */
    public int getUpdateCount()
	throws SQLException
    {
	return base.getUpdateCount();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#getMoreResults()
     */
    public boolean getMoreResults()
	throws SQLException
    {
	return base.getMoreResults();
    }


    /**
     * @param direction
     * @throws SQLException
     * @see java.sql.Statement#setFetchDirection(int)
     */
    public void setFetchDirection(int direction)
	throws SQLException
    {
	base.setFetchDirection(direction);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#getFetchDirection()
     */
    public int getFetchDirection()
	throws SQLException
    {
	return base.getFetchDirection();
    }


    /**
     * @param rows
     * @throws SQLException
     * @see java.sql.Statement#setFetchSize(int)
     */
    public void setFetchSize(int rows)
	throws SQLException
    {
	base.setFetchSize(rows);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#getFetchSize()
     */
    public int getFetchSize()
	throws SQLException
    {
	return base.getFetchSize();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#getResultSetConcurrency()
     */
    public int getResultSetConcurrency()
	throws SQLException
    {
	return base.getResultSetConcurrency();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#getResultSetType()
     */
    public int getResultSetType()
	throws SQLException
    {
	return base.getResultSetType();
    }


    /**
     * @param sql
     * @throws SQLException
     * @see java.sql.Statement#addBatch(java.lang.String)
     */
    public void addBatch(String sql)
	throws SQLException
    {
	base.addBatch(sql);
    }


    /**
     * @throws SQLException
     * @see java.sql.Statement#clearBatch()
     */
    public void clearBatch()
	throws SQLException
    {
	base.clearBatch();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#executeBatch()
     */
    public int[] executeBatch()
	throws SQLException
    {
	return base.executeBatch();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#getConnection()
     */
    public Connection getConnection()
	throws SQLException
    {
	return haConnection;
    }


    /**
     * @param current
     * @return
     * @throws SQLException
     * @see java.sql.Statement#getMoreResults(int)
     */
    public boolean getMoreResults(int current)
	throws SQLException
    {
	return base.getMoreResults(current);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#getGeneratedKeys()
     */
    public ResultSet getGeneratedKeys()
	throws SQLException
    {
	return new HaResultSet(haConnection, this, base.getGeneratedKeys());
    }


    /**
     * @param sql
     * @param autoGeneratedKeys
     * @return
     * @throws SQLException
     * @see java.sql.Statement#executeUpdate(java.lang.String, int)
     */
    public int executeUpdate(String sql, int autoGeneratedKeys)
	throws SQLException
    {
	return base.executeUpdate(sql, autoGeneratedKeys);
    }


    /**
     * @param sql
     * @param columnIndexes
     * @return
     * @throws SQLException
     * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
     */
    public int executeUpdate(String sql, int[] columnIndexes)
	throws SQLException
    {
	return base.executeUpdate(sql, columnIndexes);
    }


    /**
     * @param sql
     * @param columnNames
     * @return
     * @throws SQLException
     * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
     */
    public int executeUpdate(String sql, String[] columnNames)
	throws SQLException
    {
	return base.executeUpdate(sql, columnNames);
    }


    /**
     * @param sql
     * @param autoGeneratedKeys
     * @return
     * @throws SQLException
     * @see java.sql.Statement#execute(java.lang.String, int)
     */
    public boolean execute(String sql, int autoGeneratedKeys)
	throws SQLException
    {
	return base.execute(sql, autoGeneratedKeys);
    }


    /**
     * @param sql
     * @param columnIndexes
     * @return
     * @throws SQLException
     * @see java.sql.Statement#execute(java.lang.String, int[])
     */
    public boolean execute(String sql, int[] columnIndexes)
	throws SQLException
    {
	return base.execute(sql, columnIndexes);
    }


    /**
     * @param sql
     * @param columnNames
     * @return
     * @throws SQLException
     * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
     */
    public boolean execute(String sql, String[] columnNames)
	throws SQLException
    {
	return base.execute(sql, columnNames);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#getResultSetHoldability()
     */
    public int getResultSetHoldability()
	throws SQLException
    {
	return base.getResultSetHoldability();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#isClosed()
     */
    public boolean isClosed()
	throws SQLException
    {
	return base.isClosed();
    }


    /**
     * @param poolable
     * @throws SQLException
     * @see java.sql.Statement#setPoolable(boolean)
     */
    public void setPoolable(boolean poolable)
	throws SQLException
    {
	base.setPoolable(poolable);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Statement#isPoolable()
     */
    public boolean isPoolable()
	throws SQLException
    {
	return base.isPoolable();
    }





    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
