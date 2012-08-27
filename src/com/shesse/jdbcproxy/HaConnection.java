/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 *
 * @author sth
 */
public class HaConnection
    implements Connection
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    //private static Logger log = Logger.getLogger(HaConnection.class);

    /** */
    private Connection h2Connection;
    
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public HaConnection(Connection h2Connection)
    {
	this.h2Connection = h2Connection;
    }


    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @throws SQLException
     * @see java.sql.Connection#clearWarnings()
     */
    public void clearWarnings()
	throws SQLException
    {
	h2Connection.clearWarnings();
    }


    /**
     * @throws SQLException
     * @see java.sql.Connection#close()
     */
    public void close()
	throws SQLException
    {
	h2Connection.close();
    }


    /**
     * @throws SQLException
     * @see java.sql.Connection#commit()
     */
    public void commit()
	throws SQLException
    {
	h2Connection.commit();
    }


    /**
     * @param paramString
     * @param paramArrayOfObject
     * @return
     * @throws SQLException
     * @see java.sql.Connection#createArrayOf(java.lang.String, java.lang.Object[])
     */
    public Array createArrayOf(String paramString, Object[] paramArrayOfObject)
	throws SQLException
    {
	return new HaArray(this, h2Connection.createArrayOf(paramString, paramArrayOfObject));
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#createBlob()
     */
    public Blob createBlob()
	throws SQLException
    {
	return new HaBlob(this, h2Connection.createBlob());
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#createClob()
     */
    public Clob createClob()
	throws SQLException
    {
	return new HaClob(this, h2Connection.createClob());
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#createNClob()
     */
    public NClob createNClob()
	throws SQLException
    {
	return new HaNClob(this, h2Connection.createNClob());
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#createSQLXML()
     */
    public SQLXML createSQLXML()
	throws SQLException
    {
	return new HaSQLXML(this, h2Connection.createSQLXML());
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#createStatement()
     */
    public Statement createStatement()
	throws SQLException
    {
	return new HaStatement(this, h2Connection.createStatement());
    }


    /**
     * @param resultSetType
     * @param resultSetConcurrency
     * @param resultSetHoldability
     * @return
     * @throws SQLException
     * @see java.sql.Connection#createStatement(int, int, int)
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
	throws SQLException
    {
	return new HaStatement(this, h2Connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
    }


    /**
     * @param resultSetType
     * @param resultSetConcurrency
     * @return
     * @throws SQLException
     * @see java.sql.Connection#createStatement(int, int)
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
	throws SQLException
    {
	return new HaStatement(this, h2Connection.createStatement(resultSetType, resultSetConcurrency));
    }


    /**
     * @param paramString
     * @param paramArrayOfObject
     * @return
     * @throws SQLException
     * @see java.sql.Connection#createStruct(java.lang.String, java.lang.Object[])
     */
    public Struct createStruct(String paramString, Object[] paramArrayOfObject)
	throws SQLException
    {
	return new HaStruct(this, h2Connection.createStruct(paramString, paramArrayOfObject));
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#getAutoCommit()
     */
    public boolean getAutoCommit()
	throws SQLException
    {
	return h2Connection.getAutoCommit();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#getCatalog()
     */
    public String getCatalog()
	throws SQLException
    {
	return h2Connection.getCatalog();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#getClientInfo()
     */
    public Properties getClientInfo()
	throws SQLException
    {
	return h2Connection.getClientInfo();
    }


    /**
     * @param paramString
     * @return
     * @throws SQLException
     * @see java.sql.Connection#getClientInfo(java.lang.String)
     */
    public String getClientInfo(String paramString)
	throws SQLException
    {
	return h2Connection.getClientInfo(paramString);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#getHoldability()
     */
    public int getHoldability()
	throws SQLException
    {
	return h2Connection.getHoldability();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#getMetaData()
     */
    public DatabaseMetaData getMetaData()
	throws SQLException
    {
	return new HaDatabaseMetaData(this, h2Connection.getMetaData());
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#getTransactionIsolation()
     */
    public int getTransactionIsolation()
	throws SQLException
    {
	return h2Connection.getTransactionIsolation();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#getTypeMap()
     */
    public Map<String, Class<?>> getTypeMap()
	throws SQLException
    {
	return h2Connection.getTypeMap();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#getWarnings()
     */
    public SQLWarning getWarnings()
	throws SQLException
    {
	return h2Connection.getWarnings();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#isClosed()
     */
    public boolean isClosed()
	throws SQLException
    {
	return h2Connection.isClosed();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#isReadOnly()
     */
    public boolean isReadOnly()
	throws SQLException
    {
	return h2Connection.isReadOnly();
    }


    /**
     * @param paramInt
     * @return
     * @throws SQLException
     * @see java.sql.Connection#isValid(int)
     */
    public boolean isValid(int paramInt)
	throws SQLException
    {
	return h2Connection.isValid(paramInt);
    }


    /**
     * @param paramClass
     * @return
     * @throws SQLException
     * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
     */
    public boolean isWrapperFor(Class<?> paramClass)
	throws SQLException
    {
	return h2Connection.isWrapperFor(paramClass);
    }


    /**
     * @param paramString
     * @return
     * @throws SQLException
     * @see java.sql.Connection#nativeSQL(java.lang.String)
     */
    public String nativeSQL(String paramString)
	throws SQLException
    {
	return h2Connection.nativeSQL(paramString);
    }


    /**
     * @param paramString
     * @param paramInt1
     * @param paramInt2
     * @param paramInt3
     * @return
     * @throws SQLException
     * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
     */
    public CallableStatement prepareCall(String paramString, int paramInt1, int paramInt2,
					 int paramInt3)
	throws SQLException
    {
	return new HaCallableStatement(this, h2Connection.prepareCall(paramString, paramInt1, paramInt2, paramInt3));
    }


    /**
     * @param paramString
     * @param paramInt1
     * @param paramInt2
     * @return
     * @throws SQLException
     * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
     */
    public CallableStatement prepareCall(String paramString, int paramInt1, int paramInt2)
	throws SQLException
    {
	return new HaCallableStatement(this, h2Connection.prepareCall(paramString, paramInt1, paramInt2));
    }


    /**
     * @param paramString
     * @return
     * @throws SQLException
     * @see java.sql.Connection#prepareCall(java.lang.String)
     */
    public CallableStatement prepareCall(String paramString)
	throws SQLException
    {
	return new HaCallableStatement(this, h2Connection.prepareCall(paramString));
    }


    /**
     * @param paramString
     * @param paramInt1
     * @param paramInt2
     * @param paramInt3
     * @return
     * @throws SQLException
     * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
     */
    public PreparedStatement prepareStatement(String paramString, int paramInt1, int paramInt2,
					      int paramInt3)
	throws SQLException
    {
	return new HaPreparedStatement(this, h2Connection.prepareStatement(paramString, paramInt1, paramInt2, paramInt3));
    }


    /**
     * @param paramString
     * @param paramInt1
     * @param paramInt2
     * @return
     * @throws SQLException
     * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
     */
    public PreparedStatement prepareStatement(String paramString, int paramInt1, int paramInt2)
	throws SQLException
    {
	return new HaPreparedStatement(this, h2Connection.prepareStatement(paramString, paramInt1, paramInt2));
    }


    /**
     * @param paramString
     * @param paramInt
     * @return
     * @throws SQLException
     * @see java.sql.Connection#prepareStatement(java.lang.String, int)
     */
    public PreparedStatement prepareStatement(String paramString, int paramInt)
	throws SQLException
    {
	return new HaPreparedStatement(this, h2Connection.prepareStatement(paramString, paramInt));
    }


    /**
     * @param paramString
     * @param paramArrayOfInt
     * @return
     * @throws SQLException
     * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
     */
    public PreparedStatement prepareStatement(String paramString, int[] paramArrayOfInt)
	throws SQLException
    {
	return new HaPreparedStatement(this, h2Connection.prepareStatement(paramString, paramArrayOfInt));
    }


    /**
     * @param paramString
     * @param paramArrayOfString
     * @return
     * @throws SQLException
     * @see java.sql.Connection#prepareStatement(java.lang.String, java.lang.String[])
     */
    public PreparedStatement prepareStatement(String paramString, String[] paramArrayOfString)
	throws SQLException
    {
	return new HaPreparedStatement(this, h2Connection.prepareStatement(paramString, paramArrayOfString));
    }


    /**
     * @param paramString
     * @return
     * @throws SQLException
     * @see java.sql.Connection#prepareStatement(java.lang.String)
     */
    public PreparedStatement prepareStatement(String paramString)
	throws SQLException
    {
	return new HaPreparedStatement(this, h2Connection.prepareStatement(paramString));
    }


    /**
     * @param paramSavepoint
     * @throws SQLException
     * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
     */
    public void releaseSavepoint(Savepoint paramSavepoint)
	throws SQLException
    {
	h2Connection.releaseSavepoint(paramSavepoint);
    }


    /**
     * @throws SQLException
     * @see java.sql.Connection#rollback()
     */
    public void rollback()
	throws SQLException
    {
	h2Connection.rollback();
    }


    /**
     * @param paramSavepoint
     * @throws SQLException
     * @see java.sql.Connection#rollback(java.sql.Savepoint)
     */
    public void rollback(Savepoint paramSavepoint)
	throws SQLException
    {
	h2Connection.rollback(paramSavepoint);
    }


    /**
     * @param paramBoolean
     * @throws SQLException
     * @see java.sql.Connection#setAutoCommit(boolean)
     */
    public void setAutoCommit(boolean paramBoolean)
	throws SQLException
    {
	h2Connection.setAutoCommit(paramBoolean);
    }


    /**
     * @param paramString
     * @throws SQLException
     * @see java.sql.Connection#setCatalog(java.lang.String)
     */
    public void setCatalog(String paramString)
	throws SQLException
    {
	h2Connection.setCatalog(paramString);
    }


    /**
     * @param paramProperties
     * @throws SQLClientInfoException
     * @see java.sql.Connection#setClientInfo(java.util.Properties)
     */
    public void setClientInfo(Properties paramProperties)
	throws SQLClientInfoException
    {
	h2Connection.setClientInfo(paramProperties);
    }


    /**
     * @param paramString1
     * @param paramString2
     * @throws SQLClientInfoException
     * @see java.sql.Connection#setClientInfo(java.lang.String, java.lang.String)
     */
    public void setClientInfo(String paramString1, String paramString2)
	throws SQLClientInfoException
    {
	h2Connection.setClientInfo(paramString1, paramString2);
    }


    /**
     * @param paramInt
     * @throws SQLException
     * @see java.sql.Connection#setHoldability(int)
     */
    public void setHoldability(int paramInt)
	throws SQLException
    {
	h2Connection.setHoldability(paramInt);
    }


    /**
     * @param paramBoolean
     * @throws SQLException
     * @see java.sql.Connection#setReadOnly(boolean)
     */
    public void setReadOnly(boolean paramBoolean)
	throws SQLException
    {
	h2Connection.setReadOnly(paramBoolean);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Connection#setSavepoint()
     */
    public Savepoint setSavepoint()
	throws SQLException
    {
	return h2Connection.setSavepoint();
    }


    /**
     * @param paramString
     * @return
     * @throws SQLException
     * @see java.sql.Connection#setSavepoint(java.lang.String)
     */
    public Savepoint setSavepoint(String paramString)
	throws SQLException
    {
	return h2Connection.setSavepoint(paramString);
    }


    /**
     * @param paramInt
     * @throws SQLException
     * @see java.sql.Connection#setTransactionIsolation(int)
     */
    public void setTransactionIsolation(int paramInt)
	throws SQLException
    {
	h2Connection.setTransactionIsolation(paramInt);
    }


    /**
     * @param paramMap
     * @throws SQLException
     * @see java.sql.Connection#setTypeMap(java.util.Map)
     */
    public void setTypeMap(Map<String, Class<?>> paramMap)
	throws SQLException
    {
	h2Connection.setTypeMap(paramMap);
    }


    /**
     * @param <T>
     * @param paramClass
     * @return
     * @throws SQLException
     * @see java.sql.Wrapper#unwrap(java.lang.Class)
     */
    public <T> T unwrap(Class<T> paramClass)
	throws SQLException
    {
	return h2Connection.unwrap(paramClass);
    }


	/**
	 * {@inheritDoc}
	 *
	 * @see java.sql.Connection#setSchema(java.lang.String)
	 */
	public void setSchema(String schema)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("");
	}


	/**
	 * {@inheritDoc}
	 *
	 * @see java.sql.Connection#getSchema()
	 */
	public String getSchema()
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("");
	}


	/**
	 * {@inheritDoc}
	 *
	 * @see java.sql.Connection#abort(java.util.concurrent.Executor)
	 */
	public void abort(Executor executor)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("");
	}


	/**
	 * {@inheritDoc}
	 *
	 * @see java.sql.Connection#setNetworkTimeout(java.util.concurrent.Executor, int)
	 */
	public void setNetworkTimeout(Executor executor, int milliseconds)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("");
	}


	/**
	 * {@inheritDoc}
	 *
	 * @see java.sql.Connection#getNetworkTimeout()
	 */
	public int getNetworkTimeout()
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("");
	}




    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
