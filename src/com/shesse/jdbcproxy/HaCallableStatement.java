/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;


/**
 *
 * @author sth
 */
public class HaCallableStatement
extends HaPreparedStatement
implements CallableStatement
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    //private static Logger log = Logger.getLogger(HaCallableStatement.class.getName());

    /** */
    private CallableStatement base;
    

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @param callableStatement 
     * @param haConnection 
     */
    public HaCallableStatement(HaConnection haConnection, CallableStatement base)
    {
	super(haConnection, base);
	
	this.base = base;
    }


    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////

    /**
     * @param parameterIndex
     * @param sqlType
     * @throws SQLException
     * @see java.sql.CallableStatement#registerOutParameter(int, int)
     */
    public void registerOutParameter(int parameterIndex, int sqlType)
	throws SQLException
    {
	base.registerOutParameter(parameterIndex, sqlType);
    }


    /**
     * @param parameterIndex
     * @param sqlType
     * @param scale
     * @throws SQLException
     * @see java.sql.CallableStatement#registerOutParameter(int, int, int)
     */
    public void registerOutParameter(int parameterIndex, int sqlType, int scale)
	throws SQLException
    {
	base.registerOutParameter(parameterIndex, sqlType, scale);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getString(int)
     */
    public String getString(int parameterIndex)
	throws SQLException
    {
	return base.getString(parameterIndex);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getBoolean(int)
     */
    public boolean getBoolean(int parameterIndex)
	throws SQLException
    {
	return base.getBoolean(parameterIndex);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getByte(int)
     */
    public byte getByte(int parameterIndex)
	throws SQLException
    {
	return base.getByte(parameterIndex);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getShort(int)
     */
    public short getShort(int parameterIndex)
	throws SQLException
    {
	return base.getShort(parameterIndex);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getInt(int)
     */
    public int getInt(int parameterIndex)
	throws SQLException
    {
	return base.getInt(parameterIndex);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getLong(int)
     */
    public long getLong(int parameterIndex)
	throws SQLException
    {
	return base.getLong(parameterIndex);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getFloat(int)
     */
    public float getFloat(int parameterIndex)
	throws SQLException
    {
	return base.getFloat(parameterIndex);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getDouble(int)
     */
    public double getDouble(int parameterIndex)
	throws SQLException
    {
	return base.getDouble(parameterIndex);
    }


    /**
     * @param parameterIndex
     * @param scale
     * @return
     * @throws SQLException
     * @deprecated
     * @see java.sql.CallableStatement#getBigDecimal(int, int)
     */
    public BigDecimal getBigDecimal(int parameterIndex, int scale)
	throws SQLException
    {
	return base.getBigDecimal(parameterIndex, scale);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getBytes(int)
     */
    public byte[] getBytes(int parameterIndex)
	throws SQLException
    {
	return base.getBytes(parameterIndex);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getDate(int)
     */
    public Date getDate(int parameterIndex)
	throws SQLException
    {
	return base.getDate(parameterIndex);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getTime(int)
     */
    public Time getTime(int parameterIndex)
	throws SQLException
    {
	return base.getTime(parameterIndex);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getTimestamp(int)
     */
    public Timestamp getTimestamp(int parameterIndex)
	throws SQLException
    {
	return base.getTimestamp(parameterIndex);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getObject(int)
     */
    public Object getObject(int parameterIndex)
	throws SQLException
    {
	return base.getObject(parameterIndex);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getBigDecimal(int)
     */
    public BigDecimal getBigDecimal(int parameterIndex)
	throws SQLException
    {
	return base.getBigDecimal(parameterIndex);
    }


    /**
     * @param parameterIndex
     * @param map
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getObject(int, java.util.Map)
     */
    public Object getObject(int parameterIndex, Map<String, Class<?>> map)
	throws SQLException
    {
	return base.getObject(parameterIndex, map);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getRef(int)
     */
    public Ref getRef(int parameterIndex)
	throws SQLException
    {
	return new HaRef(haConnection, base.getRef(parameterIndex));
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getBlob(int)
     */
    public Blob getBlob(int parameterIndex)
	throws SQLException
    {
	return new HaBlob(haConnection, base.getBlob(parameterIndex));
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getClob(int)
     */
    public Clob getClob(int parameterIndex)
	throws SQLException
    {
	return new HaClob(haConnection, base.getClob(parameterIndex));
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getArray(int)
     */
    public Array getArray(int parameterIndex)
	throws SQLException
    {
	return new HaArray(haConnection, base.getArray(parameterIndex));
    }


    /**
     * @param parameterIndex
     * @param cal
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getDate(int, java.util.Calendar)
     */
    public Date getDate(int parameterIndex, Calendar cal)
	throws SQLException
    {
	return base.getDate(parameterIndex, cal);
    }


    /**
     * @param parameterIndex
     * @param cal
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getTime(int, java.util.Calendar)
     */
    public Time getTime(int parameterIndex, Calendar cal)
	throws SQLException
    {
	return base.getTime(parameterIndex, cal);
    }


    /**
     * @param parameterIndex
     * @param cal
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getTimestamp(int, java.util.Calendar)
     */
    public Timestamp getTimestamp(int parameterIndex, Calendar cal)
	throws SQLException
    {
	return base.getTimestamp(parameterIndex, cal);
    }


    /**
     * @param parameterIndex
     * @param sqlType
     * @param typeName
     * @throws SQLException
     * @see java.sql.CallableStatement#registerOutParameter(int, int, java.lang.String)
     */
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName)
	throws SQLException
    {
	base.registerOutParameter(parameterIndex, sqlType, typeName);
    }


    /**
     * @param parameterName
     * @param sqlType
     * @throws SQLException
     * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int)
     */
    public void registerOutParameter(String parameterName, int sqlType)
	throws SQLException
    {
	base.registerOutParameter(parameterName, sqlType);
    }


    /**
     * @param parameterName
     * @param sqlType
     * @param scale
     * @throws SQLException
     * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int, int)
     */
    public void registerOutParameter(String parameterName, int sqlType, int scale)
	throws SQLException
    {
	base.registerOutParameter(parameterName, sqlType, scale);
    }


    /**
     * @param parameterName
     * @param sqlType
     * @param typeName
     * @throws SQLException
     * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int, java.lang.String)
     */
    public void registerOutParameter(String parameterName, int sqlType, String typeName)
	throws SQLException
    {
	base.registerOutParameter(parameterName, sqlType, typeName);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getURL(int)
     */
    public URL getURL(int parameterIndex)
	throws SQLException
    {
	return base.getURL(parameterIndex);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getString(java.lang.String)
     */
    public String getString(String parameterName)
	throws SQLException
    {
	return base.getString(parameterName);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getBoolean(java.lang.String)
     */
    public boolean getBoolean(String parameterName)
	throws SQLException
    {
	return base.getBoolean(parameterName);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getByte(java.lang.String)
     */
    public byte getByte(String parameterName)
	throws SQLException
    {
	return base.getByte(parameterName);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getShort(java.lang.String)
     */
    public short getShort(String parameterName)
	throws SQLException
    {
	return base.getShort(parameterName);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getInt(java.lang.String)
     */
    public int getInt(String parameterName)
	throws SQLException
    {
	return base.getInt(parameterName);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getLong(java.lang.String)
     */
    public long getLong(String parameterName)
	throws SQLException
    {
	return base.getLong(parameterName);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getFloat(java.lang.String)
     */
    public float getFloat(String parameterName)
	throws SQLException
    {
	return base.getFloat(parameterName);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getDouble(java.lang.String)
     */
    public double getDouble(String parameterName)
	throws SQLException
    {
	return base.getDouble(parameterName);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getBytes(java.lang.String)
     */
    public byte[] getBytes(String parameterName)
	throws SQLException
    {
	return base.getBytes(parameterName);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getDate(java.lang.String)
     */
    public Date getDate(String parameterName)
	throws SQLException
    {
	return base.getDate(parameterName);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getTime(java.lang.String)
     */
    public Time getTime(String parameterName)
	throws SQLException
    {
	return base.getTime(parameterName);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getTimestamp(java.lang.String)
     */
    public Timestamp getTimestamp(String parameterName)
	throws SQLException
    {
	return base.getTimestamp(parameterName);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getObject(java.lang.String)
     */
    public Object getObject(String parameterName)
	throws SQLException
    {
	return base.getObject(parameterName);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getBigDecimal(java.lang.String)
     */
    public BigDecimal getBigDecimal(String parameterName)
	throws SQLException
    {
	return base.getBigDecimal(parameterName);
    }


    /**
     * @param parameterName
     * @param map
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getObject(java.lang.String, java.util.Map)
     */
    public Object getObject(String parameterName, Map<String, Class<?>> map)
	throws SQLException
    {
	return base.getObject(parameterName, map);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getRef(java.lang.String)
     */
    public Ref getRef(String parameterName)
	throws SQLException
    {
	return new HaRef(haConnection, base.getRef(parameterName));
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getBlob(java.lang.String)
     */
    public Blob getBlob(String parameterName)
	throws SQLException
    {
	return new HaBlob(haConnection, base.getBlob(parameterName));
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getClob(java.lang.String)
     */
    public Clob getClob(String parameterName)
	throws SQLException
    {
	return new HaClob(haConnection, base.getClob(parameterName));
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getArray(java.lang.String)
     */
    public Array getArray(String parameterName)
	throws SQLException
    {
	return new HaArray(haConnection, base.getArray(parameterName));
    }


    /**
     * @param parameterName
     * @param cal
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getDate(java.lang.String, java.util.Calendar)
     */
    public Date getDate(String parameterName, Calendar cal)
	throws SQLException
    {
	return base.getDate(parameterName, cal);
    }


    /**
     * @param parameterName
     * @param cal
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getTime(java.lang.String, java.util.Calendar)
     */
    public Time getTime(String parameterName, Calendar cal)
	throws SQLException
    {
	return base.getTime(parameterName, cal);
    }


    /**
     * @param parameterName
     * @param cal
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getTimestamp(java.lang.String, java.util.Calendar)
     */
    public Timestamp getTimestamp(String parameterName, Calendar cal)
	throws SQLException
    {
	return base.getTimestamp(parameterName, cal);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getURL(java.lang.String)
     */
    public URL getURL(String parameterName)
	throws SQLException
    {
	return base.getURL(parameterName);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getRowId(int)
     */
    public RowId getRowId(int parameterIndex)
	throws SQLException
    {
	return base.getRowId(parameterIndex);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getRowId(java.lang.String)
     */
    public RowId getRowId(String parameterName)
	throws SQLException
    {
	return base.getRowId(parameterName);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getNClob(int)
     */
    public NClob getNClob(int parameterIndex)
	throws SQLException
    {
	return new HaNClob(haConnection, base.getNClob(parameterIndex));
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getNClob(java.lang.String)
     */
    public NClob getNClob(String parameterName)
	throws SQLException
    {
	return new HaNClob(haConnection, base.getNClob(parameterName));
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getSQLXML(int)
     */
    public SQLXML getSQLXML(int parameterIndex)
	throws SQLException
    {
	return new HaSQLXML(haConnection, base.getSQLXML(parameterIndex));
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getSQLXML(java.lang.String)
     */
    public SQLXML getSQLXML(String parameterName)
	throws SQLException
    {
	return new HaSQLXML(haConnection, base.getSQLXML(parameterName));
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getNString(int)
     */
    public String getNString(int parameterIndex)
	throws SQLException
    {
	return base.getNString(parameterIndex);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getNString(java.lang.String)
     */
    public String getNString(String parameterName)
	throws SQLException
    {
	return base.getNString(parameterName);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getNCharacterStream(int)
     */
    public Reader getNCharacterStream(int parameterIndex)
	throws SQLException
    {
	return base.getNCharacterStream(parameterIndex);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getNCharacterStream(java.lang.String)
     */
    public Reader getNCharacterStream(String parameterName)
	throws SQLException
    {
	return base.getNCharacterStream(parameterName);
    }


    /**
     * @param parameterIndex
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getCharacterStream(int)
     */
    public Reader getCharacterStream(int parameterIndex)
	throws SQLException
    {
	return base.getCharacterStream(parameterIndex);
    }


    /**
     * @param parameterName
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#getCharacterStream(java.lang.String)
     */
    public Reader getCharacterStream(String parameterName)
	throws SQLException
    {
	return base.getCharacterStream(parameterName);
    }


    /**
     * @param parameterName
     * @param x
     * @param length
     * @throws SQLException
     * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream, int)
     */
    public void setAsciiStream(String parameterName, InputStream x, int length)
	throws SQLException
    {
	base.setAsciiStream(parameterName, x, length);
    }


    /**
     * @param parameterName
     * @param x
     * @param length
     * @throws SQLException
     * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream, long)
     */
    public void setAsciiStream(String parameterName, InputStream x, long length)
	throws SQLException
    {
	base.setAsciiStream(parameterName, x, length);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream)
     */
    public void setAsciiStream(String parameterName, InputStream x)
	throws SQLException
    {
	base.setAsciiStream(parameterName, x);
    }


    /**
     * @param parameterName
     * @param val
     * @throws SQLException
     * @see java.sql.CallableStatement#setURL(java.lang.String, java.net.URL)
     */
    public void setURL(String parameterName, URL val)
	throws SQLException
    {
	base.setURL(parameterName, val);
    }


    /**
     * @param parameterName
     * @param sqlType
     * @throws SQLException
     * @see java.sql.CallableStatement#setNull(java.lang.String, int)
     */
    public void setNull(String parameterName, int sqlType)
	throws SQLException
    {
	base.setNull(parameterName, sqlType);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setBoolean(java.lang.String, boolean)
     */
    public void setBoolean(String parameterName, boolean x)
	throws SQLException
    {
	base.setBoolean(parameterName, x);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setByte(java.lang.String, byte)
     */
    public void setByte(String parameterName, byte x)
	throws SQLException
    {
	base.setByte(parameterName, x);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setShort(java.lang.String, short)
     */
    public void setShort(String parameterName, short x)
	throws SQLException
    {
	base.setShort(parameterName, x);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setInt(java.lang.String, int)
     */
    public void setInt(String parameterName, int x)
	throws SQLException
    {
	base.setInt(parameterName, x);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setLong(java.lang.String, long)
     */
    public void setLong(String parameterName, long x)
	throws SQLException
    {
	base.setLong(parameterName, x);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setFloat(java.lang.String, float)
     */
    public void setFloat(String parameterName, float x)
	throws SQLException
    {
	base.setFloat(parameterName, x);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setDouble(java.lang.String, double)
     */
    public void setDouble(String parameterName, double x)
	throws SQLException
    {
	base.setDouble(parameterName, x);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setBigDecimal(java.lang.String, java.math.BigDecimal)
     */
    public void setBigDecimal(String parameterName, BigDecimal x)
	throws SQLException
    {
	base.setBigDecimal(parameterName, x);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setString(java.lang.String, java.lang.String)
     */
    public void setString(String parameterName, String x)
	throws SQLException
    {
	base.setString(parameterName, x);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setBytes(java.lang.String, byte[])
     */
    public void setBytes(String parameterName, byte[] x)
	throws SQLException
    {
	base.setBytes(parameterName, x);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date)
     */
    public void setDate(String parameterName, Date x)
	throws SQLException
    {
	base.setDate(parameterName, x);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setTimestamp(java.lang.String, java.sql.Timestamp)
     */
    public void setTimestamp(String parameterName, Timestamp x)
	throws SQLException
    {
	base.setTimestamp(parameterName, x);
    }


    /**
     * @param parameterName
     * @param x
     * @param length
     * @throws SQLException
     * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream, int)
     */
    public void setBinaryStream(String parameterName, InputStream x, int length)
	throws SQLException
    {
	base.setBinaryStream(parameterName, x, length);
    }


    /**
     * @param parameterName
     * @param x
     * @param targetSqlType
     * @param scale
     * @throws SQLException
     * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object, int, int)
     */
    public void setObject(String parameterName, Object x, int targetSqlType, int scale)
	throws SQLException
    {
	base.setObject(parameterName, x, targetSqlType, scale);
    }


    /**
     * @param parameterName
     * @param x
     * @param targetSqlType
     * @throws SQLException
     * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object, int)
     */
    public void setObject(String parameterName, Object x, int targetSqlType)
	throws SQLException
    {
	base.setObject(parameterName, x, targetSqlType);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object)
     */
    public void setObject(String parameterName, Object x)
	throws SQLException
    {
	base.setObject(parameterName, x);
    }


    /**
     * @param parameterName
     * @param reader
     * @param length
     * @throws SQLException
     * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader, int)
     */
    public void setCharacterStream(String parameterName, Reader reader, int length)
	throws SQLException
    {
	base.setCharacterStream(parameterName, reader, length);
    }


    /**
     * @param parameterName
     * @param x
     * @param cal
     * @throws SQLException
     * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date, java.util.Calendar)
     */
    public void setDate(String parameterName, Date x, Calendar cal)
	throws SQLException
    {
	base.setDate(parameterName, x, cal);
    }


    /**
     * @param parameterName
     * @param x
     * @param cal
     * @throws SQLException
     * @see java.sql.CallableStatement#setTimestamp(java.lang.String, java.sql.Timestamp, java.util.Calendar)
     */
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal)
	throws SQLException
    {
	base.setTimestamp(parameterName, x, cal);
    }


    /**
     * @param parameterName
     * @param sqlType
     * @param typeName
     * @throws SQLException
     * @see java.sql.CallableStatement#setNull(java.lang.String, int, java.lang.String)
     */
    public void setNull(String parameterName, int sqlType, String typeName)
	throws SQLException
    {
	base.setNull(parameterName, sqlType, typeName);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setRowId(java.lang.String, java.sql.RowId)
     */
    public void setRowId(String parameterName, RowId x)
	throws SQLException
    {
	base.setRowId(parameterName, x);
    }


    /**
     * @param parameterName
     * @param value
     * @throws SQLException
     * @see java.sql.CallableStatement#setNString(java.lang.String, java.lang.String)
     */
    public void setNString(String parameterName, String value)
	throws SQLException
    {
	base.setNString(parameterName, value);
    }


    /**
     * @param parameterName
     * @param value
     * @param length
     * @throws SQLException
     * @see java.sql.CallableStatement#setNCharacterStream(java.lang.String, java.io.Reader, long)
     */
    public void setNCharacterStream(String parameterName, Reader value, long length)
	throws SQLException
    {
	base.setNCharacterStream(parameterName, value, length);
    }


    /**
     * @param parameterName
     * @param value
     * @throws SQLException
     * @see java.sql.CallableStatement#setNClob(java.lang.String, java.sql.NClob)
     */
    public void setNClob(String parameterName, NClob value)
	throws SQLException
    {
	if (value instanceof HaNClob) {
	    base.setNClob(parameterName, ((HaNClob)value).getBase());
	} else {
	    base.setNClob(parameterName, value);
	}
    }


    /**
     * @param parameterName
     * @param reader
     * @param length
     * @throws SQLException
     * @see java.sql.CallableStatement#setClob(java.lang.String, java.io.Reader, long)
     */
    public void setClob(String parameterName, Reader reader, long length)
	throws SQLException
    {
	base.setClob(parameterName, reader, length);
    }


    /**
     * @param parameterName
     * @param inputStream
     * @param length
     * @throws SQLException
     * @see java.sql.CallableStatement#setBlob(java.lang.String, java.io.InputStream, long)
     */
    public void setBlob(String parameterName, InputStream inputStream, long length)
	throws SQLException
    {
	base.setBlob(parameterName, inputStream, length);
    }


    /**
     * @param parameterName
     * @param reader
     * @param length
     * @throws SQLException
     * @see java.sql.CallableStatement#setNClob(java.lang.String, java.io.Reader, long)
     */
    public void setNClob(String parameterName, Reader reader, long length)
	throws SQLException
    {
	base.setNClob(parameterName, reader, length);
    }


    /**
     * @param parameterName
     * @param xmlObject
     * @throws SQLException
     * @see java.sql.CallableStatement#setSQLXML(java.lang.String, java.sql.SQLXML)
     */
    public void setSQLXML(String parameterName, SQLXML value)
	throws SQLException
    {
	if (value instanceof HaSQLXML) {
	    base.setSQLXML(parameterName, ((HaSQLXML)value).getBase());
	} else {
	    base.setSQLXML(parameterName, value);
	}
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setBlob(java.lang.String, java.sql.Blob)
     */
    public void setBlob(String parameterName, Blob value)
	throws SQLException
    {
	if (value instanceof HaBlob) {
	    base.setBlob(parameterName, ((HaBlob)value).getBase());
	} else {
	    base.setBlob(parameterName, value);
	}
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setClob(java.lang.String, java.sql.Clob)
     */
    public void setClob(String parameterName, Clob value)
	throws SQLException
    {
	if (value instanceof HaClob) {
	    base.setClob(parameterName, ((HaClob)value).getBase());
	} else {
	    base.setClob(parameterName, value);
	}
    }


    /**
     * @param parameterName
     * @param x
     * @param length
     * @throws SQLException
     * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream, long)
     */
    public void setBinaryStream(String parameterName, InputStream x, long length)
	throws SQLException
    {
	base.setBinaryStream(parameterName, x, length);
    }


    /**
     * @param parameterName
     * @param reader
     * @param length
     * @throws SQLException
     * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader, long)
     */
    public void setCharacterStream(String parameterName, Reader reader, long length)
	throws SQLException
    {
	base.setCharacterStream(parameterName, reader, length);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream)
     */
    public void setBinaryStream(String parameterName, InputStream x)
	throws SQLException
    {
	base.setBinaryStream(parameterName, x);
    }


    /**
     * @param parameterName
     * @param reader
     * @throws SQLException
     * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader)
     */
    public void setCharacterStream(String parameterName, Reader reader)
	throws SQLException
    {
	base.setCharacterStream(parameterName, reader);
    }


    /**
     * @param parameterName
     * @param value
     * @throws SQLException
     * @see java.sql.CallableStatement#setNCharacterStream(java.lang.String, java.io.Reader)
     */
    public void setNCharacterStream(String parameterName, Reader value)
	throws SQLException
    {
	base.setNCharacterStream(parameterName, value);
    }


    /**
     * @param parameterName
     * @param x
     * @throws SQLException
     * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time)
     */
    public void setTime(String parameterName, Time x)
	throws SQLException
    {
	base.setTime(parameterName, x);
    }


    /**
     * @param parameterName
     * @param x
     * @param cal
     * @throws SQLException
     * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time, java.util.Calendar)
     */
    public void setTime(String parameterName, Time x, Calendar cal)
	throws SQLException
    {
	base.setTime(parameterName, x, cal);
    }


    /**
     * @param parameterName
     * @param reader
     * @throws SQLException
     * @see java.sql.CallableStatement#setClob(java.lang.String, java.io.Reader)
     */
    public void setClob(String parameterName, Reader reader)
	throws SQLException
    {
	base.setClob(parameterName, reader);
    }


    /**
     * @param parameterName
     * @param inputStream
     * @throws SQLException
     * @see java.sql.CallableStatement#setBlob(java.lang.String, java.io.InputStream)
     */
    public void setBlob(String parameterName, InputStream inputStream)
	throws SQLException
    {
	base.setBlob(parameterName, inputStream);
    }


    /**
     * @param parameterName
     * @param reader
     * @throws SQLException
     * @see java.sql.CallableStatement#setNClob(java.lang.String, java.io.Reader)
     */
    public void setNClob(String parameterName, Reader reader)
	throws SQLException
    {
	base.setNClob(parameterName, reader);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.CallableStatement#wasNull()
     */
    public boolean wasNull()
	throws SQLException
    {
	return base.wasNull();
    }




    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
