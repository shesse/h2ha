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
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 *
 * @author sth
 */
public class HaResultSet
    implements ResultSet
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(HaResultSet.class);

    /** */
    private HaConnection haConnection;
    
    /** */
    private HaStatement haStatement;
    
    /** */
    private ResultSet base;
    

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @param resultSet 
     * @param haConnection 
     */
    public HaResultSet(HaConnection haConnection, HaStatement haStatement, ResultSet base)
    {
	log.debug("HaResultSet()");
	this.haConnection = haConnection;
	this.haStatement = haStatement;
	this.base = base;
    }


    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////

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
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#next()
     */
    public boolean next()
	throws SQLException
    {
	return base.next();
    }


    /**
     * @throws SQLException
     * @see java.sql.ResultSet#close()
     */
    public void close()
	throws SQLException
    {
	base.close();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#wasNull()
     */
    public boolean wasNull()
	throws SQLException
    {
	return base.wasNull();
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getString(int)
     */
    public String getString(int columnIndex)
	throws SQLException
    {
	return base.getString(columnIndex);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getBoolean(int)
     */
    public boolean getBoolean(int columnIndex)
	throws SQLException
    {
	return base.getBoolean(columnIndex);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getByte(int)
     */
    public byte getByte(int columnIndex)
	throws SQLException
    {
	return base.getByte(columnIndex);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getShort(int)
     */
    public short getShort(int columnIndex)
	throws SQLException
    {
	return base.getShort(columnIndex);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getInt(int)
     */
    public int getInt(int columnIndex)
	throws SQLException
    {
	return base.getInt(columnIndex);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getLong(int)
     */
    public long getLong(int columnIndex)
	throws SQLException
    {
	return base.getLong(columnIndex);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getFloat(int)
     */
    public float getFloat(int columnIndex)
	throws SQLException
    {
	return base.getFloat(columnIndex);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getDouble(int)
     */
    public double getDouble(int columnIndex)
	throws SQLException
    {
	return base.getDouble(columnIndex);
    }


    /**
     * @param columnIndex
     * @param scale
     * @return
     * @throws SQLException
     * @deprecated
     * @see java.sql.ResultSet#getBigDecimal(int, int)
     */
    public BigDecimal getBigDecimal(int columnIndex, int scale)
	throws SQLException
    {
	return base.getBigDecimal(columnIndex, scale);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getBytes(int)
     */
    public byte[] getBytes(int columnIndex)
	throws SQLException
    {
	return base.getBytes(columnIndex);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getDate(int)
     */
    public Date getDate(int columnIndex)
	throws SQLException
    {
	return base.getDate(columnIndex);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getTime(int)
     */
    public Time getTime(int columnIndex)
	throws SQLException
    {
	return base.getTime(columnIndex);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getTimestamp(int)
     */
    public Timestamp getTimestamp(int columnIndex)
	throws SQLException
    {
	return base.getTimestamp(columnIndex);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getAsciiStream(int)
     */
    public InputStream getAsciiStream(int columnIndex)
	throws SQLException
    {
	return base.getAsciiStream(columnIndex);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @deprecated
     * @see java.sql.ResultSet#getUnicodeStream(int)
     */
    public InputStream getUnicodeStream(int columnIndex)
	throws SQLException
    {
	return base.getUnicodeStream(columnIndex);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getBinaryStream(int)
     */
    public InputStream getBinaryStream(int columnIndex)
	throws SQLException
    {
	return base.getBinaryStream(columnIndex);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getString(java.lang.String)
     */
    public String getString(String columnLabel)
	throws SQLException
    {
	return base.getString(columnLabel);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getBoolean(java.lang.String)
     */
    public boolean getBoolean(String columnLabel)
	throws SQLException
    {
	return base.getBoolean(columnLabel);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getByte(java.lang.String)
     */
    public byte getByte(String columnLabel)
	throws SQLException
    {
	return base.getByte(columnLabel);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getShort(java.lang.String)
     */
    public short getShort(String columnLabel)
	throws SQLException
    {
	return base.getShort(columnLabel);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getInt(java.lang.String)
     */
    public int getInt(String columnLabel)
	throws SQLException
    {
	return base.getInt(columnLabel);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getLong(java.lang.String)
     */
    public long getLong(String columnLabel)
	throws SQLException
    {
	return base.getLong(columnLabel);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getFloat(java.lang.String)
     */
    public float getFloat(String columnLabel)
	throws SQLException
    {
	return base.getFloat(columnLabel);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getDouble(java.lang.String)
     */
    public double getDouble(String columnLabel)
	throws SQLException
    {
	return base.getDouble(columnLabel);
    }


    /**
     * @param columnLabel
     * @param scale
     * @return
     * @throws SQLException
     * @deprecated
     * @see java.sql.ResultSet#getBigDecimal(java.lang.String, int)
     */
    public BigDecimal getBigDecimal(String columnLabel, int scale)
	throws SQLException
    {
	return base.getBigDecimal(columnLabel, scale);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getBytes(java.lang.String)
     */
    public byte[] getBytes(String columnLabel)
	throws SQLException
    {
	return base.getBytes(columnLabel);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getDate(java.lang.String)
     */
    public Date getDate(String columnLabel)
	throws SQLException
    {
	return base.getDate(columnLabel);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getTime(java.lang.String)
     */
    public Time getTime(String columnLabel)
	throws SQLException
    {
	return base.getTime(columnLabel);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getTimestamp(java.lang.String)
     */
    public Timestamp getTimestamp(String columnLabel)
	throws SQLException
    {
	return base.getTimestamp(columnLabel);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getAsciiStream(java.lang.String)
     */
    public InputStream getAsciiStream(String columnLabel)
	throws SQLException
    {
	return base.getAsciiStream(columnLabel);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @deprecated
     * @see java.sql.ResultSet#getUnicodeStream(java.lang.String)
     */
    public InputStream getUnicodeStream(String columnLabel)
	throws SQLException
    {
	return base.getUnicodeStream(columnLabel);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getBinaryStream(java.lang.String)
     */
    public InputStream getBinaryStream(String columnLabel)
	throws SQLException
    {
	return base.getBinaryStream(columnLabel);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getWarnings()
     */
    public SQLWarning getWarnings()
	throws SQLException
    {
	return base.getWarnings();
    }


    /**
     * @throws SQLException
     * @see java.sql.ResultSet#clearWarnings()
     */
    public void clearWarnings()
	throws SQLException
    {
	base.clearWarnings();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getCursorName()
     */
    public String getCursorName()
	throws SQLException
    {
	return base.getCursorName();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getMetaData()
     */
    public ResultSetMetaData getMetaData()
	throws SQLException
    {
	return new HaResultSetMetaData(haConnection, base.getMetaData());
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getObject(int)
     */
    public Object getObject(int columnIndex)
	throws SQLException
    {
	return base.getObject(columnIndex);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getObject(java.lang.String)
     */
    public Object getObject(String columnLabel)
	throws SQLException
    {
	return base.getObject(columnLabel);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#findColumn(java.lang.String)
     */
    public int findColumn(String columnLabel)
	throws SQLException
    {
	return base.findColumn(columnLabel);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getCharacterStream(int)
     */
    public Reader getCharacterStream(int columnIndex)
	throws SQLException
    {
	return base.getCharacterStream(columnIndex);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getCharacterStream(java.lang.String)
     */
    public Reader getCharacterStream(String columnLabel)
	throws SQLException
    {
	return base.getCharacterStream(columnLabel);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getBigDecimal(int)
     */
    public BigDecimal getBigDecimal(int columnIndex)
	throws SQLException
    {
	return base.getBigDecimal(columnIndex);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getBigDecimal(java.lang.String)
     */
    public BigDecimal getBigDecimal(String columnLabel)
	throws SQLException
    {
	return base.getBigDecimal(columnLabel);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#isBeforeFirst()
     */
    public boolean isBeforeFirst()
	throws SQLException
    {
	return base.isBeforeFirst();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#isAfterLast()
     */
    public boolean isAfterLast()
	throws SQLException
    {
	return base.isAfterLast();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#isFirst()
     */
    public boolean isFirst()
	throws SQLException
    {
	return base.isFirst();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#isLast()
     */
    public boolean isLast()
	throws SQLException
    {
	return base.isLast();
    }


    /**
     * @throws SQLException
     * @see java.sql.ResultSet#beforeFirst()
     */
    public void beforeFirst()
	throws SQLException
    {
	base.beforeFirst();
    }


    /**
     * @throws SQLException
     * @see java.sql.ResultSet#afterLast()
     */
    public void afterLast()
	throws SQLException
    {
	base.afterLast();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#first()
     */
    public boolean first()
	throws SQLException
    {
	return base.first();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#last()
     */
    public boolean last()
	throws SQLException
    {
	return base.last();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getRow()
     */
    public int getRow()
	throws SQLException
    {
	return base.getRow();
    }


    /**
     * @param row
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#absolute(int)
     */
    public boolean absolute(int row)
	throws SQLException
    {
	return base.absolute(row);
    }


    /**
     * @param rows
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#relative(int)
     */
    public boolean relative(int rows)
	throws SQLException
    {
	return base.relative(rows);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#previous()
     */
    public boolean previous()
	throws SQLException
    {
	return base.previous();
    }


    /**
     * @param direction
     * @throws SQLException
     * @see java.sql.ResultSet#setFetchDirection(int)
     */
    public void setFetchDirection(int direction)
	throws SQLException
    {
	base.setFetchDirection(direction);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getFetchDirection()
     */
    public int getFetchDirection()
	throws SQLException
    {
	return base.getFetchDirection();
    }


    /**
     * @param rows
     * @throws SQLException
     * @see java.sql.ResultSet#setFetchSize(int)
     */
    public void setFetchSize(int rows)
	throws SQLException
    {
	base.setFetchSize(rows);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getFetchSize()
     */
    public int getFetchSize()
	throws SQLException
    {
	return base.getFetchSize();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getType()
     */
    public int getType()
	throws SQLException
    {
	return base.getType();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getConcurrency()
     */
    public int getConcurrency()
	throws SQLException
    {
	return base.getConcurrency();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#rowUpdated()
     */
    public boolean rowUpdated()
	throws SQLException
    {
	return base.rowUpdated();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#rowInserted()
     */
    public boolean rowInserted()
	throws SQLException
    {
	return base.rowInserted();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#rowDeleted()
     */
    public boolean rowDeleted()
	throws SQLException
    {
	return base.rowDeleted();
    }


    /**
     * @param columnIndex
     * @throws SQLException
     * @see java.sql.ResultSet#updateNull(int)
     */
    public void updateNull(int columnIndex)
	throws SQLException
    {
	base.updateNull(columnIndex);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateBoolean(int, boolean)
     */
    public void updateBoolean(int columnIndex, boolean x)
	throws SQLException
    {
	base.updateBoolean(columnIndex, x);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateByte(int, byte)
     */
    public void updateByte(int columnIndex, byte x)
	throws SQLException
    {
	base.updateByte(columnIndex, x);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateShort(int, short)
     */
    public void updateShort(int columnIndex, short x)
	throws SQLException
    {
	base.updateShort(columnIndex, x);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateInt(int, int)
     */
    public void updateInt(int columnIndex, int x)
	throws SQLException
    {
	base.updateInt(columnIndex, x);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateLong(int, long)
     */
    public void updateLong(int columnIndex, long x)
	throws SQLException
    {
	base.updateLong(columnIndex, x);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateFloat(int, float)
     */
    public void updateFloat(int columnIndex, float x)
	throws SQLException
    {
	base.updateFloat(columnIndex, x);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateDouble(int, double)
     */
    public void updateDouble(int columnIndex, double x)
	throws SQLException
    {
	base.updateDouble(columnIndex, x);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateBigDecimal(int, java.math.BigDecimal)
     */
    public void updateBigDecimal(int columnIndex, BigDecimal x)
	throws SQLException
    {
	base.updateBigDecimal(columnIndex, x);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateString(int, java.lang.String)
     */
    public void updateString(int columnIndex, String x)
	throws SQLException
    {
	base.updateString(columnIndex, x);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateBytes(int, byte[])
     */
    public void updateBytes(int columnIndex, byte[] x)
	throws SQLException
    {
	base.updateBytes(columnIndex, x);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateDate(int, java.sql.Date)
     */
    public void updateDate(int columnIndex, Date x)
	throws SQLException
    {
	base.updateDate(columnIndex, x);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateTime(int, java.sql.Time)
     */
    public void updateTime(int columnIndex, Time x)
	throws SQLException
    {
	base.updateTime(columnIndex, x);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateTimestamp(int, java.sql.Timestamp)
     */
    public void updateTimestamp(int columnIndex, Timestamp x)
	throws SQLException
    {
	base.updateTimestamp(columnIndex, x);
    }


    /**
     * @param columnIndex
     * @param x
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, int)
     */
    public void updateAsciiStream(int columnIndex, InputStream x, int length)
	throws SQLException
    {
	base.updateAsciiStream(columnIndex, x, length);
    }


    /**
     * @param columnIndex
     * @param x
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, int)
     */
    public void updateBinaryStream(int columnIndex, InputStream x, int length)
	throws SQLException
    {
	base.updateBinaryStream(columnIndex, x, length);
    }


    /**
     * @param columnIndex
     * @param x
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, int)
     */
    public void updateCharacterStream(int columnIndex, Reader x, int length)
	throws SQLException
    {
	base.updateCharacterStream(columnIndex, x, length);
    }


    /**
     * @param columnIndex
     * @param x
     * @param scaleOrLength
     * @throws SQLException
     * @see java.sql.ResultSet#updateObject(int, java.lang.Object, int)
     */
    public void updateObject(int columnIndex, Object x, int scaleOrLength)
	throws SQLException
    {
	base.updateObject(columnIndex, x, scaleOrLength);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateObject(int, java.lang.Object)
     */
    public void updateObject(int columnIndex, Object x)
	throws SQLException
    {
	base.updateObject(columnIndex, x);
    }


    /**
     * @param columnLabel
     * @throws SQLException
     * @see java.sql.ResultSet#updateNull(java.lang.String)
     */
    public void updateNull(String columnLabel)
	throws SQLException
    {
	base.updateNull(columnLabel);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateBoolean(java.lang.String, boolean)
     */
    public void updateBoolean(String columnLabel, boolean x)
	throws SQLException
    {
	base.updateBoolean(columnLabel, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateByte(java.lang.String, byte)
     */
    public void updateByte(String columnLabel, byte x)
	throws SQLException
    {
	base.updateByte(columnLabel, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateShort(java.lang.String, short)
     */
    public void updateShort(String columnLabel, short x)
	throws SQLException
    {
	base.updateShort(columnLabel, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateInt(java.lang.String, int)
     */
    public void updateInt(String columnLabel, int x)
	throws SQLException
    {
	base.updateInt(columnLabel, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateLong(java.lang.String, long)
     */
    public void updateLong(String columnLabel, long x)
	throws SQLException
    {
	base.updateLong(columnLabel, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateFloat(java.lang.String, float)
     */
    public void updateFloat(String columnLabel, float x)
	throws SQLException
    {
	base.updateFloat(columnLabel, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateDouble(java.lang.String, double)
     */
    public void updateDouble(String columnLabel, double x)
	throws SQLException
    {
	base.updateDouble(columnLabel, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateBigDecimal(java.lang.String, java.math.BigDecimal)
     */
    public void updateBigDecimal(String columnLabel, BigDecimal x)
	throws SQLException
    {
	base.updateBigDecimal(columnLabel, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateString(java.lang.String, java.lang.String)
     */
    public void updateString(String columnLabel, String x)
	throws SQLException
    {
	base.updateString(columnLabel, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateBytes(java.lang.String, byte[])
     */
    public void updateBytes(String columnLabel, byte[] x)
	throws SQLException
    {
	base.updateBytes(columnLabel, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateDate(java.lang.String, java.sql.Date)
     */
    public void updateDate(String columnLabel, Date x)
	throws SQLException
    {
	base.updateDate(columnLabel, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateTime(java.lang.String, java.sql.Time)
     */
    public void updateTime(String columnLabel, Time x)
	throws SQLException
    {
	base.updateTime(columnLabel, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateTimestamp(java.lang.String, java.sql.Timestamp)
     */
    public void updateTimestamp(String columnLabel, Timestamp x)
	throws SQLException
    {
	base.updateTimestamp(columnLabel, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream, int)
     */
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
	throws SQLException
    {
	base.updateAsciiStream(columnLabel, x, length);
    }


    /**
     * @param columnLabel
     * @param x
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream, int)
     */
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
	throws SQLException
    {
	base.updateBinaryStream(columnLabel, x, length);
    }


    /**
     * @param columnLabel
     * @param reader
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader, int)
     */
    public void updateCharacterStream(String columnLabel, Reader reader, int length)
	throws SQLException
    {
	base.updateCharacterStream(columnLabel, reader, length);
    }


    /**
     * @param columnLabel
     * @param x
     * @param scaleOrLength
     * @throws SQLException
     * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object, int)
     */
    public void updateObject(String columnLabel, Object x, int scaleOrLength)
	throws SQLException
    {
	base.updateObject(columnLabel, x, scaleOrLength);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object)
     */
    public void updateObject(String columnLabel, Object x)
	throws SQLException
    {
	base.updateObject(columnLabel, x);
    }


    /**
     * @throws SQLException
     * @see java.sql.ResultSet#insertRow()
     */
    public void insertRow()
	throws SQLException
    {
	base.insertRow();
    }


    /**
     * @throws SQLException
     * @see java.sql.ResultSet#updateRow()
     */
    public void updateRow()
	throws SQLException
    {
	base.updateRow();
    }


    /**
     * @throws SQLException
     * @see java.sql.ResultSet#deleteRow()
     */
    public void deleteRow()
	throws SQLException
    {
	base.deleteRow();
    }


    /**
     * @throws SQLException
     * @see java.sql.ResultSet#refreshRow()
     */
    public void refreshRow()
	throws SQLException
    {
	base.refreshRow();
    }


    /**
     * @throws SQLException
     * @see java.sql.ResultSet#cancelRowUpdates()
     */
    public void cancelRowUpdates()
	throws SQLException
    {
	base.cancelRowUpdates();
    }


    /**
     * @throws SQLException
     * @see java.sql.ResultSet#moveToInsertRow()
     */
    public void moveToInsertRow()
	throws SQLException
    {
	base.moveToInsertRow();
    }


    /**
     * @throws SQLException
     * @see java.sql.ResultSet#moveToCurrentRow()
     */
    public void moveToCurrentRow()
	throws SQLException
    {
	base.moveToCurrentRow();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getStatement()
     */
    public Statement getStatement()
	throws SQLException
    {
	Statement stmnt = base.getStatement();
	if (stmnt == null) return null;
	
	if (haStatement == null) {
	    return new HaStatement(haConnection, stmnt);
	} else {
	    return haStatement;
	}
    }


    /**
     * @param columnIndex
     * @param map
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getObject(int, java.util.Map)
     */
    public Object getObject(int columnIndex, Map<String, Class<?>> map)
	throws SQLException
    {
	return base.getObject(columnIndex, map);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getRef(int)
     */
    public Ref getRef(int columnIndex)
	throws SQLException
    {
	return new HaRef(haConnection, base.getRef(columnIndex));
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getBlob(int)
     */
    public Blob getBlob(int columnIndex)
	throws SQLException
    {
	return new HaBlob(haConnection, base.getBlob(columnIndex));
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getClob(int)
     */
    public Clob getClob(int columnIndex)
	throws SQLException
    {
	return new HaClob(haConnection, base.getClob(columnIndex));
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getArray(int)
     */
    public Array getArray(int columnIndex)
	throws SQLException
    {
	return new HaArray(haConnection, base.getArray(columnIndex));
    }


    /**
     * @param columnLabel
     * @param map
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getObject(java.lang.String, java.util.Map)
     */
    public Object getObject(String columnLabel, Map<String, Class<?>> map)
	throws SQLException
    {
	return base.getObject(columnLabel, map);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getRef(java.lang.String)
     */
    public HaRef getRef(String columnLabel)
	throws SQLException
    {
	return new HaRef(haConnection, base.getRef(columnLabel));
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getBlob(java.lang.String)
     */
    public Blob getBlob(String columnLabel)
	throws SQLException
    {
	return new HaBlob(haConnection, base.getBlob(columnLabel));
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getClob(java.lang.String)
     */
    public Clob getClob(String columnLabel)
	throws SQLException
    {
	return new HaClob(haConnection, base.getClob(columnLabel));
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getArray(java.lang.String)
     */
    public Array getArray(String columnLabel)
	throws SQLException
    {
	return new HaArray(haConnection, base.getArray(columnLabel));
    }


    /**
     * @param columnIndex
     * @param cal
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getDate(int, java.util.Calendar)
     */
    public Date getDate(int columnIndex, Calendar cal)
	throws SQLException
    {
	return base.getDate(columnIndex, cal);
    }


    /**
     * @param columnLabel
     * @param cal
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getDate(java.lang.String, java.util.Calendar)
     */
    public Date getDate(String columnLabel, Calendar cal)
	throws SQLException
    {
	return base.getDate(columnLabel, cal);
    }


    /**
     * @param columnIndex
     * @param cal
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getTime(int, java.util.Calendar)
     */
    public Time getTime(int columnIndex, Calendar cal)
	throws SQLException
    {
	return base.getTime(columnIndex, cal);
    }


    /**
     * @param columnLabel
     * @param cal
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getTime(java.lang.String, java.util.Calendar)
     */
    public Time getTime(String columnLabel, Calendar cal)
	throws SQLException
    {
	return base.getTime(columnLabel, cal);
    }


    /**
     * @param columnIndex
     * @param cal
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getTimestamp(int, java.util.Calendar)
     */
    public Timestamp getTimestamp(int columnIndex, Calendar cal)
	throws SQLException
    {
	return base.getTimestamp(columnIndex, cal);
    }


    /**
     * @param columnLabel
     * @param cal
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getTimestamp(java.lang.String, java.util.Calendar)
     */
    public Timestamp getTimestamp(String columnLabel, Calendar cal)
	throws SQLException
    {
	return base.getTimestamp(columnLabel, cal);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getURL(int)
     */
    public URL getURL(int columnIndex)
	throws SQLException
    {
	return base.getURL(columnIndex);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getURL(java.lang.String)
     */
    public URL getURL(String columnLabel)
	throws SQLException
    {
	return base.getURL(columnLabel);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateRef(int, java.sql.Ref)
     */
    public void updateRef(int columnIndex, Ref x)
	throws SQLException
    {
	if (x instanceof HaRef) {
	    base.updateRef(columnIndex, ((HaRef)x).getBase());
	} else {
	    base.updateRef(columnIndex, x);
	}
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateRef(java.lang.String, java.sql.Ref)
     */
    public void updateRef(String columnLabel, Ref x)
	throws SQLException
    {
	if (x instanceof HaRef) {
	    base.updateRef(columnLabel, ((HaRef)x).getBase());
	} else {
	    base.updateRef(columnLabel, x);
	}
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateBlob(int, java.sql.Blob)
     */
    public void updateBlob(int columnIndex, Blob x)
	throws SQLException
    {
	if (x instanceof HaBlob) {
	    base.updateBlob(columnIndex, ((HaBlob)x).getBase());
	} else {
	    base.updateBlob(columnIndex, x);
	}
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateBlob(java.lang.String, java.sql.Blob)
     */
    public void updateBlob(String columnLabel, Blob x)
	throws SQLException
    {
	if (x instanceof HaBlob) {
	    base.updateBlob(columnLabel, ((HaBlob)x).getBase());
	} else {
	    base.updateBlob(columnLabel, x);
	}
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateClob(int, java.sql.Clob)
     */
    public void updateClob(int columnIndex, Clob x)
	throws SQLException
    {
	if (x instanceof HaClob) {
	    base.updateClob(columnIndex, ((HaClob)x).getBase());
	} else {
	    base.updateClob(columnIndex, x);
	}
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateClob(java.lang.String, java.sql.Clob)
     */
    public void updateClob(String columnLabel, Clob x)
	throws SQLException
    {
	if (x instanceof HaClob) {
	    base.updateClob(columnLabel, ((HaClob)x).getBase());
	} else {
	    base.updateClob(columnLabel, x);
	}
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateArray(int, java.sql.Array)
     */
    public void updateArray(int columnIndex, Array x)
	throws SQLException
    {
	if (x instanceof HaArray) {
	    base.updateArray(columnIndex, ((HaArray)x).getBase());
	} else {
	    base.updateArray(columnIndex, x);
	}
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateArray(java.lang.String, java.sql.Array)
     */
    public void updateArray(String columnLabel, Array x)
	throws SQLException
    {
	if (x instanceof HaArray) {
	    base.updateArray(columnLabel, ((HaArray)x).getBase());
	} else {
	    base.updateArray(columnLabel, x);
	}
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getRowId(int)
     */
    public RowId getRowId(int columnIndex)
	throws SQLException
    {
	return base.getRowId(columnIndex);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getRowId(java.lang.String)
     */
    public RowId getRowId(String columnLabel)
	throws SQLException
    {
	return base.getRowId(columnLabel);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateRowId(int, java.sql.RowId)
     */
    public void updateRowId(int columnIndex, RowId x)
	throws SQLException
    {
	base.updateRowId(columnIndex, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateRowId(java.lang.String, java.sql.RowId)
     */
    public void updateRowId(String columnLabel, RowId x)
	throws SQLException
    {
	base.updateRowId(columnLabel, x);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getHoldability()
     */
    public int getHoldability()
	throws SQLException
    {
	return base.getHoldability();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#isClosed()
     */
    public boolean isClosed()
	throws SQLException
    {
	return base.isClosed();
    }


    /**
     * @param columnIndex
     * @param nString
     * @throws SQLException
     * @see java.sql.ResultSet#updateNString(int, java.lang.String)
     */
    public void updateNString(int columnIndex, String nString)
	throws SQLException
    {
	base.updateNString(columnIndex, nString);
    }


    /**
     * @param columnLabel
     * @param nString
     * @throws SQLException
     * @see java.sql.ResultSet#updateNString(java.lang.String, java.lang.String)
     */
    public void updateNString(String columnLabel, String nString)
	throws SQLException
    {
	base.updateNString(columnLabel, nString);
    }


    /**
     * @param columnIndex
     * @param nClob
     * @throws SQLException
     * @see java.sql.ResultSet#updateNClob(int, java.sql.NClob)
     */
    public void updateNClob(int columnIndex, NClob x)
	throws SQLException
    {
	if (x instanceof HaNClob) {
	    base.updateNClob(columnIndex, ((HaNClob)x).getBase());
	} else {
	    base.updateNClob(columnIndex, x);
	}
    }


    /**
     * @param columnLabel
     * @param nClob
     * @throws SQLException
     * @see java.sql.ResultSet#updateNClob(java.lang.String, java.sql.NClob)
     */
    public void updateNClob(String columnLabel, NClob x)
	throws SQLException
    {
	if (x instanceof HaNClob) {
	    base.updateNClob(columnLabel, ((HaNClob)x).getBase());
	} else {
	    base.updateNClob(columnLabel, x);
	}
    }

    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getNClob(int)
     */
    public NClob getNClob(int columnIndex)
	throws SQLException
    {
	return new HaNClob(haConnection, base.getNClob(columnIndex));
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getNClob(java.lang.String)
     */
    public NClob getNClob(String columnLabel)
	throws SQLException
    {
	return new HaNClob(haConnection, base.getNClob(columnLabel));
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getSQLXML(int)
     */
    public SQLXML getSQLXML(int columnIndex)
	throws SQLException
    {
	return new HaSQLXML(haConnection, base.getSQLXML(columnIndex));
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getSQLXML(java.lang.String)
     */
    public SQLXML getSQLXML(String columnLabel)
	throws SQLException
    {
	return new HaSQLXML(haConnection, base.getSQLXML(columnLabel));
    }


    /**
     * @param columnIndex
     * @param xmlObject
     * @throws SQLException
     * @see java.sql.ResultSet#updateSQLXML(int, java.sql.SQLXML)
     */
    public void updateSQLXML(int columnIndex, SQLXML x)
	throws SQLException
    {
	if (x instanceof HaSQLXML) {
	    base.updateSQLXML(columnIndex, ((HaSQLXML)x).getBase());
	} else {
	    base.updateSQLXML(columnIndex, x);
	}
    }


    /**
     * @param columnLabel
     * @param xmlObject
     * @throws SQLException
     * @see java.sql.ResultSet#updateSQLXML(java.lang.String, java.sql.SQLXML)
     */
    public void updateSQLXML(String columnLabel, SQLXML x)
	throws SQLException
    {
	if (x instanceof HaSQLXML) {
	    base.updateSQLXML(columnLabel, ((HaSQLXML)x).getBase());
	} else {
	    base.updateSQLXML(columnLabel, x);
	}
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getNString(int)
     */
    public String getNString(int columnIndex)
	throws SQLException
    {
	return base.getNString(columnIndex);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getNString(java.lang.String)
     */
    public String getNString(String columnLabel)
	throws SQLException
    {
	return base.getNString(columnLabel);
    }


    /**
     * @param columnIndex
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getNCharacterStream(int)
     */
    public Reader getNCharacterStream(int columnIndex)
	throws SQLException
    {
	return base.getNCharacterStream(columnIndex);
    }


    /**
     * @param columnLabel
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#getNCharacterStream(java.lang.String)
     */
    public Reader getNCharacterStream(String columnLabel)
	throws SQLException
    {
	return base.getNCharacterStream(columnLabel);
    }


    /**
     * @param columnIndex
     * @param x
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateNCharacterStream(int, java.io.Reader, long)
     */
    public void updateNCharacterStream(int columnIndex, Reader x, long length)
	throws SQLException
    {
	base.updateNCharacterStream(columnIndex, x, length);
    }


    /**
     * @param columnLabel
     * @param reader
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateNCharacterStream(java.lang.String, java.io.Reader, long)
     */
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
	throws SQLException
    {
	base.updateNCharacterStream(columnLabel, reader, length);
    }


    /**
     * @param columnIndex
     * @param x
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, long)
     */
    public void updateAsciiStream(int columnIndex, InputStream x, long length)
	throws SQLException
    {
	base.updateAsciiStream(columnIndex, x, length);
    }


    /**
     * @param columnIndex
     * @param x
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, long)
     */
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
	throws SQLException
    {
	base.updateBinaryStream(columnIndex, x, length);
    }


    /**
     * @param columnIndex
     * @param x
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, long)
     */
    public void updateCharacterStream(int columnIndex, Reader x, long length)
	throws SQLException
    {
	base.updateCharacterStream(columnIndex, x, length);
    }


    /**
     * @param columnLabel
     * @param x
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream, long)
     */
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
	throws SQLException
    {
	base.updateAsciiStream(columnLabel, x, length);
    }


    /**
     * @param columnLabel
     * @param x
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream, long)
     */
    public void updateBinaryStream(String columnLabel, InputStream x, long length)
	throws SQLException
    {
	base.updateBinaryStream(columnLabel, x, length);
    }


    /**
     * @param columnLabel
     * @param reader
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader, long)
     */
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
	throws SQLException
    {
	base.updateCharacterStream(columnLabel, reader, length);
    }


    /**
     * @param columnIndex
     * @param inputStream
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateBlob(int, java.io.InputStream, long)
     */
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
	throws SQLException
    {
	base.updateBlob(columnIndex, inputStream, length);
    }


    /**
     * @param columnLabel
     * @param inputStream
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateBlob(java.lang.String, java.io.InputStream, long)
     */
    public void updateBlob(String columnLabel, InputStream inputStream, long length)
	throws SQLException
    {
	base.updateBlob(columnLabel, inputStream, length);
    }


    /**
     * @param columnIndex
     * @param reader
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateClob(int, java.io.Reader, long)
     */
    public void updateClob(int columnIndex, Reader reader, long length)
	throws SQLException
    {
	base.updateClob(columnIndex, reader, length);
    }


    /**
     * @param columnLabel
     * @param reader
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateClob(java.lang.String, java.io.Reader, long)
     */
    public void updateClob(String columnLabel, Reader reader, long length)
	throws SQLException
    {
	base.updateClob(columnLabel, reader, length);
    }


    /**
     * @param columnIndex
     * @param reader
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateNClob(int, java.io.Reader, long)
     */
    public void updateNClob(int columnIndex, Reader reader, long length)
	throws SQLException
    {
	base.updateNClob(columnIndex, reader, length);
    }


    /**
     * @param columnLabel
     * @param reader
     * @param length
     * @throws SQLException
     * @see java.sql.ResultSet#updateNClob(java.lang.String, java.io.Reader, long)
     */
    public void updateNClob(String columnLabel, Reader reader, long length)
	throws SQLException
    {
	base.updateNClob(columnLabel, reader, length);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateNCharacterStream(int, java.io.Reader)
     */
    public void updateNCharacterStream(int columnIndex, Reader x)
	throws SQLException
    {
	base.updateNCharacterStream(columnIndex, x);
    }


    /**
     * @param columnLabel
     * @param reader
     * @throws SQLException
     * @see java.sql.ResultSet#updateNCharacterStream(java.lang.String, java.io.Reader)
     */
    public void updateNCharacterStream(String columnLabel, Reader reader)
	throws SQLException
    {
	base.updateNCharacterStream(columnLabel, reader);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream)
     */
    public void updateAsciiStream(int columnIndex, InputStream x)
	throws SQLException
    {
	base.updateAsciiStream(columnIndex, x);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream)
     */
    public void updateBinaryStream(int columnIndex, InputStream x)
	throws SQLException
    {
	base.updateBinaryStream(columnIndex, x);
    }


    /**
     * @param columnIndex
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader)
     */
    public void updateCharacterStream(int columnIndex, Reader x)
	throws SQLException
    {
	base.updateCharacterStream(columnIndex, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream)
     */
    public void updateAsciiStream(String columnLabel, InputStream x)
	throws SQLException
    {
	base.updateAsciiStream(columnLabel, x);
    }


    /**
     * @param columnLabel
     * @param x
     * @throws SQLException
     * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream)
     */
    public void updateBinaryStream(String columnLabel, InputStream x)
	throws SQLException
    {
	base.updateBinaryStream(columnLabel, x);
    }


    /**
     * @param columnLabel
     * @param reader
     * @throws SQLException
     * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader)
     */
    public void updateCharacterStream(String columnLabel, Reader reader)
	throws SQLException
    {
	base.updateCharacterStream(columnLabel, reader);
    }


    /**
     * @param columnIndex
     * @param inputStream
     * @throws SQLException
     * @see java.sql.ResultSet#updateBlob(int, java.io.InputStream)
     */
    public void updateBlob(int columnIndex, InputStream inputStream)
	throws SQLException
    {
	base.updateBlob(columnIndex, inputStream);
    }


    /**
     * @param columnLabel
     * @param inputStream
     * @throws SQLException
     * @see java.sql.ResultSet#updateBlob(java.lang.String, java.io.InputStream)
     */
    public void updateBlob(String columnLabel, InputStream inputStream)
	throws SQLException
    {
	base.updateBlob(columnLabel, inputStream);
    }


    /**
     * @param columnIndex
     * @param reader
     * @throws SQLException
     * @see java.sql.ResultSet#updateClob(int, java.io.Reader)
     */
    public void updateClob(int columnIndex, Reader reader)
	throws SQLException
    {
	base.updateClob(columnIndex, reader);
    }


    /**
     * @param columnLabel
     * @param reader
     * @throws SQLException
     * @see java.sql.ResultSet#updateClob(java.lang.String, java.io.Reader)
     */
    public void updateClob(String columnLabel, Reader reader)
	throws SQLException
    {
	base.updateClob(columnLabel, reader);
    }


    /**
     * @param columnIndex
     * @param reader
     * @throws SQLException
     * @see java.sql.ResultSet#updateNClob(int, java.io.Reader)
     */
    public void updateNClob(int columnIndex, Reader reader)
	throws SQLException
    {
	base.updateNClob(columnIndex, reader);
    }


    /**
     * @param columnLabel
     * @param reader
     * @throws SQLException
     * @see java.sql.ResultSet#updateNClob(java.lang.String, java.io.Reader)
     */
    public void updateNClob(String columnLabel, Reader reader)
	throws SQLException
    {
	base.updateNClob(columnLabel, reader);
    }


 


    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
