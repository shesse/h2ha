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
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * 
 * @author sth
 */
public class HaPreparedStatement
	extends HaStatement
	implements PreparedStatement
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	// private static Logger log = Logger.getLogger(HaPreparedStatement.class);

	/** */
	private PreparedStatement base;


	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @param preparedStatement
	 * @param haConnection
	 */
	public HaPreparedStatement(HaConnection haConnection, PreparedStatement base)
	{
		super(haConnection, base);

		this.base = base;
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#executeQuery()
	 */
	public ResultSet executeQuery()
		throws SQLException
	{
		return new HaResultSet(haConnection, this, base.executeQuery());
	}


	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#executeUpdate()
	 */
	public int executeUpdate()
		throws SQLException
	{
		return base.executeUpdate();
	}


	/**
	 * @param parameterIndex
	 * @param sqlType
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setNull(int, int)
	 */
	public void setNull(int parameterIndex, int sqlType)
		throws SQLException
	{
		base.setNull(parameterIndex, sqlType);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setBoolean(int, boolean)
	 */
	public void setBoolean(int parameterIndex, boolean x)
		throws SQLException
	{
		base.setBoolean(parameterIndex, x);
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
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setByte(int, byte)
	 */
	public void setByte(int parameterIndex, byte x)
		throws SQLException
	{
		base.setByte(parameterIndex, x);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setShort(int, short)
	 */
	public void setShort(int parameterIndex, short x)
		throws SQLException
	{
		base.setShort(parameterIndex, x);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setInt(int, int)
	 */
	public void setInt(int parameterIndex, int x)
		throws SQLException
	{
		base.setInt(parameterIndex, x);
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
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setLong(int, long)
	 */
	public void setLong(int parameterIndex, long x)
		throws SQLException
	{
		base.setLong(parameterIndex, x);
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
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setFloat(int, float)
	 */
	public void setFloat(int parameterIndex, float x)
		throws SQLException
	{
		base.setFloat(parameterIndex, x);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setDouble(int, double)
	 */
	public void setDouble(int parameterIndex, double x)
		throws SQLException
	{
		base.setDouble(parameterIndex, x);
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
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setBigDecimal(int, java.math.BigDecimal)
	 */
	public void setBigDecimal(int parameterIndex, BigDecimal x)
		throws SQLException
	{
		base.setBigDecimal(parameterIndex, x);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setString(int, java.lang.String)
	 */
	public void setString(int parameterIndex, String x)
		throws SQLException
	{
		base.setString(parameterIndex, x);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setBytes(int, byte[])
	 */
	public void setBytes(int parameterIndex, byte[] x)
		throws SQLException
	{
		base.setBytes(parameterIndex, x);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date)
	 */
	public void setDate(int parameterIndex, Date x)
		throws SQLException
	{
		base.setDate(parameterIndex, x);
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
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time)
	 */
	public void setTime(int parameterIndex, Time x)
		throws SQLException
	{
		base.setTime(parameterIndex, x);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp)
	 */
	public void setTimestamp(int parameterIndex, Timestamp x)
		throws SQLException
	{
		base.setTimestamp(parameterIndex, x);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream,
	 *      int)
	 */
	public void setAsciiStream(int parameterIndex, InputStream x, int length)
		throws SQLException
	{
		base.setAsciiStream(parameterIndex, x, length);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @deprecated
	 * @see java.sql.PreparedStatement#setUnicodeStream(int,
	 *      java.io.InputStream, int)
	 */
	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
		throws SQLException
	{
		base.setUnicodeStream(parameterIndex, x, length);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream,
	 *      int)
	 */
	public void setBinaryStream(int parameterIndex, InputStream x, int length)
		throws SQLException
	{
		base.setBinaryStream(parameterIndex, x, length);
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
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#clearParameters()
	 */
	public void clearParameters()
		throws SQLException
	{
		base.clearParameters();
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @param targetSqlType
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int)
	 */
	public void setObject(int parameterIndex, Object x, int targetSqlType)
		throws SQLException
	{
		base.setObject(parameterIndex, x, targetSqlType);
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
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object)
	 */
	public void setObject(int parameterIndex, Object x)
		throws SQLException
	{
		base.setObject(parameterIndex, x);
	}


	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#execute()
	 */
	public boolean execute()
		throws SQLException
	{
		return base.execute();
	}


	/**
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#addBatch()
	 */
	public void addBatch()
		throws SQLException
	{
		base.addBatch();
	}


	/**
	 * @param parameterIndex
	 * @param reader
	 * @param length
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader,
	 *      int)
	 */
	public void setCharacterStream(int parameterIndex, Reader reader, int length)
		throws SQLException
	{
		base.setCharacterStream(parameterIndex, reader, length);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
	 */
	public void setRef(int parameterIndex, Ref x)
		throws SQLException
	{
		base.setRef(parameterIndex, x);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob)
	 */
	public void setBlob(int parameterIndex, Blob x)
		throws SQLException
	{
		if (x instanceof HaBlob) {
			base.setBlob(parameterIndex, ((HaBlob) x).getBase());
		} else {
			base.setBlob(parameterIndex, x);
		}
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setClob(int, java.sql.Clob)
	 */
	public void setClob(int parameterIndex, Clob x)
		throws SQLException
	{
		if (x instanceof HaClob) {
			base.setClob(parameterIndex, ((HaClob) x).getBase());
		} else {
			base.setClob(parameterIndex, x);
		}
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setArray(int, java.sql.Array)
	 */
	public void setArray(int parameterIndex, Array x)
		throws SQLException
	{
		if (x instanceof HaArray) {
			base.setArray(parameterIndex, ((HaArray) x).getBase());
		} else {
			base.setArray(parameterIndex, x);
		}
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @param cal
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date,
	 *      java.util.Calendar)
	 */
	public void setDate(int parameterIndex, Date x, Calendar cal)
		throws SQLException
	{
		base.setDate(parameterIndex, x, cal);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @param cal
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time,
	 *      java.util.Calendar)
	 */
	public void setTime(int parameterIndex, Time x, Calendar cal)
		throws SQLException
	{
		base.setTime(parameterIndex, x, cal);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @param cal
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp,
	 *      java.util.Calendar)
	 */
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
		throws SQLException
	{
		base.setTimestamp(parameterIndex, x, cal);
	}


	/**
	 * @param parameterIndex
	 * @param sqlType
	 * @param typeName
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setNull(int, int, java.lang.String)
	 */
	public void setNull(int parameterIndex, int sqlType, String typeName)
		throws SQLException
	{
		base.setNull(parameterIndex, sqlType, typeName);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setURL(int, java.net.URL)
	 */
	public void setURL(int parameterIndex, URL x)
		throws SQLException
	{
		base.setURL(parameterIndex, x);
	}


	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#getMetaData()
	 */
	public ResultSetMetaData getMetaData()
		throws SQLException
	{
		return new HaResultSetMetaData(haConnection, base.getMetaData());
	}


	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#getParameterMetaData()
	 */
	public ParameterMetaData getParameterMetaData()
		throws SQLException
	{
		return new HaParameterMetaData(haConnection, base.getParameterMetaData());
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setRowId(int, java.sql.RowId)
	 */
	public void setRowId(int parameterIndex, RowId x)
		throws SQLException
	{
		base.setRowId(parameterIndex, x);
	}


	/**
	 * @param parameterIndex
	 * @param value
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setNString(int, java.lang.String)
	 */
	public void setNString(int parameterIndex, String value)
		throws SQLException
	{
		base.setNString(parameterIndex, value);
	}


	/**
	 * @param parameterIndex
	 * @param value
	 * @param length
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setNCharacterStream(int, java.io.Reader,
	 *      long)
	 */
	public void setNCharacterStream(int parameterIndex, Reader value, long length)
		throws SQLException
	{
		base.setNCharacterStream(parameterIndex, value, length);
	}


	/**
	 * @param parameterIndex
	 * @param value
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setNClob(int, java.sql.NClob)
	 */
	public void setNClob(int parameterIndex, NClob value)
		throws SQLException
	{
		if (value instanceof HaNClob) {
			base.setNClob(parameterIndex, ((HaNClob) value).getBase());
		} else {
			base.setNClob(parameterIndex, value);
		}
	}


	/**
	 * @param parameterIndex
	 * @param reader
	 * @param length
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setClob(int, java.io.Reader, long)
	 */
	public void setClob(int parameterIndex, Reader reader, long length)
		throws SQLException
	{
		base.setClob(parameterIndex, reader, length);
	}


	/**
	 * @param parameterIndex
	 * @param inputStream
	 * @param length
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setBlob(int, java.io.InputStream, long)
	 */
	public void setBlob(int parameterIndex, InputStream inputStream, long length)
		throws SQLException
	{
		base.setBlob(parameterIndex, inputStream, length);
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
	 * @param parameterIndex
	 * @param reader
	 * @param length
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setNClob(int, java.io.Reader, long)
	 */
	public void setNClob(int parameterIndex, Reader reader, long length)
		throws SQLException
	{
		base.setNClob(parameterIndex, reader, length);
	}


	/**
	 * @param parameterIndex
	 * @param xmlObject
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setSQLXML(int, java.sql.SQLXML)
	 */
	public void setSQLXML(int parameterIndex, SQLXML value)
		throws SQLException
	{
		if (value instanceof HaSQLXML) {
			base.setSQLXML(parameterIndex, ((HaSQLXML) value).getBase());
		} else {
			base.setSQLXML(parameterIndex, value);
		}
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @param targetSqlType
	 * @param scaleOrLength
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int,
	 *      int)
	 */
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
		throws SQLException
	{
		base.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream,
	 *      long)
	 */
	public void setAsciiStream(int parameterIndex, InputStream x, long length)
		throws SQLException
	{
		base.setAsciiStream(parameterIndex, x, length);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @param length
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream,
	 *      long)
	 */
	public void setBinaryStream(int parameterIndex, InputStream x, long length)
		throws SQLException
	{
		base.setBinaryStream(parameterIndex, x, length);
	}


	/**
	 * @param parameterIndex
	 * @param reader
	 * @param length
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader,
	 *      long)
	 */
	public void setCharacterStream(int parameterIndex, Reader reader, long length)
		throws SQLException
	{
		base.setCharacterStream(parameterIndex, reader, length);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream)
	 */
	public void setAsciiStream(int parameterIndex, InputStream x)
		throws SQLException
	{
		base.setAsciiStream(parameterIndex, x);
	}


	/**
	 * @param parameterIndex
	 * @param x
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream)
	 */
	public void setBinaryStream(int parameterIndex, InputStream x)
		throws SQLException
	{
		base.setBinaryStream(parameterIndex, x);
	}


	/**
	 * @param parameterIndex
	 * @param reader
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader)
	 */
	public void setCharacterStream(int parameterIndex, Reader reader)
		throws SQLException
	{
		base.setCharacterStream(parameterIndex, reader);
	}


	/**
	 * @param parameterIndex
	 * @param value
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setNCharacterStream(int, java.io.Reader)
	 */
	public void setNCharacterStream(int parameterIndex, Reader value)
		throws SQLException
	{
		base.setNCharacterStream(parameterIndex, value);
	}


	/**
	 * @param parameterIndex
	 * @param reader
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setClob(int, java.io.Reader)
	 */
	public void setClob(int parameterIndex, Reader reader)
		throws SQLException
	{
		base.setClob(parameterIndex, reader);
	}


	/**
	 * @param parameterIndex
	 * @param inputStream
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setBlob(int, java.io.InputStream)
	 */
	public void setBlob(int parameterIndex, InputStream inputStream)
		throws SQLException
	{
		base.setBlob(parameterIndex, inputStream);
	}


	/**
	 * @param parameterIndex
	 * @param reader
	 * @throws SQLException
	 * @see java.sql.PreparedStatement#setNClob(int, java.io.Reader)
	 */
	public void setNClob(int parameterIndex, Reader reader)
		throws SQLException
	{
		base.setNClob(parameterIndex, reader);
	}


	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
