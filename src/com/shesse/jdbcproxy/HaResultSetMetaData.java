/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.log4j.Logger;

/**
 *
 * @author sth
 */
public class HaResultSetMetaData
    implements ResultSetMetaData
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(HaResultSetMetaData.class);

    /** */
    @SuppressWarnings("unused")
    private HaConnection haConnection;
    
    /** */
    private ResultSetMetaData base;
    

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @param resultSetMetaData 
     * @param haConnection 
     */
    public HaResultSetMetaData(HaConnection haConnection, ResultSetMetaData base)
    {
	log.debug("HaResultSetMetaData()");
	this.haConnection = haConnection;
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
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#getColumnCount()
     */
    public int getColumnCount()
	throws SQLException
    {
	return base.getColumnCount();
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#isAutoIncrement(int)
     */
    public boolean isAutoIncrement(int column)
	throws SQLException
    {
	return base.isAutoIncrement(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#isCaseSensitive(int)
     */
    public boolean isCaseSensitive(int column)
	throws SQLException
    {
	return base.isCaseSensitive(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#isSearchable(int)
     */
    public boolean isSearchable(int column)
	throws SQLException
    {
	return base.isSearchable(column);
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
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#isCurrency(int)
     */
    public boolean isCurrency(int column)
	throws SQLException
    {
	return base.isCurrency(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#isNullable(int)
     */
    public int isNullable(int column)
	throws SQLException
    {
	return base.isNullable(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#isSigned(int)
     */
    public boolean isSigned(int column)
	throws SQLException
    {
	return base.isSigned(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#getColumnDisplaySize(int)
     */
    public int getColumnDisplaySize(int column)
	throws SQLException
    {
	return base.getColumnDisplaySize(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#getColumnLabel(int)
     */
    public String getColumnLabel(int column)
	throws SQLException
    {
	return base.getColumnLabel(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#getColumnName(int)
     */
    public String getColumnName(int column)
	throws SQLException
    {
	return base.getColumnName(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#getSchemaName(int)
     */
    public String getSchemaName(int column)
	throws SQLException
    {
	return base.getSchemaName(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#getPrecision(int)
     */
    public int getPrecision(int column)
	throws SQLException
    {
	return base.getPrecision(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#getScale(int)
     */
    public int getScale(int column)
	throws SQLException
    {
	return base.getScale(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#getTableName(int)
     */
    public String getTableName(int column)
	throws SQLException
    {
	return base.getTableName(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#getCatalogName(int)
     */
    public String getCatalogName(int column)
	throws SQLException
    {
	return base.getCatalogName(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#getColumnType(int)
     */
    public int getColumnType(int column)
	throws SQLException
    {
	return base.getColumnType(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#getColumnTypeName(int)
     */
    public String getColumnTypeName(int column)
	throws SQLException
    {
	return base.getColumnTypeName(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#isReadOnly(int)
     */
    public boolean isReadOnly(int column)
	throws SQLException
    {
	return base.isReadOnly(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#isWritable(int)
     */
    public boolean isWritable(int column)
	throws SQLException
    {
	return base.isWritable(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#isDefinitelyWritable(int)
     */
    public boolean isDefinitelyWritable(int column)
	throws SQLException
    {
	return base.isDefinitelyWritable(column);
    }


    /**
     * @param column
     * @return
     * @throws SQLException
     * @see java.sql.ResultSetMetaData#getColumnClassName(int)
     */
    public String getColumnClassName(int column)
	throws SQLException
    {
	return base.getColumnClassName(column);
    }




    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
