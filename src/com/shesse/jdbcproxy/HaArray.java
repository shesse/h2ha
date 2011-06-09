/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 *
 * @author sth
 */
public class HaArray
    implements Array
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(HaArray.class);
    
    /** */
    private HaConnection haConnection;
    
    /** */
    private Array base;
    
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @param array 
     * @param haConnection 
     */
    public HaArray(HaConnection haConnection, Array base)
    {
	log.debug("HaArray()");
	this.haConnection = haConnection;
	this.base = base;
    }


    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @return
     */
    public Array getBase()
    {
	return base;
    }

    /**
     * @return
     * @throws SQLException
     * @see java.sql.Array#getBaseTypeName()
     */
    public String getBaseTypeName()
	throws SQLException
    {
	return base.getBaseTypeName();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Array#getBaseType()
     */
    public int getBaseType()
	throws SQLException
    {
	return base.getBaseType();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Array#getArray()
     */
    public Object getArray()
	throws SQLException
    {
	return base.getArray();
    }


    /**
     * @param map
     * @return
     * @throws SQLException
     * @see java.sql.Array#getArray(java.util.Map)
     */
    public Object getArray(Map<String, Class<?>> map)
	throws SQLException
    {
	return base.getArray(map);
    }


    /**
     * @param index
     * @param count
     * @return
     * @throws SQLException
     * @see java.sql.Array#getArray(long, int)
     */
    public Object getArray(long index, int count)
	throws SQLException
    {
	return base.getArray(index, count);
    }


    /**
     * @param index
     * @param count
     * @param map
     * @return
     * @throws SQLException
     * @see java.sql.Array#getArray(long, int, java.util.Map)
     */
    public Object getArray(long index, int count, Map<String, Class<?>> map)
	throws SQLException
    {
	return base.getArray(index, count, map);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Array#getResultSet()
     */
    public ResultSet getResultSet()
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getResultSet());
    }


    /**
     * @param map
     * @return
     * @throws SQLException
     * @see java.sql.Array#getResultSet(java.util.Map)
     */
    public ResultSet getResultSet(Map<String, Class<?>> map)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getResultSet(map));
    }


    /**
     * @param index
     * @param count
     * @return
     * @throws SQLException
     * @see java.sql.Array#getResultSet(long, int)
     */
    public ResultSet getResultSet(long index, int count)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getResultSet(index, count));
    }


    /**
     * @param index
     * @param count
     * @param map
     * @return
     * @throws SQLException
     * @see java.sql.Array#getResultSet(long, int, java.util.Map)
     */
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getResultSet(index, count, map));
    }


    /**
     * @throws SQLException
     * @see java.sql.Array#free()
     */
    public void free()
	throws SQLException
    {
	base.free();
    }






    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
