/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

import org.apache.log4j.Logger;

/**
 *
 * @author sth
 */
public class HaParameterMetaData
    implements ParameterMetaData
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(HaParameterMetaData.class);

    /** */
    @SuppressWarnings("unused")
    private HaConnection haConnection;
    
    /** */
    private ParameterMetaData base;
    

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @param parameterMetaData 
     * @param haConnection 
     */
    public HaParameterMetaData(HaConnection haConnection, ParameterMetaData base)
    {
	log.debug("HaParameterMetaData()");
	this.haConnection = haConnection;
	this.base = base;
    }


    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @return
     * @throws SQLException
     * @see java.sql.ParameterMetaData#getParameterCount()
     */
    public int getParameterCount()
	throws SQLException
    {
	return base.getParameterCount();
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
     * @param param
     * @return
     * @throws SQLException
     * @see java.sql.ParameterMetaData#isNullable(int)
     */
    public int isNullable(int param)
	throws SQLException
    {
	return base.isNullable(param);
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
     * @param param
     * @return
     * @throws SQLException
     * @see java.sql.ParameterMetaData#isSigned(int)
     */
    public boolean isSigned(int param)
	throws SQLException
    {
	return base.isSigned(param);
    }


    /**
     * @param param
     * @return
     * @throws SQLException
     * @see java.sql.ParameterMetaData#getPrecision(int)
     */
    public int getPrecision(int param)
	throws SQLException
    {
	return base.getPrecision(param);
    }


    /**
     * @param param
     * @return
     * @throws SQLException
     * @see java.sql.ParameterMetaData#getScale(int)
     */
    public int getScale(int param)
	throws SQLException
    {
	return base.getScale(param);
    }


    /**
     * @param param
     * @return
     * @throws SQLException
     * @see java.sql.ParameterMetaData#getParameterType(int)
     */
    public int getParameterType(int param)
	throws SQLException
    {
	return base.getParameterType(param);
    }


    /**
     * @param param
     * @return
     * @throws SQLException
     * @see java.sql.ParameterMetaData#getParameterTypeName(int)
     */
    public String getParameterTypeName(int param)
	throws SQLException
    {
	return base.getParameterTypeName(param);
    }


    /**
     * @param param
     * @return
     * @throws SQLException
     * @see java.sql.ParameterMetaData#getParameterClassName(int)
     */
    public String getParameterClassName(int param)
	throws SQLException
    {
	return base.getParameterClassName(param);
    }


    /**
     * @param param
     * @return
     * @throws SQLException
     * @see java.sql.ParameterMetaData#getParameterMode(int)
     */
    public int getParameterMode(int param)
	throws SQLException
    {
	return base.getParameterMode(param);
    }





    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
