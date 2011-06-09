/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 *
 * @author sth
 */
public class HaStruct
    implements Struct
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(HaStruct.class);

    /** */
    @SuppressWarnings("unused")
    private HaConnection haConnection;
    
    /** */
    private Struct base;
    

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @param struct 
     * @param haConnection 
     */
    public HaStruct(HaConnection haConnection, Struct base)
    {
	log.debug("HaStruct()");
	this.haConnection = haConnection;
	this.base = base;
    }


    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////

    /**
     * @return
     * @throws SQLException
     * @see java.sql.Struct#getSQLTypeName()
     */
    public String getSQLTypeName()
	throws SQLException
    {
	return base.getSQLTypeName();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Struct#getAttributes()
     */
    public Object[] getAttributes()
	throws SQLException
    {
	return base.getAttributes();
    }


    /**
     * @param map
     * @return
     * @throws SQLException
     * @see java.sql.Struct#getAttributes(java.util.Map)
     */
    public Object[] getAttributes(Map<String, Class<?>> map)
	throws SQLException
    {
	return base.getAttributes(map);
    }


 

    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
