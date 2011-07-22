/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.h2.jdbc.JdbcConnection;

/**
 *
 * @author sth
 */
public class Driver
    implements java.sql.Driver
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    //private static Logger log = Logger.getLogger(Driver.class.getName());
    
    /** */
    private static org.h2.Driver h2Driver = new org.h2.Driver();
    
    /** */
    private static Driver instance = new Driver();
    
    static {
	try {
	    DriverManager.registerDriver(instance);
	} catch (SQLException x) {
	    x.printStackTrace();
	}
    }
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public Driver()
    {
    }


    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * {@inheritDoc}
     *
     * @see java.sql.Driver#acceptsURL(java.lang.String)
     */
    public boolean acceptsURL(String url)
	throws SQLException
    {
        if (url != null) {
            if (url.startsWith("jdbc:h2ha:")) {
                return true;
            }
        }
	return false;
    }


    /**
     * {@inheritDoc}
     *
     * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
     */
    public Connection connect(String url, Properties info)
	throws SQLException
    {
	JdbcConnection h2Conn = AlternatingConnectionFactory.getFactory(url, info).getConnection();
	return new HaConnection(h2Conn);
    }
    
    /**
     * {@inheritDoc}
     *
     * @see java.sql.Driver#getMajorVersion()
     */
    public int getMajorVersion()
    {
	return h2Driver.getMajorVersion();
    }


    /**
     * {@inheritDoc}
     *
     * @see java.sql.Driver#getMinorVersion()
     */
    public int getMinorVersion()
    {
	return h2Driver.getMinorVersion();
    }


    /**
     * {@inheritDoc}
     *
     * @see java.sql.Driver#getPropertyInfo(java.lang.String, java.util.Properties)
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
	throws SQLException
    {
	return h2Driver.getPropertyInfo(url, info);
    }


    /**
     * {@inheritDoc}
     *
     * @see java.sql.Driver#jdbcCompliant()
     */
    public boolean jdbcCompliant()
    {
	return h2Driver.jdbcCompliant();
    }



    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
