/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Test;

/**
 *
 * @author sth
 */
public class DriverTest
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(DriverTest.class);


    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public DriverTest()
    {
	log.debug("DriverTest()");
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    @Test
    public void connect()
    throws SQLException
    {
	Driver dr = new Driver();
	Properties props = new Properties();
	props.setProperty("user", "escal");
	props.setProperty("password", "escal");
	Connection conn  = dr.connect("jdbc:h2:tcp://localhost:9092,localhost:9093/escal", props);
	Assert.assertNotNull(conn);
	Assert.assertTrue("conn instanceof HaConnection", conn instanceof HaConnection);
	
	Statement stmnt = conn.createStatement();
	Assert.assertTrue("stmnt instanceof HaStatement", stmnt instanceof HaStatement);

	ResultSet rset = stmnt.executeQuery("show tables");
	Assert.assertTrue("rset instanceof HaResultSet", rset instanceof HaResultSet);

	conn.close();
    }


    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
