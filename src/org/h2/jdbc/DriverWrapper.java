/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package org.h2.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.h2.Driver;
import org.h2.message.DbException;
import org.h2.message.TraceSystem;
import org.h2.upgrade.DbUpgrade;

/**
 *
 * @author sth
 */
public class DriverWrapper
    extends Driver
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    //private static Logger log = Logger.getLogger(DriverWrapper.class);

    private static final String DEFAULT_URL = "jdbc:default:connection";

    private static final DriverWrapper INSTANCE = new DriverWrapper();

    private static volatile boolean registered;


    static {
        load();
    }


    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public DriverWrapper()
    {
	//log.debug("DriverWrapper()");
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * INTERNAL
     */
    public static synchronized Driver load() {
        try {
            if (!registered) {
                registered = true;
                DriverManager.registerDriver(INSTANCE);
            }
        } catch (SQLException e) {
            TraceSystem.traceThrowable(e);
        }
        return INSTANCE;
    }

    /**
     * Open a database connection.
     * This method should not be called by an application.
     * Instead, the method DriverManager.getConnection should be used.
     *
     * @param url the database URL
     * @param info the connection properties
     * @return the new connection or null if the URL is not supported
     */
    public Connection connect(String url, Properties info)
    throws SQLException
    {
        try {
            if (info == null) {
                info = new Properties();
            }
            if (!acceptsURL(url)) {
                return null;
            }
            if (url.equals(DEFAULT_URL)) {
                return super.connect(url, info);
            }
            Connection c = DbUpgrade.connectOrUpgrade(url, info);
            if (c != null) {
                return c;
            }
            return new JdbcConnectionWrapper(url, info);
        } catch (Exception e) {
            throw DbException.toSQLException(e);
        }
    }



    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
