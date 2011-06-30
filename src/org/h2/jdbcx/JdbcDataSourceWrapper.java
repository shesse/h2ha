/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package org.h2.jdbcx;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.XAConnection;

import org.h2.jdbc.DriverWrapper;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcConnectionWrapper;
import org.h2.util.StringUtils;

/**
 *
 * @author sth
 */
public class JdbcDataSourceWrapper
    extends JdbcDataSource
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    //private static Logger log = Logger.getLogger(JdbcDataSourceWrapper.class);


    /** */
    private static final long serialVersionUID = 1L;

    private transient JdbcDataSourceFactoryWrapper factory;

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public JdbcDataSourceWrapper()
    {
	//log.debug("JdbcDataSourceWrapper()");
	factory = new JdbcDataSourceFactoryWrapper();
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * Open a new XA connection using the current URL, user name and password.
     *
     * @return the connection
     */
    public XAConnection getXAConnection() throws SQLException {
        debugCodeCall("getXAConnection");
        int id = getNextId(XA_DATA_SOURCE);
        return new JdbcXAConnectionWrapper(factory, id, getJdbcConnection(getUser(), StringUtils.cloneCharArray(getPassword().toCharArray())));
    }

    /**
     * Open a new XA connection using the current URL and the specified user
     * name and password.
     *
     * @param user the user name
     * @param password the password
     * @return the connection
     */
    public XAConnection getXAConnection(String user, String password) throws SQLException {
        if (isDebugEnabled()) {
            debugCode("getXAConnection("+quote(user)+", \"\");");
        }
        int id = getNextId(XA_DATA_SOURCE);
        return new JdbcXAConnectionWrapper(factory, id, getJdbcConnection(user, getPassword().toCharArray()));
    }


    private JdbcConnectionWrapper getJdbcConnection(String user, char[] password) throws SQLException {
        if (isDebugEnabled()) {
            debugCode("getJdbcConnection("+quote(user)+", new char[0]);");
        }
        Properties info = new Properties();
        info.setProperty("user", user);
        info.put("password", password);
        Connection conn = DriverWrapper.load().connect(getURL(), info);
        if (conn == null) {
            throw new SQLException("No suitable driver found for " + getURL(), "08001", 8001);
        } else if (!(conn instanceof JdbcConnection)) {
            throw new SQLException("Connecting with old version is not supported: " + getURL(), "08001", 8001);
        }
        return (JdbcConnectionWrapper) conn;
    }


    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
