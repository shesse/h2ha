/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package org.h2.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;

import org.h2.engine.ConnectionInfo;
import org.h2.engine.SessionInterface;
import org.h2.message.TraceObject;

/**
 *
 * @author sth
 */
public class JdbcConnectionWrapper
    extends JdbcConnection
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    //private static Logger log = Logger.getLogger(JdbcConnectionWrapper.class);

    /** */
    ConnectionInfo connectionInfo;
    
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @param conn
     */
    public JdbcConnectionWrapper(JdbcConnectionWrapper conn)
    {
	super(conn);
	connectionInfo = conn.connectionInfo;
    }


    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public JdbcConnectionWrapper(String url, Properties info)
    throws SQLException
    {
        this(new ConnectionInfo(url, info), true);
    }


    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public JdbcConnectionWrapper(ConnectionInfo ci, boolean useBaseDir)
    throws SQLException
    {
	super(ci, useBaseDir);
	this.connectionInfo = ci;
    }


    /**
     * @param session
     * @param user
     * @param url
     */
    public JdbcConnectionWrapper(SessionInterface session, String user, String url)
    {
	super(session, user, url);

	this.connectionInfo = new ConnectionInfo(url, new Properties());
    }




    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * {@inheritDoc}
     *
     * @see org.h2.jdbc.JdbcConnection#getMetaData()
     */
    @Override
    public DatabaseMetaData getMetaData()
    throws SQLException
    {
	try {
	    int id = getNextId(TraceObject.DATABASE_META_DATA);
	    if (isDebugEnabled()) {
		debugCodeAssign("DatabaseMetaData", TraceObject.DATABASE_META_DATA, id, "getMetaData()");
	    }
	    checkClosed();
	    return new JdbcDatabaseMetaDataWrapper(this, getSession().getTrace(), id);
	    
	} catch (Exception e) {
	    throw logAndConvert(e);
	}
    }



    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
