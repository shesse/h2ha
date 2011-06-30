/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package org.h2.jdbcx;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcConnectionWrapper;
import org.h2.message.DbException;

/**
 * 
 * @author sth
 */
public class JdbcXAConnectionWrapper
    extends JdbcXAConnection
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    // private static Logger log =
    // Logger.getLogger(JdbcXAConnectionWrapper.class);


    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public JdbcXAConnectionWrapper(JdbcDataSourceFactory factory, int id,
				   JdbcConnection physicalConn)
    {
	super(factory, id, physicalConn);
	// log.debug("JdbcXAConnectionWrapper()");
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////


    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////
    /**
     * A pooled connection.
     */
    class PooledJdbcConnectionWrapper
	extends PooledJdbcConnection
    {
	private boolean isClosed;

	public PooledJdbcConnectionWrapper(JdbcConnectionWrapper conn)
	{
	    super(conn);
	}


	public synchronized void close()
	    throws SQLException
	{
	    if (!isClosed) {
		rollback();
		setAutoCommit(true);
		closedHandle();
		isClosed = true;
	    }
	}

	public synchronized boolean isClosed()
	    throws SQLException
	{
	    return isClosed || super.isClosed();
	}

	protected synchronized void checkClosed(boolean write)
	    throws SQLException
	{
	    if (isClosed) {
		throw DbException.get(ErrorCode.OBJECT_CLOSED);
	    }
	    super.checkClosed(write);
	}


    }


}
