/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.h2.jdbcx.JdbcConnectionPool;

/**
 *
 * @author sth
 */
public class DbManager
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(DbManager.class);

    /** */
    private JdbcConnectionPool cp = null;
    
    /** */
    private String autoReconnect = "";

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public DbManager()
    {
        log.debug("DbManager()");
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @throws SQLException 
     * 
     */
    public void shutdown() throws SQLException
    {
        cp.dispose();
        cp = null;
    }

    /**
     * @throws SQLException 
     * 
     */
    public Connection createConnection() 
    throws SQLException
    {
        if (cp == null) {
            cp = JdbcConnectionPool.create("jdbc:h2:tcp://localhost:9092,localhost:9093/test"+autoReconnect,
                "sth", "sth");
        }
        return cp.getConnection();
    }
    
    /**
     * 
     */
    public void setAutoReconnect(boolean autoReconnect)
    {
        if (autoReconnect) {
            this.autoReconnect = ";AUTO_RECONNECT=TRUE";
        } else {
            this.autoReconnect = "";
        }
    }
    
    /**
     * Calls syncWithAllReplicators on the master db server
     * @throws SQLException 
     */
    public void syncWithAllReplicators()
    throws SQLException
    {
        Connection conn = createConnection();
        try {
            Statement stmnt = conn.createStatement();
            stmnt.executeUpdate("create alias SYNC_ALL for \"com.shesse.h2ha.H2HaServer.syncWithAllReplicators\"");
            stmnt.executeUpdate("call SYNC_ALL()");
            
        } finally {
            conn.close();
        }
    }
    
    
    /**
     * for testing only
     */
    public static void main(String[] args)
    throws Exception
    {
        log.info("connecting ...");
        new DbManager().createConnection();
        log.info("connected");
    }
    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
