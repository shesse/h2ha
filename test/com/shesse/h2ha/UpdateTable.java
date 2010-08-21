/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.h2.tools.Server;


/**
 *
 * @author sth
 */
public class UpdateTable
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(UpdateTable.class);

    /** */
    @SuppressWarnings("unused")
    private Server tcpServer;
    
    /** */
    @SuppressWarnings("unused")
    private Server webServer;
    
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public UpdateTable()
    {
        log.debug("UpdateTable()");
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @throws SQLException 
     * @throws ClassNotFoundException 
     * @throws InterruptedException 
     * 
     */
    public static void main(String[] args)
    throws SQLException, ClassNotFoundException, InterruptedException
    {
        new UpdateTable().run(args);
    }
    
    /**
     * @throws SQLException 
     * @throws ClassNotFoundException 
     * @throws InterruptedException 
     * 
     */
    private void run(String[] args)
    throws SQLException, ClassNotFoundException, InterruptedException
    {
        tcpServer = Server.createTcpServer(args).start();
        webServer = Server.createWebServer(args).start();

        //new ReplicationServer(fileSystem, args).start();

        Class.forName("org.h2.Driver");
        Connection conn1 = makeConnection();
        Connection conn2 = makeConnection();
        Connection conn3 = makeConnection();
        
        Statement stmnt1 = conn1.createStatement();
        Statement stmnt2 = conn2.createStatement();
        Statement stmnt3 = conn3.createStatement();
        
        makeTable(stmnt1, "testtab1");
        makeTable(stmnt1, "testtab2");
        conn1.commit();
        
        Thread i1 = makeInserter(stmnt1, "testtab1", 0);
        Thread i2 = makeInserter(stmnt2, "testtab1", 100);
        Thread i3 = makeInserter(stmnt3, "testtab2", 200);
               
        i1.start();
        i2.start();
        i3.start();
        
        i1.join();
        i2.join();
        i3.join();
               
        conn1.close();
        conn2.close();
        conn3.close();

    }
    
    /**
     * @throws SQLException  
     */
    private Connection makeConnection() 
    throws SQLException
    {
        Connection conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/test;LOCK_TIMEOUT=30000", "sth", "sth");
        //Connection conn = DriverManager.getConnection("jdbc:h2:ha:~/config/h2/test;LOCK_TIMEOUT=30000", "sth", "sth");
        conn.setAutoCommit(false);
        
        return conn;
    }
    
    /**
     * @throws SQLException 
     * 
     */
    private void makeTable(Statement stmnt, String table)
    throws SQLException
    {
        try {
            stmnt.executeUpdate("drop table "+table);
        } catch (SQLException x) {
            // ignore
        }
        
        stmnt.executeUpdate("create table "+table+" (i int, c varchar)");
    }
    
    /**
     * 
     */
    private Thread makeInserter(final Statement stmnt, final String table, final int offs) 
    {
        return new Thread() {
            public void run()
            {
                try {
                    insertInto(stmnt, table, offs); 
                } catch (SQLException x) {
                    log.error("cannot insert", x);
                } catch (InterruptedException x) {
                    log.error("InterruptedException", x);
                }
            }
        };
    }

    /**
     * @throws SQLException 
     * @throws InterruptedException 
     * 
     */
    private void insertInto(Statement stmnt, String table, int offs) 
    throws SQLException, InterruptedException
    {
        for (int i = 0; i < 2; i++) {
            log.debug("insert "+offs+"_"+i);
            stmnt.executeUpdate("insert into "+table+" (i, c) values ("+(i+offs)+", 'val_"+offs+"_"+i+"')");
 
            Thread.sleep(500);
        }
        stmnt.getConnection().commit();
    }
        
    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
