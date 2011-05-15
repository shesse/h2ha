/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;


/**
 *
 * @author sth
 */
public class TestTable
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(TestTable.class);
    
    /** */
    private DbManager dbManager;
    
    /** */
    private String name;
    
    /** */
    private int nextIndex = 0;
        

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public TestTable(DbManager dbManager, String name)
    {
        log.debug("TestTable()");
        
        this.dbManager = dbManager;
        this.name = name;
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }
    
    /**
     * @throws SQLException 
     * 
     */
    public void create() throws SQLException
    {
        Connection conn = dbManager.createConnection();
        try {
            String sql = "create table " + name + "(" + //
            "  i integer not null auto_increment primary key, " + //
            "  j integer, " + //
            "  s varchar2(255) not null);";

            Statement stmnt = conn.createStatement();
            stmnt.executeUpdate(sql);

            sql = "create index x_"+ name + "_1 on " +name+ "( s );";
            stmnt.executeUpdate(sql);

            sql = "create index x_"+ name + "_2 on " +name+ "( j );";
            stmnt.executeUpdate(sql);
            conn.commit();
            
        } finally {
            conn.close();
        }
    }

    /**
     * @throws SQLException 
     * 
     */
    public void insertRecord() throws SQLException
    {
        Connection conn = dbManager.createConnection();
        try {
            Statement stmnt = conn.createStatement();
            insertRecordWithoutCommit(stmnt);
            conn.commit();
            
        } finally {
            conn.close();
        }
    }
    
    /**
     * @throws SQLException 
     * 
     */
    public void insertRecordWithoutCommit(Statement stmnt)
    throws SQLException
    {
	int i = nextIndex++;
        int j = (i+45) % 222;
        String s = "Hallo: "+i;
        String sql = "insert into " + name + " (i, j, s) values ("+i+", "+j+", '"+s+"');";

        stmnt.executeUpdate(sql);
    }

    /**
     * @throws SQLException 
     * 
     */
    public int getNoOfRecords() throws SQLException
    {
        Connection conn = dbManager.createConnection();
        try {
            String sql = "select count(*) from " + name;

            Statement stmnt = conn.createStatement();
            ResultSet rs = stmnt.executeQuery(sql);
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                return 0;
            }
            
        } finally {
            conn.close();
        }
    }

    /**
     * 
     */
    public String toString()
    {
        return name;
    }

    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
