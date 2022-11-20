/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;


/**
 *
 * @author sth
 */
public class TableRegistry
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(TableRegistry.class);
    
    /** */
    private DbManager dbManager;
    
    /** */
    private Map<String, TestTable> tablesByName = new HashMap<String, TestTable>();
    
    /** */
    private List<TestTable> allTables = new ArrayList<TestTable>();
    
    /** */
    private Random rnd = new Random();
    
    
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @throws SQLException 
     */
    public TableRegistry(DbManager dbManager)
    {
        log.debug("TableRegistry()");
        this.dbManager = dbManager;
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @throws SQLException 
     * 
     */
    public void startup() throws SQLException
    {
        rnd = new Random(918364);
        discoverTables();
    }

    /**
     * 
     */
    public void shutdown()
    {
        allTables.clear();
        tablesByName.clear();
    }

    /**
     * @throws SQLException 
     * 
     */
    private void discoverTables()
    throws SQLException
    {
        allTables.clear();
        tablesByName.clear();
        Connection conn = dbManager.createConnection();
        try {
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet trset = meta.getTables(null, null, null, null);
            while (trset.next()) {
                String schema = trset.getString("TABLE_SCHEM");
                String name = trset.getString("TABLE_NAME");
                //log.info("schema=" + schema + ", name=" + name);
                
                if ("PUBLIC".equals(schema)) {
                    TestTable table = new TestTable(dbManager, name);
                    allTables.add(table);
                    tablesByName.put(name, table);
                }
            }
            log.debug("discovered tables: "+allTables);
            
        } finally {
            conn.close();
        }
    }
    
    /**
     * @throws SQLException 
     * 
     */
    public TestTable createTable()
    throws SQLException
    {
        String name;
        do {
            name = "HAT_1_"+String.format("%08d", rnd.nextInt(99999999));
        } while (tablesByName.containsKey(name));

        TestTable table = new TestTable(dbManager, name);
        table.create();

        allTables.add(table);
        tablesByName.put(name, table);
        
        return table;
    }

    /**
     * @throws SQLException 
     * 
     */
    public void dropTable(String name)
    throws SQLException
    {
        TestTable table = tablesByName.get(name);
        
        if (table != null) {
            String sql = "drop table "+name;
            Connection conn = dbManager.createConnection();
            try {
                conn.createStatement().executeUpdate(sql);
                allTables.remove(table);
                tablesByName.remove(name);
                conn.commit();

            } finally {
                conn.close();
            }
        }
    }

    /**
     * @return
     */
    public DbManager getDbManager()
    {
        return dbManager;
    }
    
    /**
     * for testing only
     */
    public static void main(String[] args)
    throws Exception
    {
        log.info("discovering ...");
        TableRegistry tr = new TableRegistry(new DbManager(true));
        TestTable table = tr.createTable();
        tr.dropTable(table.getName());
        log.info("done");
    }

    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
