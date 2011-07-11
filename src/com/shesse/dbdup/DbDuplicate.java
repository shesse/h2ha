package com.shesse.dbdup;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class DbDuplicate
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(DbDuplicate.class);


    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public DbDuplicate()
    {
	log.debug("DbDuplicate()");
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * 
     */
    public static void main(String[] args)
    {
	try {
	    new DbDuplicate().run(args);
	} catch (SQLException x) {
	    System.err.println("SQL error: "+x.getMessage());
	    System.exit(1);
	}
    }

    /**
     * @param args
     * @throws SQLException 
     */
    private void run(String[] args)
    throws SQLException
    {
	if (args.length != 6) {
	    System.err.println("usage: DbDuplicate from-url from-user from-password to-url to-user to-password");
	}
	
	Connection fromConn = openDbConnection(args[0], args[1], args[2]);
	try {
	    Connection toConn = openDbConnection(args[3], args[4], args[5]);
	    try {
		dupSchema(fromConn, toConn);
	    } finally {
		toConn.close();
	    }
	} finally {
	    fromConn.close();
	}
    }

    /**
     * @param fromConn
     * @param toConn
     * @throws SQLException 
     */
    private void dupSchema(Connection fromConn, Connection toConn)
    throws SQLException
    {
	DatabaseMetaData dbMeta = fromConn.getMetaData();
	
	ResultSet tables = dbMeta.getTables(fromConn.getCatalog(), "", null, null);
	try {
	    while (tables.next()) {
		String tableType = tables.getString("TABLE_TYPE");
		String tableName = tables.getString("TABLE_NAME");
		if ("TABLE".equals(tableType)) {
		    dupTable(fromConn, toConn, tableName);
		} else {
		    System.err.println("won't dup table of type "+tableName);
		}
	    }
	    
	} finally {
	    tables.close();
	}
    }

    /**
     * @param fromConn
     * @param toConn
     * @param tableName
     * @throws SQLException 
     */
    private void dupTable(Connection fromConn, Connection toConn, String tableName)
    throws SQLException
    {
	System.err.println("copying table "+tableName);
	Statement toStmnt = toConn.createStatement();
	Statement fromStmnt = fromConn.createStatement();
	
	try {
	    String sql = "drop table if exists "+tableName;
	    System.err.println(sql);
	    toStmnt.executeUpdate(sql);
	    toConn.commit();

	    sql = "select * from "+tableName;
	    ResultSet srcRecords = fromStmnt.executeQuery(sql);
	    
	    try {
		sql = genDdl(tableName, srcRecords.getMetaData());
		System.err.println(sql);
		toStmnt.executeUpdate(sql);
		toConn.commit();
		
		dupRecords(fromConn, toConn, tableName, srcRecords);
		
	    } finally {
		srcRecords.close();
	    }
	    
	} finally {
	    toStmnt.close();
	    fromStmnt.close();
	}
    }

    /**
     * @param fromConn
     * @param toConn
     * @param tableName
     * @param srcRecords
     * @throws SQLException 
     */
    private void dupRecords(Connection fromConn, Connection toConn, String tableName,
			    ResultSet srcRecords)
    throws SQLException
    {
	ResultSetMetaData meta = srcRecords.getMetaData();
	
	StringBuilder sb = new StringBuilder();
	
	sb.append("insert into "+tableName+" (");
	String delim = "";
	
	int ncol = meta.getColumnCount();
	
	for (int c = 1; c <= ncol; c++) {
	    sb.append(delim).append(meta.getColumnName(c));
	    delim = ", ";
	}
	
	sb.append(") values (");
	
	delim = "";
	for (int c = 1; c <= ncol; c++) {
	    sb.append(delim).append("?");
	    delim = ", ";
	}
	sb.append(")");

	System.err.println(sb);
	PreparedStatement pins = toConn.prepareStatement(sb.toString());
	int nonCommitedCount = 0;
	try {
	    while (srcRecords.next()) {
		for (int c = 1; c <= ncol; c++) {
		    pins.setObject(c, srcRecords.getObject(c));
		}

		pins.execute();
		
		nonCommitedCount++;
		if (nonCommitedCount > 1000) {
		    toConn.commit();
		    nonCommitedCount = 0;
		}
	    }
	    
	} finally {
	    pins.close();
	}
	
	toConn.commit();
    }

    /**
     * @param tableName
     * @param meta
     * @throws SQLException 
     */
    private String genDdl(String tableName, ResultSetMetaData meta)
    throws SQLException
    {
	StringBuilder sb = new StringBuilder();
	
	sb.append("create table "+tableName+" (\n");

	int ncol = meta.getColumnCount();
	
	for (int c = 1; c <= ncol; c++) {
	    sb.append("  ").append(meta.getColumnName(c)).append(" ");
	    
	    String columnTypeName = meta.getColumnTypeName(c);
	    
	    sb.append(columnTypeName);
	    
	    int p = meta.getPrecision(c);
	    if (p > 0) {
		sb.append("(").append(p);
		int s = meta.getScale(c);
		if (s > 0) sb.append(", ").append(s);
		sb.append(")");
	    }
	    
	    if (meta.isNullable(c) == ResultSetMetaData.columnNoNulls) {
		sb.append(" not null");
	    }
	    
	    if (c < ncol) {
		sb.append(",\n");
	    } else {
		sb.append("\n");
	    }
	}
	
	sb.append(")");
	
	return sb.toString();
    }

    /**
     * @param string
     * @return
     * @throws SQLException 
     */
    private Connection openDbConnection(String dbUrl, String dbUser, String dbPassword)
    throws SQLException
    {
	if (dbUrl.startsWith("jdbc:h2:")) {
	    try {
		Class.forName("org.h2.Driver");
	    } catch (ClassNotFoundException x) {
		throw new SQLException("cannot find H2 driver");
	    }
	    
	} else if (dbUrl.startsWith("jdbc:mysql:")) {
	    try {
		Class.forName("com.mysql.jdbc.Driver");
	    } catch (ClassNotFoundException x) {
		throw new SQLException("cannot find MySQL driver");
	    }
	    
	} else {
	    throw new SQLException("unknown URL: "+dbUrl);
	}

	Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
	conn.setAutoCommit(false);
	return conn;
    }


    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
