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
	
	/** */
	private static String cont;
	static {
		cont = "0123456789"; // 10
		cont += cont; // 20
		cont += cont; // 40
		cont += cont; // 80
		cont += cont; // 160
		cont += cont; // 320
		cont += cont; // 640
		cont += cont; // 1280
	}

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
	public void create()
		throws SQLException
	{
		Connection conn = dbManager.createConnection();
		try {
			String sql = "create table " + name + "(" + //
				"  i integer not null auto_increment primary key, " + //
				"  j integer, " + //
				"  s varchar2(255) not null, " + //
				"  s0 varchar2(4000), " + //
				"  s1 varchar2(4000), " + //
				"  s2 varchar2(4000), " + //
				"  s3 varchar2(4000), " + //
				"  s4 varchar2(4000), " + //
				"  s5 varchar2(4000), " + //
				"  s6 varchar2(4000), " + //
				"  s7 varchar2(4000), " + //
				"  s8 varchar2(4000), " + //
				"  s9 varchar2(4000));";

			Statement stmnt = conn.createStatement();
			stmnt.executeUpdate(sql);

			sql = "create index x_" + name + "_1 on " + name + "( s );";
			stmnt.executeUpdate(sql);

			sql = "create index x_" + name + "_2 on " + name + "( j );";
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
	public void insertRecord()
		throws SQLException
	{
		insertRecord(0);
	}

	/**
	 * @throws SQLException
	 * 
	 */
	public void insertRecord(int addData)
					throws SQLException
	{
		Connection conn = dbManager.createConnection();
		try {
			Statement stmnt = conn.createStatement();
			insertRecordWithoutCommit(stmnt, addData);
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
		insertRecordWithoutCommit(stmnt, 0);
		
	}
	/**
	 * @throws SQLException
	 * 
	 */
	public void insertRecordWithoutCommit(Statement stmnt, int addData)
		throws SQLException
	{
		int i = nextIndex++;
		int j = (i + 45) % 222;
		String s = "Hallo: " + i;
		String sql =
			"insert into " + name + " (i, j, s";
		for (int c = 0; c < addData; c++) {
			sql += ", s"+c;
		}
		sql += ") values (" + i + ", " + j + ", '" + s + "'";
		
		for (int c = 0; c < addData; c++) {
			sql += ", '"+cont+"'";
		}
		sql += ");";

		stmnt.executeUpdate(sql);
	}

	/**
	 * @throws SQLException
	 * 
	 */
	public int getNoOfRecords()
		throws SQLException
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
