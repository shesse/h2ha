package com.shesse.dbdup;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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

		} catch (Throwable x) {
			log.fatal("unexpected exception within main thread", x);
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
			/*
			System.err.println("got args:");
			for (int i = 0; i < args.length; i++) {
				System.err.println(" args["+i+"] = "+args[i]);
			}
			System.err.println("\n");
			*/
			
			System.err.println("usage: DbDuplicate from-url from-user from-password to-url to-user to-password");
			System.exit(1);
		}

		Connection fromConn = openDbConnection(args[0], args[1], args[2]);
		try {
			Connection toConn = openDbConnection(args[3], args[4], args[5]);
			try {
				dupSchema(fromConn, toConn);
				toConn.commit();
			} finally {
				try {
					toConn.close();
				} catch (SQLException x) {
				}
			}
		} finally {
			try {
				fromConn.close();
			} catch (SQLException x) {
			}
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
		List<String> tableNames = new ArrayList<String>();
		try {
			while (tables.next()) {
				String tableType = tables.getString("TABLE_TYPE");
				String tableName = tables.getString("TABLE_NAME");
				if ("TABLE".equals(tableType)) {
					tableNames.add(tableName);

				} else {
					System.err.println("won't dup table of type "+tableName);
				}
			}

		} finally {
			tables.close();
		}
		
		for (String tableName: tableNames) {
			dropForeignKeyConstraints(toConn, tableName);
		}
		
		for (String tableName: tableNames) {
			dupTable(fromConn, toConn, tableName);
			String pkeyName = buildPrimaryKey(fromConn, dbMeta, toConn, tableName);
			buildIndexes(fromConn, dbMeta, toConn, tableName, pkeyName);
		}

		for (String tableName: tableNames) {
			buildForeignKeyConstraints(fromConn, dbMeta, toConn, tableName);
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
			log.debug(sql);
			toStmnt.executeUpdate(sql);
			toConn.commit();

			sql = "select * from "+tableName;
			ResultSet srcRecords = fromStmnt.executeQuery(sql);

			try {
				sql = genDdl(tableName, srcRecords.getMetaData());
				log.info(sql);
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

		long startStamp = System.currentTimeMillis();
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

		log.debug(sb);
		PreparedStatement pins = toConn.prepareStatement(sb.toString());
		int nonCommitedCount = 0;
		int recordCount = 0;
		try {
			while (srcRecords.next()) {
				for (int c = 1; c <= ncol; c++) {
					pins.setObject(c, srcRecords.getObject(c));
				}

				pins.execute();
				recordCount++;

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
		System.err.println(recordCount+" records done in "+(System.currentTimeMillis()-startStamp)+" ms for "+tableName);
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

			int p = meta.getPrecision(c);
			if ("DATETIME".equals(columnTypeName) && p == 19) {
				sb.append("TIMESTAMP");

			} else if ("TEXT".equals(columnTypeName)) {
				sb.append("VARCHAR");
				if (p < 65536 && p > 0) {
					sb.append("("+p+")");
				}
			} else {
				sb.append(columnTypeName);

				if (p > 0 && p < 65536) {
					sb.append("(").append(p);
					int s = meta.getScale(c);
					if (s > 0) sb.append(", ").append(s);
					sb.append(")");
				}
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

	/**
	 * @param fromConn
	 * @param toConn
	 * @throws SQLException 
	 */
	private String buildPrimaryKey(Connection fromConn, DatabaseMetaData dbMeta, Connection toConn, String tableName)
	throws SQLException
	{
		ResultSet pkeys = dbMeta.getPrimaryKeys(fromConn.getCatalog(), "", tableName);
		try {
			String pkeyName = null;
			List<String> columns = new ArrayList<String>();
			while (pkeys.next()) {
				short seq = pkeys.getShort("KEY_SEQ");
				while (columns.size() < seq) {
					columns.add(null);
				}
				columns.set(seq-1, pkeys.getString("COLUMN_NAME"));
				pkeyName = pkeys.getString("PK_NAME");
			}
			buildPrimaryKey(toConn, tableName, pkeyName, columns);
			return pkeyName;

		} finally {
			pkeys.close();
		}

	}

	/**
	 * @param toConn
	 * @param tableName
	 * @param pkeyName
	 * @param columns
	 * @throws SQLException 
	 */
	private void buildPrimaryKey(Connection toConn, String tableName, String pkeyName,
	                             List<String> columns)
	throws SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("create primary key on ").append(tableName).append("(");
		String delim = "";
		for (String col: columns) {
			sb.append(delim).append(col);
			delim = ", ";
		}
		sb.append(")");

		Statement stmnt = toConn.createStatement();
		try {
			log.debug(sb);
			stmnt.execute(sb.toString());
		} finally {
			stmnt.close();
		}
	}


	/**
	 * @param fromConn
	 * @param toConn
	 * @throws SQLException 
	 */
	private void buildIndexes(Connection fromConn, DatabaseMetaData dbMeta, Connection toConn, String tableName, String pkeyName)
	throws SQLException
	{
		ResultSet indexes = dbMeta.getIndexInfo(fromConn.getCatalog(), "", tableName, false, false);
		try {
			String currentIndex = null;
			boolean nonUnique = false;
			List<String> columns = new ArrayList<String>();
			while (indexes.next()) {
				String indexName = indexes.getString("INDEX_NAME");
				//log.debug("index "+indexName+", col="+indexes.getString("COLUMN_NAME")+", nuniq="+indexes.getBoolean("NON_UNIQUE"));

				short indexType = indexes.getShort("TYPE");
				if (indexType == DatabaseMetaData.tableIndexStatistic) {
					continue;
				}

				if (indexName.equals(pkeyName)) {
					continue;
				}

				if (!indexName.equals(currentIndex)) {
					buildIndex(toConn, tableName, currentIndex, columns, nonUnique);
					currentIndex = indexName;
					columns.clear();
				}

				columns.add(indexes.getString("COLUMN_NAME"));
				nonUnique = indexes.getBoolean("NON_UNIQUE");
			}
			buildIndex(toConn, tableName, currentIndex, columns, nonUnique);

		} finally {
			indexes.close();
		}

	}

	/**
	 * @param toConn
	 * @param tableName
	 * @param currentIndex
	 * @param columns
	 * @param boolean1
	 * @throws SQLException 
	 */
	private void buildIndex(Connection toConn, String tableName, String indexName,
	                        List<String> columns, boolean nonUnique)
	throws SQLException
	{
		if (indexName == null) {
			return;
		}

		StringBuilder sb = new StringBuilder();
		sb.append("create ");

		if (!nonUnique) {
			sb.append("unique ");
		}

		sb.append("index ").append(indexName).append(" on ").append(tableName).append(" (");

		String delim = "";
		for (String col: columns) {
			sb.append(delim).append(col);
			delim = ", ";
		}
		sb.append(")");

		Statement stmnt = toConn.createStatement();
		try {
			log.debug(sb);
			stmnt.execute(sb.toString());
		} finally {
			stmnt.close();
		}
	}

	/**
	 * @param toConn
	 * @param tableName
	 * @throws SQLException 
	 */
	private void dropForeignKeyConstraints(Connection toConn, String tableName)
	throws SQLException
	{
		DatabaseMetaData dbMeta = toConn.getMetaData();
	
		System.err.println("dropping FK constraints for "+tableName);
		ResultSet crefs = dbMeta.getImportedKeys(toConn.getCatalog(), "", tableName.toUpperCase());
		try {
			while (crefs.next()) {
				//log.debug("  "+crefs.getString("FK_NAME")+": "+crefs.getShort("KEY_SEQ"));
				if (crefs.getShort("KEY_SEQ") == 1) {
					dropForeignKeyConstraint(toConn, tableName, crefs.getString("FK_NAME"));
				}
			}

		} finally {
			crefs.close();
		}
	}

	/**
	 * @param toConn
	 * @param tableName
	 * @param currentPkTable
	 * @param pcolumns
	 * @param fcolumns
	 * @param updateRule
	 * @param deleteRule
	 * @throws SQLException 
	 */
	private void dropForeignKeyConstraint(Connection toConn, String tableName,
										  String fkName)
	throws SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("alter table ").append(tableName).append(" drop constraint "+fkName);

		Statement stmnt = toConn.createStatement();
		try {
			log.debug(sb);
			stmnt.execute(sb.toString());
		} finally {
			stmnt.close();
		}
	}

	/**
	 * @param fromConn
	 * @param dbMeta
	 * @param toConn
	 * @param tableName
	 * @throws SQLException 
	 */
	private void buildForeignKeyConstraints(Connection fromConn, DatabaseMetaData dbMeta,
	                                        Connection toConn, String tableName)
	throws SQLException
	{
		ResultSet crefs = dbMeta.getImportedKeys(fromConn.getCatalog(), "", tableName);
		try {
			String currentPkTable = null;
			List<String> pcolumns = new ArrayList<String>();
			List<String> fcolumns = new ArrayList<String>();
			short updateRule = 0;
			short deleteRule = 0;
			while (crefs.next()) {
				String pkTable = crefs.getString("PKTABLE_NAME");
				//log.debug("index "+indexName+", col="+indexes.getString("COLUMN_NAME")+", nuniq="+indexes.getBoolean("NON_UNIQUE"));

				if (!pkTable.equals(currentPkTable)) {
					buildForeignKeyConstraint(toConn, tableName, currentPkTable, pcolumns, fcolumns, updateRule, deleteRule);
					currentPkTable = pkTable;
					pcolumns.clear();
					fcolumns.clear();
				}

				pcolumns.add(crefs.getString("PKCOLUMN_NAME"));
				fcolumns.add(crefs.getString("FKCOLUMN_NAME"));
				updateRule = crefs.getShort("UPDATE_RULE");
				deleteRule = crefs.getShort("DELETE_RULE");
			}
			if (currentPkTable != null) {
				buildForeignKeyConstraint(toConn, tableName, currentPkTable, pcolumns, fcolumns, updateRule, deleteRule);
			}
		} finally {
			crefs.close();
		}
	}

	/**
	 * @param toConn
	 * @param tableName
	 * @param currentFkTable
	 * @param pcolumns
	 * @param fcolumns
	 * @throws SQLException 
	 */
	private void buildForeignKeyConstraint(Connection toConn, String tableName,
	                                       String pkTableName, List<String> pcolumns,
	                                       List<String> fcolumns, short updateRule, short deleteRule)
	throws SQLException
	{
		if (pkTableName == null) {
			return;
		}

		System.err.println("building foreign key constraint "+tableName+" -> "+pkTableName);
		StringBuilder sb = new StringBuilder();
		sb.append("alter table ").append(tableName).append(" add foreign key (");
		String delim = "";
		for (String col: fcolumns) {
			sb.append(delim).append(col);
			delim = ", ";
		}
		sb.append(") references ").append(pkTableName).append(" (");
		delim = "";
		for (String col: pcolumns) {
			sb.append(delim).append(col);
			delim = ", ";
		}
		sb.append(")");

		sb.append(" on delete ").append(decodeRefRule(deleteRule));
		sb.append(" on update ").append(decodeRefRule(updateRule));

		Statement stmnt = toConn.createStatement();
		try {
			log.debug(sb);
			stmnt.execute(sb.toString());
		} finally {
			stmnt.close();
		}

	}

	/**
	 * @param deleteRule
	 * @return
	 */
	private String decodeRefRule(short rule)
	{
		switch (rule) {
		case DatabaseMetaData.importedKeyNoAction:
			return "no action";
		case DatabaseMetaData.importedKeyCascade:
			return "cascade";
		case DatabaseMetaData.importedKeySetNull:
			return "set null";
		case DatabaseMetaData.importedKeySetDefault:
			return "det default";
		case DatabaseMetaData.importedKeyRestrict:
			return "restrict";
		default:
			return "restrict";
		}
	}


	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
