/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 *
 * @author sth
 */
public class HaDatabaseMetaData
    implements DatabaseMetaData
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    //private static Logger log = Logger.getLogger(HaDatabaseMetaData.class);

    /** */
    private HaConnection haConnection;
    
    /** */
    private DatabaseMetaData base;
    

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @param databaseMetaData 
     * @param haConnection 
     */
    public HaDatabaseMetaData(HaConnection haConnection, DatabaseMetaData base)
    {
	this.haConnection = haConnection;
	this.base = base;
    }


    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @param <T>
     * @param iface
     * @return
     * @throws SQLException
     * @see java.sql.Wrapper#unwrap(java.lang.Class)
     */
    public <T> T unwrap(Class<T> iface)
	throws SQLException
    {
	return base.unwrap(iface);
    }


    /**
     * @param iface
     * @return
     * @throws SQLException
     * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
     */
    public boolean isWrapperFor(Class<?> iface)
	throws SQLException
    {
	return base.isWrapperFor(iface);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#allProceduresAreCallable()
     */
    public boolean allProceduresAreCallable()
	throws SQLException
    {
	return base.allProceduresAreCallable();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#allTablesAreSelectable()
     */
    public boolean allTablesAreSelectable()
	throws SQLException
    {
	return base.allTablesAreSelectable();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getURL()
     */
    public String getURL()
	throws SQLException
    {
	return base.getURL();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getUserName()
     */
    public String getUserName()
	throws SQLException
    {
	return base.getUserName();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#isReadOnly()
     */
    public boolean isReadOnly()
	throws SQLException
    {
	return base.isReadOnly();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#nullsAreSortedHigh()
     */
    public boolean nullsAreSortedHigh()
	throws SQLException
    {
	return base.nullsAreSortedHigh();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#nullsAreSortedLow()
     */
    public boolean nullsAreSortedLow()
	throws SQLException
    {
	return base.nullsAreSortedLow();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#nullsAreSortedAtStart()
     */
    public boolean nullsAreSortedAtStart()
	throws SQLException
    {
	return base.nullsAreSortedAtStart();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#nullsAreSortedAtEnd()
     */
    public boolean nullsAreSortedAtEnd()
	throws SQLException
    {
	return base.nullsAreSortedAtEnd();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getDatabaseProductName()
     */
    public String getDatabaseProductName()
	throws SQLException
    {
	return base.getDatabaseProductName();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getDatabaseProductVersion()
     */
    public String getDatabaseProductVersion()
	throws SQLException
    {
	return base.getDatabaseProductVersion();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getDriverName()
     */
    public String getDriverName()
	throws SQLException
    {
	return base.getDriverName();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getDriverVersion()
     */
    public String getDriverVersion()
	throws SQLException
    {
	return base.getDriverVersion();
    }


    /**
     * @return
     * @see java.sql.DatabaseMetaData#getDriverMajorVersion()
     */
    public int getDriverMajorVersion()
    {
	return base.getDriverMajorVersion();
    }


    /**
     * @return
     * @see java.sql.DatabaseMetaData#getDriverMinorVersion()
     */
    public int getDriverMinorVersion()
    {
	return base.getDriverMinorVersion();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#usesLocalFiles()
     */
    public boolean usesLocalFiles()
	throws SQLException
    {
	return base.usesLocalFiles();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#usesLocalFilePerTable()
     */
    public boolean usesLocalFilePerTable()
	throws SQLException
    {
	return base.usesLocalFilePerTable();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsMixedCaseIdentifiers()
     */
    public boolean supportsMixedCaseIdentifiers()
	throws SQLException
    {
	return base.supportsMixedCaseIdentifiers();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#storesUpperCaseIdentifiers()
     */
    public boolean storesUpperCaseIdentifiers()
	throws SQLException
    {
	return base.storesUpperCaseIdentifiers();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#storesLowerCaseIdentifiers()
     */
    public boolean storesLowerCaseIdentifiers()
	throws SQLException
    {
	return base.storesLowerCaseIdentifiers();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#storesMixedCaseIdentifiers()
     */
    public boolean storesMixedCaseIdentifiers()
	throws SQLException
    {
	return base.storesMixedCaseIdentifiers();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsMixedCaseQuotedIdentifiers()
     */
    public boolean supportsMixedCaseQuotedIdentifiers()
	throws SQLException
    {
	return base.supportsMixedCaseQuotedIdentifiers();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#storesUpperCaseQuotedIdentifiers()
     */
    public boolean storesUpperCaseQuotedIdentifiers()
	throws SQLException
    {
	return base.storesUpperCaseQuotedIdentifiers();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#storesLowerCaseQuotedIdentifiers()
     */
    public boolean storesLowerCaseQuotedIdentifiers()
	throws SQLException
    {
	return base.storesLowerCaseQuotedIdentifiers();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#storesMixedCaseQuotedIdentifiers()
     */
    public boolean storesMixedCaseQuotedIdentifiers()
	throws SQLException
    {
	return base.storesMixedCaseQuotedIdentifiers();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getIdentifierQuoteString()
     */
    public String getIdentifierQuoteString()
	throws SQLException
    {
	return base.getIdentifierQuoteString();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getSQLKeywords()
     */
    public String getSQLKeywords()
	throws SQLException
    {
	return base.getSQLKeywords();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getNumericFunctions()
     */
    public String getNumericFunctions()
	throws SQLException
    {
	return base.getNumericFunctions();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getStringFunctions()
     */
    public String getStringFunctions()
	throws SQLException
    {
	return base.getStringFunctions();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getSystemFunctions()
     */
    public String getSystemFunctions()
	throws SQLException
    {
	return base.getSystemFunctions();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getTimeDateFunctions()
     */
    public String getTimeDateFunctions()
	throws SQLException
    {
	return base.getTimeDateFunctions();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getSearchStringEscape()
     */
    public String getSearchStringEscape()
	throws SQLException
    {
	return base.getSearchStringEscape();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getExtraNameCharacters()
     */
    public String getExtraNameCharacters()
	throws SQLException
    {
	return base.getExtraNameCharacters();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsAlterTableWithAddColumn()
     */
    public boolean supportsAlterTableWithAddColumn()
	throws SQLException
    {
	return base.supportsAlterTableWithAddColumn();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsAlterTableWithDropColumn()
     */
    public boolean supportsAlterTableWithDropColumn()
	throws SQLException
    {
	return base.supportsAlterTableWithDropColumn();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsColumnAliasing()
     */
    public boolean supportsColumnAliasing()
	throws SQLException
    {
	return base.supportsColumnAliasing();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#nullPlusNonNullIsNull()
     */
    public boolean nullPlusNonNullIsNull()
	throws SQLException
    {
	return base.nullPlusNonNullIsNull();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsConvert()
     */
    public boolean supportsConvert()
	throws SQLException
    {
	return base.supportsConvert();
    }


    /**
     * @param fromType
     * @param toType
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsConvert(int, int)
     */
    public boolean supportsConvert(int fromType, int toType)
	throws SQLException
    {
	return base.supportsConvert(fromType, toType);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsTableCorrelationNames()
     */
    public boolean supportsTableCorrelationNames()
	throws SQLException
    {
	return base.supportsTableCorrelationNames();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsDifferentTableCorrelationNames()
     */
    public boolean supportsDifferentTableCorrelationNames()
	throws SQLException
    {
	return base.supportsDifferentTableCorrelationNames();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsExpressionsInOrderBy()
     */
    public boolean supportsExpressionsInOrderBy()
	throws SQLException
    {
	return base.supportsExpressionsInOrderBy();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsOrderByUnrelated()
     */
    public boolean supportsOrderByUnrelated()
	throws SQLException
    {
	return base.supportsOrderByUnrelated();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsGroupBy()
     */
    public boolean supportsGroupBy()
	throws SQLException
    {
	return base.supportsGroupBy();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsGroupByUnrelated()
     */
    public boolean supportsGroupByUnrelated()
	throws SQLException
    {
	return base.supportsGroupByUnrelated();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsGroupByBeyondSelect()
     */
    public boolean supportsGroupByBeyondSelect()
	throws SQLException
    {
	return base.supportsGroupByBeyondSelect();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsLikeEscapeClause()
     */
    public boolean supportsLikeEscapeClause()
	throws SQLException
    {
	return base.supportsLikeEscapeClause();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsMultipleResultSets()
     */
    public boolean supportsMultipleResultSets()
	throws SQLException
    {
	return base.supportsMultipleResultSets();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsMultipleTransactions()
     */
    public boolean supportsMultipleTransactions()
	throws SQLException
    {
	return base.supportsMultipleTransactions();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsNonNullableColumns()
     */
    public boolean supportsNonNullableColumns()
	throws SQLException
    {
	return base.supportsNonNullableColumns();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsMinimumSQLGrammar()
     */
    public boolean supportsMinimumSQLGrammar()
	throws SQLException
    {
	return base.supportsMinimumSQLGrammar();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsCoreSQLGrammar()
     */
    public boolean supportsCoreSQLGrammar()
	throws SQLException
    {
	return base.supportsCoreSQLGrammar();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsExtendedSQLGrammar()
     */
    public boolean supportsExtendedSQLGrammar()
	throws SQLException
    {
	return base.supportsExtendedSQLGrammar();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsANSI92EntryLevelSQL()
     */
    public boolean supportsANSI92EntryLevelSQL()
	throws SQLException
    {
	return base.supportsANSI92EntryLevelSQL();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsANSI92IntermediateSQL()
     */
    public boolean supportsANSI92IntermediateSQL()
	throws SQLException
    {
	return base.supportsANSI92IntermediateSQL();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsANSI92FullSQL()
     */
    public boolean supportsANSI92FullSQL()
	throws SQLException
    {
	return base.supportsANSI92FullSQL();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsIntegrityEnhancementFacility()
     */
    public boolean supportsIntegrityEnhancementFacility()
	throws SQLException
    {
	return base.supportsIntegrityEnhancementFacility();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsOuterJoins()
     */
    public boolean supportsOuterJoins()
	throws SQLException
    {
	return base.supportsOuterJoins();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsFullOuterJoins()
     */
    public boolean supportsFullOuterJoins()
	throws SQLException
    {
	return base.supportsFullOuterJoins();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsLimitedOuterJoins()
     */
    public boolean supportsLimitedOuterJoins()
	throws SQLException
    {
	return base.supportsLimitedOuterJoins();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getSchemaTerm()
     */
    public String getSchemaTerm()
	throws SQLException
    {
	return base.getSchemaTerm();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getProcedureTerm()
     */
    public String getProcedureTerm()
	throws SQLException
    {
	return base.getProcedureTerm();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getCatalogTerm()
     */
    public String getCatalogTerm()
	throws SQLException
    {
	return base.getCatalogTerm();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#isCatalogAtStart()
     */
    public boolean isCatalogAtStart()
	throws SQLException
    {
	return base.isCatalogAtStart();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getCatalogSeparator()
     */
    public String getCatalogSeparator()
	throws SQLException
    {
	return base.getCatalogSeparator();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsSchemasInDataManipulation()
     */
    public boolean supportsSchemasInDataManipulation()
	throws SQLException
    {
	return base.supportsSchemasInDataManipulation();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsSchemasInProcedureCalls()
     */
    public boolean supportsSchemasInProcedureCalls()
	throws SQLException
    {
	return base.supportsSchemasInProcedureCalls();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsSchemasInTableDefinitions()
     */
    public boolean supportsSchemasInTableDefinitions()
	throws SQLException
    {
	return base.supportsSchemasInTableDefinitions();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsSchemasInIndexDefinitions()
     */
    public boolean supportsSchemasInIndexDefinitions()
	throws SQLException
    {
	return base.supportsSchemasInIndexDefinitions();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsSchemasInPrivilegeDefinitions()
     */
    public boolean supportsSchemasInPrivilegeDefinitions()
	throws SQLException
    {
	return base.supportsSchemasInPrivilegeDefinitions();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsCatalogsInDataManipulation()
     */
    public boolean supportsCatalogsInDataManipulation()
	throws SQLException
    {
	return base.supportsCatalogsInDataManipulation();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsCatalogsInProcedureCalls()
     */
    public boolean supportsCatalogsInProcedureCalls()
	throws SQLException
    {
	return base.supportsCatalogsInProcedureCalls();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsCatalogsInTableDefinitions()
     */
    public boolean supportsCatalogsInTableDefinitions()
	throws SQLException
    {
	return base.supportsCatalogsInTableDefinitions();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsCatalogsInIndexDefinitions()
     */
    public boolean supportsCatalogsInIndexDefinitions()
	throws SQLException
    {
	return base.supportsCatalogsInIndexDefinitions();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsCatalogsInPrivilegeDefinitions()
     */
    public boolean supportsCatalogsInPrivilegeDefinitions()
	throws SQLException
    {
	return base.supportsCatalogsInPrivilegeDefinitions();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsPositionedDelete()
     */
    public boolean supportsPositionedDelete()
	throws SQLException
    {
	return base.supportsPositionedDelete();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsPositionedUpdate()
     */
    public boolean supportsPositionedUpdate()
	throws SQLException
    {
	return base.supportsPositionedUpdate();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsSelectForUpdate()
     */
    public boolean supportsSelectForUpdate()
	throws SQLException
    {
	return base.supportsSelectForUpdate();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsStoredProcedures()
     */
    public boolean supportsStoredProcedures()
	throws SQLException
    {
	return base.supportsStoredProcedures();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsSubqueriesInComparisons()
     */
    public boolean supportsSubqueriesInComparisons()
	throws SQLException
    {
	return base.supportsSubqueriesInComparisons();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsSubqueriesInExists()
     */
    public boolean supportsSubqueriesInExists()
	throws SQLException
    {
	return base.supportsSubqueriesInExists();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsSubqueriesInIns()
     */
    public boolean supportsSubqueriesInIns()
	throws SQLException
    {
	return base.supportsSubqueriesInIns();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsSubqueriesInQuantifieds()
     */
    public boolean supportsSubqueriesInQuantifieds()
	throws SQLException
    {
	return base.supportsSubqueriesInQuantifieds();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsCorrelatedSubqueries()
     */
    public boolean supportsCorrelatedSubqueries()
	throws SQLException
    {
	return base.supportsCorrelatedSubqueries();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsUnion()
     */
    public boolean supportsUnion()
	throws SQLException
    {
	return base.supportsUnion();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsUnionAll()
     */
    public boolean supportsUnionAll()
	throws SQLException
    {
	return base.supportsUnionAll();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsOpenCursorsAcrossCommit()
     */
    public boolean supportsOpenCursorsAcrossCommit()
	throws SQLException
    {
	return base.supportsOpenCursorsAcrossCommit();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsOpenCursorsAcrossRollback()
     */
    public boolean supportsOpenCursorsAcrossRollback()
	throws SQLException
    {
	return base.supportsOpenCursorsAcrossRollback();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsOpenStatementsAcrossCommit()
     */
    public boolean supportsOpenStatementsAcrossCommit()
	throws SQLException
    {
	return base.supportsOpenStatementsAcrossCommit();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsOpenStatementsAcrossRollback()
     */
    public boolean supportsOpenStatementsAcrossRollback()
	throws SQLException
    {
	return base.supportsOpenStatementsAcrossRollback();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxBinaryLiteralLength()
     */
    public int getMaxBinaryLiteralLength()
	throws SQLException
    {
	return base.getMaxBinaryLiteralLength();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxCharLiteralLength()
     */
    public int getMaxCharLiteralLength()
	throws SQLException
    {
	return base.getMaxCharLiteralLength();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxColumnNameLength()
     */
    public int getMaxColumnNameLength()
	throws SQLException
    {
	return base.getMaxColumnNameLength();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxColumnsInGroupBy()
     */
    public int getMaxColumnsInGroupBy()
	throws SQLException
    {
	return base.getMaxColumnsInGroupBy();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxColumnsInIndex()
     */
    public int getMaxColumnsInIndex()
	throws SQLException
    {
	return base.getMaxColumnsInIndex();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxColumnsInOrderBy()
     */
    public int getMaxColumnsInOrderBy()
	throws SQLException
    {
	return base.getMaxColumnsInOrderBy();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxColumnsInSelect()
     */
    public int getMaxColumnsInSelect()
	throws SQLException
    {
	return base.getMaxColumnsInSelect();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxColumnsInTable()
     */
    public int getMaxColumnsInTable()
	throws SQLException
    {
	return base.getMaxColumnsInTable();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxConnections()
     */
    public int getMaxConnections()
	throws SQLException
    {
	return base.getMaxConnections();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxCursorNameLength()
     */
    public int getMaxCursorNameLength()
	throws SQLException
    {
	return base.getMaxCursorNameLength();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxIndexLength()
     */
    public int getMaxIndexLength()
	throws SQLException
    {
	return base.getMaxIndexLength();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxSchemaNameLength()
     */
    public int getMaxSchemaNameLength()
	throws SQLException
    {
	return base.getMaxSchemaNameLength();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxProcedureNameLength()
     */
    public int getMaxProcedureNameLength()
	throws SQLException
    {
	return base.getMaxProcedureNameLength();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxCatalogNameLength()
     */
    public int getMaxCatalogNameLength()
	throws SQLException
    {
	return base.getMaxCatalogNameLength();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxRowSize()
     */
    public int getMaxRowSize()
	throws SQLException
    {
	return base.getMaxRowSize();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#doesMaxRowSizeIncludeBlobs()
     */
    public boolean doesMaxRowSizeIncludeBlobs()
	throws SQLException
    {
	return base.doesMaxRowSizeIncludeBlobs();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxStatementLength()
     */
    public int getMaxStatementLength()
	throws SQLException
    {
	return base.getMaxStatementLength();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxStatements()
     */
    public int getMaxStatements()
	throws SQLException
    {
	return base.getMaxStatements();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxTableNameLength()
     */
    public int getMaxTableNameLength()
	throws SQLException
    {
	return base.getMaxTableNameLength();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxTablesInSelect()
     */
    public int getMaxTablesInSelect()
	throws SQLException
    {
	return base.getMaxTablesInSelect();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getMaxUserNameLength()
     */
    public int getMaxUserNameLength()
	throws SQLException
    {
	return base.getMaxUserNameLength();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getDefaultTransactionIsolation()
     */
    public int getDefaultTransactionIsolation()
	throws SQLException
    {
	return base.getDefaultTransactionIsolation();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsTransactions()
     */
    public boolean supportsTransactions()
	throws SQLException
    {
	return base.supportsTransactions();
    }


    /**
     * @param level
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsTransactionIsolationLevel(int)
     */
    public boolean supportsTransactionIsolationLevel(int level)
	throws SQLException
    {
	return base.supportsTransactionIsolationLevel(level);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsDataDefinitionAndDataManipulationTransactions()
     */
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
	throws SQLException
    {
	return base.supportsDataDefinitionAndDataManipulationTransactions();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsDataManipulationTransactionsOnly()
     */
    public boolean supportsDataManipulationTransactionsOnly()
	throws SQLException
    {
	return base.supportsDataManipulationTransactionsOnly();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#dataDefinitionCausesTransactionCommit()
     */
    public boolean dataDefinitionCausesTransactionCommit()
	throws SQLException
    {
	return base.dataDefinitionCausesTransactionCommit();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#dataDefinitionIgnoredInTransactions()
     */
    public boolean dataDefinitionIgnoredInTransactions()
	throws SQLException
    {
	return base.dataDefinitionIgnoredInTransactions();
    }


    /**
     * @param catalog
     * @param schemaPattern
     * @param procedureNamePattern
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getProcedures(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getProcedures(catalog, schemaPattern, procedureNamePattern));
    }


    /**
     * @param catalog
     * @param schemaPattern
     * @param procedureNamePattern
     * @param columnNamePattern
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getProcedureColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
					 String procedureNamePattern, String columnNamePattern)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getProcedureColumns(catalog, schemaPattern, procedureNamePattern,
	    columnNamePattern));
    }


    /**
     * @param catalog
     * @param schemaPattern
     * @param tableNamePattern
     * @param types
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getTables(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])
     */
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
			       String[] types)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getTables(catalog, schemaPattern, tableNamePattern, types));
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getSchemas()
     */
    public ResultSet getSchemas()
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getSchemas());
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getCatalogs()
     */
    public ResultSet getCatalogs()
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getCatalogs());
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getTableTypes()
     */
    public ResultSet getTableTypes()
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getTableTypes());
    }


    /**
     * @param catalog
     * @param schemaPattern
     * @param tableNamePattern
     * @param columnNamePattern
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
				String columnNamePattern)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern));
    }


    /**
     * @param catalog
     * @param schema
     * @param table
     * @param columnNamePattern
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getColumnPrivileges(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getColumnPrivileges(String catalog, String schema, String table,
					 String columnNamePattern)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getColumnPrivileges(catalog, schema, table, columnNamePattern));
    }


    /**
     * @param catalog
     * @param schemaPattern
     * @param tableNamePattern
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getTablePrivileges(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getTablePrivileges(String catalog, String schemaPattern,
					String tableNamePattern)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getTablePrivileges(catalog, schemaPattern, tableNamePattern));
    }


    /**
     * @param catalog
     * @param schema
     * @param table
     * @param scope
     * @param nullable
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getBestRowIdentifier(java.lang.String, java.lang.String, java.lang.String, int, boolean)
     */
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
					  boolean nullable)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getBestRowIdentifier(catalog, schema, table, scope, nullable));
    }


    /**
     * @param catalog
     * @param schema
     * @param table
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getVersionColumns(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getVersionColumns(String catalog, String schema, String table)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getVersionColumns(catalog, schema, table));
    }


    /**
     * @param catalog
     * @param schema
     * @param table
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getPrimaryKeys(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getPrimaryKeys(catalog, schema, table));
    }


    /**
     * @param catalog
     * @param schema
     * @param table
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getImportedKeys(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getImportedKeys(String catalog, String schema, String table)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getImportedKeys(catalog, schema, table));
    }


    /**
     * @param catalog
     * @param schema
     * @param table
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getExportedKeys(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getExportedKeys(String catalog, String schema, String table)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getExportedKeys(catalog, schema, table));
    }


    /**
     * @param parentCatalog
     * @param parentSchema
     * @param parentTable
     * @param foreignCatalog
     * @param foreignSchema
     * @param foreignTable
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getCrossReference(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getCrossReference(String parentCatalog, String parentSchema,
				       String parentTable, String foreignCatalog,
				       String foreignSchema, String foreignTable)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getCrossReference(parentCatalog, parentSchema, parentTable, foreignCatalog,
	    foreignSchema, foreignTable));
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getTypeInfo()
     */
    public ResultSet getTypeInfo()
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getTypeInfo());
    }


    /**
     * @param catalog
     * @param schema
     * @param table
     * @param unique
     * @param approximate
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getIndexInfo(java.lang.String, java.lang.String, java.lang.String, boolean, boolean)
     */
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
				  boolean approximate)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getIndexInfo(catalog, schema, table, unique, approximate));
    }


    /**
     * @param type
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsResultSetType(int)
     */
    public boolean supportsResultSetType(int type)
	throws SQLException
    {
	return base.supportsResultSetType(type);
    }


    /**
     * @param type
     * @param concurrency
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsResultSetConcurrency(int, int)
     */
    public boolean supportsResultSetConcurrency(int type, int concurrency)
	throws SQLException
    {
	return base.supportsResultSetConcurrency(type, concurrency);
    }


    /**
     * @param type
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#ownUpdatesAreVisible(int)
     */
    public boolean ownUpdatesAreVisible(int type)
	throws SQLException
    {
	return base.ownUpdatesAreVisible(type);
    }


    /**
     * @param type
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#ownDeletesAreVisible(int)
     */
    public boolean ownDeletesAreVisible(int type)
	throws SQLException
    {
	return base.ownDeletesAreVisible(type);
    }


    /**
     * @param type
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#ownInsertsAreVisible(int)
     */
    public boolean ownInsertsAreVisible(int type)
	throws SQLException
    {
	return base.ownInsertsAreVisible(type);
    }


    /**
     * @param type
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#othersUpdatesAreVisible(int)
     */
    public boolean othersUpdatesAreVisible(int type)
	throws SQLException
    {
	return base.othersUpdatesAreVisible(type);
    }


    /**
     * @param type
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#othersDeletesAreVisible(int)
     */
    public boolean othersDeletesAreVisible(int type)
	throws SQLException
    {
	return base.othersDeletesAreVisible(type);
    }


    /**
     * @param type
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#othersInsertsAreVisible(int)
     */
    public boolean othersInsertsAreVisible(int type)
	throws SQLException
    {
	return base.othersInsertsAreVisible(type);
    }


    /**
     * @param type
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#updatesAreDetected(int)
     */
    public boolean updatesAreDetected(int type)
	throws SQLException
    {
	return base.updatesAreDetected(type);
    }


    /**
     * @param type
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#deletesAreDetected(int)
     */
    public boolean deletesAreDetected(int type)
	throws SQLException
    {
	return base.deletesAreDetected(type);
    }


    /**
     * @param type
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#insertsAreDetected(int)
     */
    public boolean insertsAreDetected(int type)
	throws SQLException
    {
	return base.insertsAreDetected(type);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsBatchUpdates()
     */
    public boolean supportsBatchUpdates()
	throws SQLException
    {
	return base.supportsBatchUpdates();
    }


    /**
     * @param catalog
     * @param schemaPattern
     * @param typeNamePattern
     * @param types
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getUDTs(java.lang.String, java.lang.String, java.lang.String, int[])
     */
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
			     int[] types)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getUDTs(catalog, schemaPattern, typeNamePattern, types));
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getConnection()
     */
    public Connection getConnection()
	throws SQLException
    {
	return haConnection;
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsSavepoints()
     */
    public boolean supportsSavepoints()
	throws SQLException
    {
	return base.supportsSavepoints();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsNamedParameters()
     */
    public boolean supportsNamedParameters()
	throws SQLException
    {
	return base.supportsNamedParameters();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsMultipleOpenResults()
     */
    public boolean supportsMultipleOpenResults()
	throws SQLException
    {
	return base.supportsMultipleOpenResults();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsGetGeneratedKeys()
     */
    public boolean supportsGetGeneratedKeys()
	throws SQLException
    {
	return base.supportsGetGeneratedKeys();
    }


    /**
     * @param catalog
     * @param schemaPattern
     * @param typeNamePattern
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getSuperTypes(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getSuperTypes(catalog, schemaPattern, typeNamePattern));
    }


    /**
     * @param catalog
     * @param schemaPattern
     * @param tableNamePattern
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getSuperTables(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getSuperTables(catalog, schemaPattern, tableNamePattern));
    }


    /**
     * @param catalog
     * @param schemaPattern
     * @param typeNamePattern
     * @param attributeNamePattern
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getAttributes(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
				   String attributeNamePattern)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern));
    }


    /**
     * @param holdability
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsResultSetHoldability(int)
     */
    public boolean supportsResultSetHoldability(int holdability)
	throws SQLException
    {
	return base.supportsResultSetHoldability(holdability);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getResultSetHoldability()
     */
    public int getResultSetHoldability()
	throws SQLException
    {
	return base.getResultSetHoldability();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getDatabaseMajorVersion()
     */
    public int getDatabaseMajorVersion()
	throws SQLException
    {
	return base.getDatabaseMajorVersion();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getDatabaseMinorVersion()
     */
    public int getDatabaseMinorVersion()
	throws SQLException
    {
	return base.getDatabaseMinorVersion();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getJDBCMajorVersion()
     */
    public int getJDBCMajorVersion()
	throws SQLException
    {
	return base.getJDBCMajorVersion();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getJDBCMinorVersion()
     */
    public int getJDBCMinorVersion()
	throws SQLException
    {
	return base.getJDBCMinorVersion();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getSQLStateType()
     */
    public int getSQLStateType()
	throws SQLException
    {
	return base.getSQLStateType();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#locatorsUpdateCopy()
     */
    public boolean locatorsUpdateCopy()
	throws SQLException
    {
	return base.locatorsUpdateCopy();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsStatementPooling()
     */
    public boolean supportsStatementPooling()
	throws SQLException
    {
	return base.supportsStatementPooling();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getRowIdLifetime()
     */
    public RowIdLifetime getRowIdLifetime()
	throws SQLException
    {
	return base.getRowIdLifetime();
    }


    /**
     * @param catalog
     * @param schemaPattern
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getSchemas(java.lang.String, java.lang.String)
     */
    public ResultSet getSchemas(String catalog, String schemaPattern)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getSchemas(catalog, schemaPattern));
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#supportsStoredFunctionsUsingCallSyntax()
     */
    public boolean supportsStoredFunctionsUsingCallSyntax()
	throws SQLException
    {
	return base.supportsStoredFunctionsUsingCallSyntax();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#autoCommitFailureClosesAllResultSets()
     */
    public boolean autoCommitFailureClosesAllResultSets()
	throws SQLException
    {
	return base.autoCommitFailureClosesAllResultSets();
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getClientInfoProperties()
     */
    public ResultSet getClientInfoProperties()
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getClientInfoProperties());
    }


    /**
     * @param catalog
     * @param schemaPattern
     * @param functionNamePattern
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getFunctions(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getFunctions(catalog, schemaPattern, functionNamePattern));
    }


    /**
     * @param catalog
     * @param schemaPattern
     * @param functionNamePattern
     * @param columnNamePattern
     * @return
     * @throws SQLException
     * @see java.sql.DatabaseMetaData#getFunctionColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
					String functionNamePattern, String columnNamePattern)
	throws SQLException
    {
	return new HaResultSet(haConnection, null, base.getFunctionColumns(catalog, schemaPattern, functionNamePattern,
	    columnNamePattern));
    }


	/**
	 * {@inheritDoc}
	 *
	 * @see java.sql.DatabaseMetaData#getPseudoColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	public ResultSet getPseudoColumns(String catalog, String schemaPattern,
									  String tableNamePattern, String columnNamePattern)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("");
	}


	/**
	 * {@inheritDoc}
	 *
	 * @see java.sql.DatabaseMetaData#generatedKeyAlwaysReturned()
	 */
	public boolean generatedKeyAlwaysReturned()
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("");
	}




    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
