/**
 * (c) St. Hesse,   2008
 *
 * $Id: FileInfo.java 142 2011-05-25 13:30:34Z sth $
 */

package org.h2.jdbc;

import org.h2.message.Trace;

/**
 * Represents the meta data for a database.
 */
public class JdbcDatabaseMetaDataWrapper
extends JdbcDatabaseMetaData
{

    private JdbcConnectionWrapper conn;

    JdbcDatabaseMetaDataWrapper(JdbcConnectionWrapper conn, Trace trace, int id)
    {
	super(conn, trace, id);
	this.conn = conn;
    }

    public boolean supportsMixedCaseIdentifiers()
    {
	debugCodeCall("supportsMixedCaseIdentifiers");
	return !isDatabaseToUpper(); 
    }
    
    private boolean isDatabaseToUpper()
    {
	String url = conn.connectionInfo.getOriginalURL();
	final String toUpper = ";DATABASE_TO_UPPER=";
	int tui = url.indexOf(toUpper);
	if (tui < 0) {
	    return true;
	}

	int etuv = url.indexOf(";", tui+1);
	if (etuv < 0) {
	    etuv = url.length();
	}

	String tuv = url.substring(tui+toUpper.length(), etuv);

	return Boolean.parseBoolean(tuv);
    }

    public boolean storesUpperCaseIdentifiers()
    {
	debugCodeCall("storesUpperCaseIdentifiers");
	return isDatabaseToUpper();
    }

    public boolean storesLowerCaseIdentifiers()
    {
	debugCodeCall("storesLowerCaseIdentifiers");
	return false;
    }
    
    public boolean storesMixedCaseIdentifiers()
    {
	debugCodeCall("storesMixedCaseIdentifiers");
	return !isDatabaseToUpper();
    }

}
