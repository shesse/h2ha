/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.SQLXML;

import javax.xml.transform.Result;
import javax.xml.transform.Source;

import org.apache.log4j.Logger;

/**
 *
 * @author sth
 */
public class HaSQLXML
    implements SQLXML
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(HaSQLXML.class);

    /** */
    @SuppressWarnings("unused")
    private HaConnection haConnection;
    
    /** */
    private SQLXML base;
    


    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @param sqlxml 
     * @param haConnection 
     */
    public HaSQLXML(HaConnection haConnection, SQLXML base)
    {
	log.debug("HaSQLXML()");
	this.haConnection = haConnection;
	this.base = base;
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @return
     */
    public SQLXML getBase()
    {
	return base;
    }

    /**
     * @throws SQLException
     * @see java.sql.SQLXML#free()
     */
    public void free()
	throws SQLException
    {
	base.free();
    }

    /**
     * @return
     * @throws SQLException
     * @see java.sql.SQLXML#getBinaryStream()
     */
    public InputStream getBinaryStream()
	throws SQLException
    {
	return base.getBinaryStream();
    }

    /**
     * @return
     * @throws SQLException
     * @see java.sql.SQLXML#setBinaryStream()
     */
    public OutputStream setBinaryStream()
	throws SQLException
    {
	return base.setBinaryStream();
    }

    /**
     * @return
     * @throws SQLException
     * @see java.sql.SQLXML#getCharacterStream()
     */
    public Reader getCharacterStream()
	throws SQLException
    {
	return base.getCharacterStream();
    }

    /**
     * @return
     * @throws SQLException
     * @see java.sql.SQLXML#setCharacterStream()
     */
    public Writer setCharacterStream()
	throws SQLException
    {
	return base.setCharacterStream();
    }

    /**
     * @return
     * @throws SQLException
     * @see java.sql.SQLXML#getString()
     */
    public String getString()
	throws SQLException
    {
	return base.getString();
    }

    /**
     * @param value
     * @throws SQLException
     * @see java.sql.SQLXML#setString(java.lang.String)
     */
    public void setString(String value)
	throws SQLException
    {
	base.setString(value);
    }

    /**
     * @param <T>
     * @param sourceClass
     * @return
     * @throws SQLException
     * @see java.sql.SQLXML#getSource(java.lang.Class)
     */
    public <T extends Source> T getSource(Class<T> sourceClass)
	throws SQLException
    {
	return base.getSource(sourceClass);
    }

    /**
     * @param <T>
     * @param resultClass
     * @return
     * @throws SQLException
     * @see java.sql.SQLXML#setResult(java.lang.Class)
     */
    public <T extends Result> T setResult(Class<T> resultClass)
	throws SQLException
    {
	return base.setResult(resultClass);
    }




    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
