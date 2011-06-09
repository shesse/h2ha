/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import org.apache.log4j.Logger;

/**
 *
 * @author sth
 */
public class HaBlob
    implements Blob
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(HaBlob.class);

    /** */
    @SuppressWarnings("unused")
    private HaConnection haConnection;
    
    /** */
    private Blob base;
    

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @param blob 
     * @param haConnection 
     */
    public HaBlob(HaConnection haConnection, Blob base)
    {
	log.debug("HaBlob()");
	this.haConnection = haConnection;
	this.base = base;
    }


    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * @return
     */
    public Blob getBase()
    {
        return base;
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Blob#length()
     */
    public long length()
	throws SQLException
    {
	return base.length();
    }


    /**
     * @param pos
     * @param length
     * @return
     * @throws SQLException
     * @see java.sql.Blob#getBytes(long, int)
     */
    public byte[] getBytes(long pos, int length)
	throws SQLException
    {
	return base.getBytes(pos, length);
    }


    /**
     * @return
     * @throws SQLException
     * @see java.sql.Blob#getBinaryStream()
     */
    public InputStream getBinaryStream()
	throws SQLException
    {
	return base.getBinaryStream();
    }


    /**
     * @param pattern
     * @param start
     * @return
     * @throws SQLException
     * @see java.sql.Blob#position(byte[], long)
     */
    public long position(byte[] pattern, long start)
	throws SQLException
    {
	return base.position(pattern, start);
    }


    /**
     * @param pattern
     * @param start
     * @return
     * @throws SQLException
     * @see java.sql.Blob#position(java.sql.Blob, long)
     */
    public long position(Blob pattern, long start)
    throws SQLException
    {
	if (pattern instanceof HaBlob) {
	    return base.position(((HaBlob)pattern).getBase(), start);
	} else {
	    return base.position(pattern, start);
	}
    }


    /**
     * @param pos
     * @param bytes
     * @return
     * @throws SQLException
     * @see java.sql.Blob#setBytes(long, byte[])
     */
    public int setBytes(long pos, byte[] bytes)
	throws SQLException
    {
	return base.setBytes(pos, bytes);
    }


    /**
     * @param pos
     * @param bytes
     * @param offset
     * @param len
     * @return
     * @throws SQLException
     * @see java.sql.Blob#setBytes(long, byte[], int, int)
     */
    public int setBytes(long pos, byte[] bytes, int offset, int len)
	throws SQLException
    {
	return base.setBytes(pos, bytes, offset, len);
    }


    /**
     * @param pos
     * @return
     * @throws SQLException
     * @see java.sql.Blob#setBinaryStream(long)
     */
    public OutputStream setBinaryStream(long pos)
	throws SQLException
    {
	return base.setBinaryStream(pos);
    }


    /**
     * @param len
     * @throws SQLException
     * @see java.sql.Blob#truncate(long)
     */
    public void truncate(long len)
	throws SQLException
    {
	base.truncate(len);
    }


    /**
     * @throws SQLException
     * @see java.sql.Blob#free()
     */
    public void free()
	throws SQLException
    {
	base.free();
    }


    /**
     * @param pos
     * @param length
     * @return
     * @throws SQLException
     * @see java.sql.Blob#getBinaryStream(long, long)
     */
    public InputStream getBinaryStream(long pos, long length)
	throws SQLException
    {
	return base.getBinaryStream(pos, length);
    }




    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
