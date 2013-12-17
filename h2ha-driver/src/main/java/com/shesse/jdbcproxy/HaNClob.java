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
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;

/**
 * 
 * @author sth
 */
public class HaNClob
	implements NClob
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	// private static Logger log = Logger.getLogger(HaNClob.class);

	/** */
	@SuppressWarnings("unused")
	private HaConnection haConnection;

	/** */
	private NClob base;


	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @param nClob
	 * @param haConnection
	 */
	public HaNClob(HaConnection haConnection, NClob base)
	{
		this.haConnection = haConnection;
		this.base = base;
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * @return
	 */
	public NClob getBase()
	{
		return base;
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Clob#length()
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
	 * @see java.sql.Clob#getSubString(long, int)
	 */
	public String getSubString(long pos, int length)
		throws SQLException
	{
		return base.getSubString(pos, length);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Clob#getCharacterStream()
	 */
	public Reader getCharacterStream()
		throws SQLException
	{
		return base.getCharacterStream();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Clob#getAsciiStream()
	 */
	public InputStream getAsciiStream()
		throws SQLException
	{
		return base.getAsciiStream();
	}

	/**
	 * @param searchstr
	 * @param start
	 * @return
	 * @throws SQLException
	 * @see java.sql.Clob#position(java.lang.String, long)
	 */
	public long position(String searchstr, long start)
		throws SQLException
	{
		return base.position(searchstr, start);
	}

	/**
	 * @param searchstr
	 * @param start
	 * @return
	 * @throws SQLException
	 * @see java.sql.Clob#position(java.sql.Clob, long)
	 */
	public long position(Clob searchstr, long start)
		throws SQLException
	{
		if (searchstr instanceof HaClob) {
			return base.position(((HaClob) searchstr).getBase(), start);
		} else {
			return base.position(searchstr, start);
		}
	}

	/**
	 * @param pos
	 * @param str
	 * @return
	 * @throws SQLException
	 * @see java.sql.Clob#setString(long, java.lang.String)
	 */
	public int setString(long pos, String str)
		throws SQLException
	{
		return base.setString(pos, str);
	}

	/**
	 * @param pos
	 * @param str
	 * @param offset
	 * @param len
	 * @return
	 * @throws SQLException
	 * @see java.sql.Clob#setString(long, java.lang.String, int, int)
	 */
	public int setString(long pos, String str, int offset, int len)
		throws SQLException
	{
		return base.setString(pos, str, offset, len);
	}

	/**
	 * @param pos
	 * @return
	 * @throws SQLException
	 * @see java.sql.Clob#setAsciiStream(long)
	 */
	public OutputStream setAsciiStream(long pos)
		throws SQLException
	{
		return base.setAsciiStream(pos);
	}

	/**
	 * @param pos
	 * @return
	 * @throws SQLException
	 * @see java.sql.Clob#setCharacterStream(long)
	 */
	public Writer setCharacterStream(long pos)
		throws SQLException
	{
		return base.setCharacterStream(pos);
	}

	/**
	 * @param len
	 * @throws SQLException
	 * @see java.sql.Clob#truncate(long)
	 */
	public void truncate(long len)
		throws SQLException
	{
		base.truncate(len);
	}

	/**
	 * @throws SQLException
	 * @see java.sql.Clob#free()
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
	 * @see java.sql.Clob#getCharacterStream(long, long)
	 */
	public Reader getCharacterStream(long pos, long length)
		throws SQLException
	{
		return base.getCharacterStream(pos, length);
	}


	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
