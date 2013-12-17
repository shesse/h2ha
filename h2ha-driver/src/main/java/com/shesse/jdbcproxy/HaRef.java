/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.sql.Ref;
import java.sql.SQLException;
import java.util.Map;

/**
 * 
 * @author sth
 */
public class HaRef
	implements Ref
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	// private static Logger log = Logger.getLogger(HaRef.class);

	/** */
	@SuppressWarnings("unused")
	private HaConnection haConnection;

	/** */
	private Ref base;


	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @param ref
	 * @param haConnection
	 */
	public HaRef(HaConnection haConnection, Ref base)
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
	public Ref getBase()
	{
		return base;
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Ref#getBaseTypeName()
	 */
	public String getBaseTypeName()
		throws SQLException
	{
		return base.getBaseTypeName();
	}


	/**
	 * @param map
	 * @return
	 * @throws SQLException
	 * @see java.sql.Ref#getObject(java.util.Map)
	 */
	public Object getObject(Map<String, Class<?>> map)
		throws SQLException
	{
		return base.getObject(map);
	}


	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Ref#getObject()
	 */
	public Object getObject()
		throws SQLException
	{
		return base.getObject();
	}


	/**
	 * @param value
	 * @throws SQLException
	 * @see java.sql.Ref#setObject(java.lang.Object)
	 */
	public void setObject(Object value)
		throws SQLException
	{
		base.setObject(value);
	}


	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
