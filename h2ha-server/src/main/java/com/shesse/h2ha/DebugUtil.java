/**
 * (c) DICOS GmbH, 2017
 *
 * $Id$
 */

package com.shesse.h2ha;

/**
 *
 * @author sth
 */
public class DebugUtil
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	//private static Logger log = LoggerFactory.getLogger(DebugUtil.class);


	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 */
	private DebugUtil()
	{
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * 
	 */
	public static String debugId(Object obj)
	{
		if (obj == null) {
			return "null";
		} else {
			Class<?> cls = obj.getClass();
			return cls.getName().replaceFirst(".*\\.", "")+"@"+Integer.toHexString(System.identityHashCode(obj));
		}
	}

	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
