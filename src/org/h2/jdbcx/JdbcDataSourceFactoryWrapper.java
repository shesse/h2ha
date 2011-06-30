/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package org.h2.jdbcx;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;

/**
 * 
 * @author sth
 */
public class JdbcDataSourceFactoryWrapper
    extends JdbcDataSourceFactory
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    // private static Logger log =
    // Logger.getLogger(JdbcDataSourceFactoryWrapper.class);


    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public JdbcDataSourceFactoryWrapper()
    {
	// log.debug("JdbcDataSourceFactoryWrapper()");
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    public synchronized Object getObjectInstance(Object obj, Name name, Context nameCtx,
						 Hashtable<?, ?> environment)
    {
	if (obj instanceof Reference) {
	    Reference ref = (Reference) obj;
	    if (ref.getClassName().equals(JdbcDataSourceWrapper.class.getName())) {
		JdbcDataSource dataSource = new JdbcDataSourceWrapper();
		dataSource.setURL((String) ref.get("url").getContent());
		dataSource.setUser((String) ref.get("user").getContent());
		dataSource.setPassword((String) ref.get("password").getContent());
		dataSource.setDescription((String) ref.get("description").getContent());
		String s = (String) ref.get("loginTimeout").getContent();
		dataSource.setLoginTimeout(Integer.parseInt(s));
		return dataSource;
	    }
	}
	return super.getObjectInstance(obj, name, nameCtx, environment);
    }


    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
