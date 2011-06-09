/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import org.apache.log4j.Logger;
import org.h2.jdbcx.JdbcDataSource;

/**
 *
 * @author sth
 */
public class HaDataSourceFactory
    implements ObjectFactory
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(HaDataSourceFactory.class);


    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public HaDataSourceFactory()
    {
	log.debug("HaDataSourceFactory()");
    }


    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * {@inheritDoc}
     *
     * @see javax.naming.spi.ObjectFactory#getObjectInstance(java.lang.Object, javax.naming.Name, javax.naming.Context, java.util.Hashtable)
     */
    public Object getObjectInstance(Object obj, Name name, Context nameCtx,
				    Hashtable<?, ?> environment)
	throws Exception
    {
        if (obj instanceof Reference) {
            Reference ref = (Reference) obj;
            if (ref.getClassName().equals(HaDataSource.class.getName())) {
                JdbcDataSource h2DataSource = new JdbcDataSource();
                
                h2DataSource.setURL((String) ref.get("url").getContent());
                h2DataSource.setUser((String) ref.get("user").getContent());
                h2DataSource.setPassword((String) ref.get("password").getContent());
                h2DataSource.setDescription((String) ref.get("description").getContent());
                String s = (String) ref.get("loginTimeout").getContent();
                h2DataSource.setLoginTimeout(Integer.parseInt(s));

                return new HaDataSource(h2DataSource);
            }
        }
        return null;
    }



    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
