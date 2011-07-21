/**
 * (c) DICOS GmbH, 2011
 *
 * $Id$
 */

package com.shesse.jdbcproxy;

import java.util.Hashtable;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import org.apache.log4j.Logger;

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
        	String url = (String) ref.get("url").getContent();
        	Properties props = new Properties();
        	for (String propName: new String[]{
        	    "user", "password", "description", "loginTimeout"})
        	{
        	    String propValue = (String)ref.get(propName).getContent();
        	    if (propValue != null) {
        		props.setProperty(propName, propValue);
        	    }
        	}
        	
               return new HaDataSource(url, props);
            }
        }
        return null;
    }



    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
