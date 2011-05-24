package com.shesse.h2ha;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sun.misc.Resource;
import sun.misc.URLClassPath;


public class BootFromJar
extends URLClassLoader
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public BootFromJar(URL[] urls, ClassLoader parent)
    {
	super(urls, parent);
	
	System.err.println("urls="+Arrays.toString(getURLs()));
	
	final URLClassPath ucp = new URLClassPath(urls);

	//final String name = "com.shesse.h2ha.H2HaServer";
	final String name = "org.apache.log4j.Logger";
	String path = name.replace('.', '/').concat(".class");
	Resource res = ucp.getResource(path, false);
	System.err.println("res="+res);
	

	AccessControlContext acc = AccessController.getContext();
	try {
	    AccessController.doPrivileged(new PrivilegedExceptionAction() {
		public Object run() throws ClassNotFoundException {
		    String path = name.replace('.', '/').concat(".class");
		    Resource res = ucp.getResource(path, false);
		    System.err.println("pres="+res);
		    return null;
		}
	    }, acc);
	} catch (PrivilegedActionException x) {
	    x.printStackTrace(System.err);
	}
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * 
     */
    public static void main(String[] args)
    throws Exception
    {
	ProtectionDomain protectionDomain = BootFromJar.class.getProtectionDomain();  
	CodeSource codeSource = protectionDomain.getCodeSource();  
	URL containerUrl = codeSource.getLocation();
	System.err.println("container="+containerUrl);
	
	ClassLoader cl = Thread.currentThread().getContextClassLoader();
	if (containerUrl.getFile().endsWith(".jar")) {
	    URL jarBaseUrl = new URL("jar", "", containerUrl + "!/");  
	    
	    List<URL> urls = new ArrayList<URL>();
	    urls.addAll(Arrays.asList(((URLClassLoader)ClassLoader.getSystemClassLoader()).getURLs()));

	    urls.add(cl.getResource("lib/log4j-1.2.9.jar"));
	    //urls.add(new URL(jarBaseUrl, "lib/h2-1.3.154.jar"));
	    //urls.add(new URL("file:/home/sth/eclipse/test/h2ha/lib/log4j-1.2.9.jar"));
	    //urls.add(new URL("file:/home/sth/prj-eclipse-3.5/h2ha/lib/log4j-1.2.9.jar"));
	    System.err.println("urls="+urls);
	    
	    cl = new BootFromJar(urls.toArray(new URL[urls.size()]), cl);
	}
	
	Class<?> cls = cl.loadClass("com.shesse.h2ha.H2HaServer");
	Thread.currentThread().setContextClassLoader(cl);
	
	cls.getMethod("main", String[].class).invoke(null, (Object)args);
    }

    /**
     * {@inheritDoc}
     *
     * @see java.lang.ClassLoader#loadClass(java.lang.String, boolean)
     */
    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve)
	throws ClassNotFoundException
    {
	// First, check if the class has already been loaded
	Class<?> c = findLoadedClass(name);
	if (c == null) {
	    try {
		c = findClass(name);
		if (!name.startsWith("java")) {
		    System.err.println("found class "+name);
		}
		
	    } catch (NoClassDefFoundError x) {
		System.err.println("did not find class "+name+": "+x);
	    } catch (ClassNotFoundException x) {
		System.err.println("did not find class cnf "+name+": "+x);
	    }
	}
	if (c == null) {
	    return super.loadClass(name, resolve);
	}

	if (resolve) {
	    resolveClass(c);
	}
	return c;
    }
    
    

    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////

}
