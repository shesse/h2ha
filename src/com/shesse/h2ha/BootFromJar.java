package com.shesse.h2ha;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class BootFromJar
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////


    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public BootFromJar()
    {
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

	    //urls.add(cl.getResource("lib/log4j-1.2.9.jar"));
	    //urls.add(new URL(jarBaseUrl, "lib/h2-1.3.154.jar"));
	    urls.add(new URL("file:/home/sth/eclipse/test/h2ha/lib/log4j-1.2.9.jar"));
	    System.err.println("urls="+urls);
	    
	    cl = URLClassLoader.newInstance(urls.toArray(new URL[urls.size()]), cl);
	}
	
	Class<?> cls = cl.loadClass("com.shesse.h2ha.H2HaServer");
	Thread.currentThread().setContextClassLoader(cl);
	
	cls.getMethod("main", String[].class).invoke(null, (Object)args);
    }

    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////

}
