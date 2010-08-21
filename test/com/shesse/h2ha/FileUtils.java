/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 *
 * @author sth
 */
public class FileUtils
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(FileUtils.class);


    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public FileUtils()
    {
        log.debug("FileUtils()");
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * 
     */
    public static void deleteRecursive(File file)
    {
        if (file.isDirectory()) {
            for (File sub: file.listFiles()) {
                deleteRecursive(sub);
            }
        }
        file.delete();
    }

    /**
     * 
     */
    public static boolean dbDirsEqual(File dira, File dirb)
    {
        log.debug("compare "+dira+" <=> "+dirb);
        if (dira.isDirectory()) {
            if (dirb.isDirectory()) {
                Set<String> suba = listDirectory(dira);
                Set<String> subb = listDirectory(dirb);

                if (suba.equals(subb)) {
                    for (String sub: suba) {
                        if (!dbDirsEqual(new File(dira, sub), new File(dirb, sub))) {
                            return false;
                        }
                    }
                } else {
                    log.error("directories mismatch: "+dira+" <> "+dirb);
                    log.error("dira: "+suba);
                    log.error("dirb: "+subb);
                    return false;
                }
            } else {
                log.error("file mismatch: "+dira+", is a directory - "+dirb+" not");
                return false;
            }
        } else {
            if (dirb.isDirectory()) {
                log.error("file mismatch: "+dirb+", is a directory - "+dira+" not");
                return false;

            } else {
                if (!dira.canRead()) {
                    log.error("file cmp error: cannot read "+dira);
                    return false;
                }
                if (!dirb.canRead()) {
                    log.error("file cmp error: cannot read "+dirb);
                    return false;
                }

                try {
                    InputStream isa = new FileInputStream(dira);
                    InputStream isb = new FileInputStream(dirb);
                    try {

                        byte[] bufa = new byte[4096];
                        byte[] bufb = new byte[4096];

                        for (;;) {
                            int la = isa.read(bufa);
                            int lb = isb.read(bufb);

                            if (la < 0 && lb < 0) break;

                            if (la != lb) {
                                log.error("file cmp error: length mismatch between "+dira+" and "+dirb);
                                return false;
                            }

                            if (!Arrays.equals(bufa, bufb))  {
                                log.error("file cmp error: content differs between "+dira+" and "+dirb);
                                return false;
                            }
                        }
                    } finally {
                        isa.close();
                        isb.close();
                    }
                } catch (IOException x) {
                    log.error("file cmp error: IO exception ", x);
                    return false;
                }
            }
        }
        return true;
    }
    
    private static Set<String> listDirectory(File dir)
    {
        Set<String> entries = new HashSet<String>();
        
        for (String e: dir.list()) {
            File fe = new File(dir, e);
            FileInfo fi = new FileInfo("ha://"+e, fe.getPath(), true);
            if (fe.isDirectory() || fi.mustReplicate()) entries.add(e);
        }
        
        return entries;
    }
    
    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
