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
import java.util.Date;
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
        log.info("compare "+dira+" <=> "+dirb);
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

                        byte[] bufa = new byte[4096000];
                        byte[] bufb = new byte[4096000];
                        long pos = 0;
                        for (;;) {
                            int la = isa.read(bufa);
                            int lb = isb.read(bufb);

                            if (la < 0 && lb < 0) break;

                            if (la != lb) {
                                log.error("file cmp error: length mismatch between "+dira+" and "+dirb);
                                log.error(lsl(dira));
                                log.error(lsl(dirb));
                                 return false;
                            }

                            if (!Arrays.equals(bufa, bufb))  {
                                log.error("file cmp error: content differs between "+dira+" and "+dirb);
                                int ndiff = 0;
                                for (int i = 0; i < la; i++) {
                                	if (bufa[i] != bufb[i]) {
                                		if (++ndiff > 10) {
                                			log.error("  ...");
                                			break;
                                		}
                                		log.error(String.format("  %06x: %02x -> %02x", pos+i, 
                                			bufa[i] & 0xff, bufb[i] & 0xff));
                                	}
                                }
                                log.error(lsl(dira));
                                log.error(lsl(dirb));
                                return false;
                            }
                            
                            pos += la;
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
    
    /**
	 * @param dira
	 * @return
	 */
	private static String lsl(File file)
	{
		StringBuilder sb = new StringBuilder();
		sb.append(file);
		sb.append(": ").append(file.length());
		sb.append(" - ").append(file.lastModified());
		sb.append(" = ").append(new Date(file.lastModified()));
		return sb.toString();
	}

	private static Set<String> listDirectory(File dir)
    {
        Set<String> entries = new HashSet<String>();
        
        FileSystemHa dummyFileSystem;
		try {
			dummyFileSystem = new FileSystemHa(null, Arrays.asList("-haBaseDir", dir.getPath() ));
		} catch (TerminateThread x) {
			throw new IllegalStateException(x.getMessage());
		}

        
        for (String e: dir.list()) {
            File fe = new File(dir, e);
            FilePathHa fp = new FilePathHa(dummyFileSystem, "ha:///"+e, false);
            if (fe.isDirectory() || fp.mustReplicate()) entries.add(e);
        }
        
        return entries;
    }
    
    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////


}
