/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;


import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.h2.store.fs.FileObject;


/**
 *
 * @author sth
 */
public abstract class ServerSideProtocolInstance 
extends ReplicationProtocolInstance
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(ServerSideProtocolInstance.class);

    /** */
    protected H2HaServer haServer;

    /** */
    protected FileSystemHa fileSystem;
    
    /** */
    private MessageDigest md5Digest;
    
    /** */
    private Map<String, FileObject> openFiles = new HashMap<String, FileObject>();
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public ServerSideProtocolInstance(H2HaServer haServer, FileSystemHa fileSystem)
    {
        this.haServer = haServer;
        this.fileSystem = fileSystem;
        
        try {
            md5Digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException x) {
            throw new IllegalStateException("cannot find MD5 algorithm", x);
        }
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * 
     */
    protected Set<FileInfo> discoverExistingFiles()
    {
        Set<FileInfo> existingFiles = new HashSet<FileInfo>();

        synchronized (fileSystem) {
            discoverFilesWithinDirectory(existingFiles, FileSystemHa.getRoot());
        }
        
        return existingFiles;
    }

    /**
     * 
     */
    private void discoverFilesWithinDirectory(Set<FileInfo> existingFiles, String directory)
    {
        log.debug("discovering local files within "+directory);
        for (FileInfo sub: fileSystem.listFileInfos(directory)) {
            String haName = sub.getHaName();
            if (fileSystem.isDirectory(haName)) {
                log.debug("file "+haName+" is a subdirectory");
                discoverFilesWithinDirectory(existingFiles, haName);
                
            } else if (fileSystem.exists(haName)) {
                if (sub.isDatabaseFile()) {
                    existingFiles.add(sub);
                }
            }
        }
        log.debug("end of local files discovery within "+directory);
   }

    /**
     *  Berechnet die MD5 Quersumme aus den übergebenen Bytes
     * @param in
     * @return
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     */
    protected byte[] computeMd5(byte[] in, int offset, int len)
    {
        md5Digest.reset();
        md5Digest.update(in, offset, len);
        return md5Digest.digest();
    }

    /**
     * @throws IOException 
     * 
     */
    protected FileObject getFileObject(String haName)
    throws IOException
    {
        FileObject fo = openFiles.get(haName);
        if (fo == null) {
            fo = fileSystem.openFileObject(haName, "rw");
            openFiles.put(haName, fo);
        }

        return fo;
    }

    /**
     * 
     * @param haName
     * @throws IOException
     */
    protected void closeFileObject(String haName, long lastModified)
    throws IOException
    {
        FileObject fo = openFiles.remove(haName);
        if (fo != null) {
            fo.close();
        }
        if (fileSystem.exists(haName)) {
            fileSystem.setLastModified(haName, lastModified);
        }
    }
    
    /**
     * 
     */
    protected void closeAllFileObjects()
    {
        for (FileObject fo: openFiles.values()) {
            try {
                fo.close();
            } catch (IOException x) {
                log.debug("error when trying to close a FileObject", x);
            }
        }
        
        openFiles.clear();
    }
    

}