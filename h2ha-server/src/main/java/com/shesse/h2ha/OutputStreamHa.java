/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;
import org.h2.store.fs.FileObject;

/**
 *
 * @author sth
 */
public class OutputStreamHa
    extends OutputStream
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(OutputStreamHa.class);
 
    /** */
    @SuppressWarnings("unused")
    private FileSystemHa fileSystem;

    /** */
    @SuppressWarnings("unused")
    private String haName;
    
    /** */
    private FileObject baseFileObject;
    
    /** */
    private byte[] singleByteBuffer = new byte[1];


    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * @throws IOException 
     */
    public OutputStreamHa(FileSystemHa fileSystem, String haName, FileObject baseFileObject, boolean append) throws IOException
    {
        log.debug("OutputStreamHa()");
        
        this.fileSystem = fileSystem;
        this.haName = haName;
        this.baseFileObject = baseFileObject;
        
        if (append) {
            baseFileObject.seek(baseFileObject.length());
        } else {
            baseFileObject.setFileLength(0L);
        }
    }


    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * {@inheritDoc}
     *
     * @see java.io.OutputStream#close()
     */
    @Override
    public void close()
        throws IOException
    {
        baseFileObject.close();
    }


    /**
     * {@inheritDoc}
     *
     * @see java.io.OutputStream#flush()
     */
    @Override
    public void flush()
        throws IOException
    {
    }


    /**
     * {@inheritDoc}
     *
     * @see java.io.OutputStream#write(byte[], int, int)
     */
    @Override
    public void write(byte[] buffer, int offset, int length)
        throws IOException
    {
        baseFileObject.write(buffer, offset, length);
    }


    /**
     * {@inheritDoc}
     *
     * @see java.io.OutputStream#write(byte[])
     */
    @Override
    public void write(byte[] buffer)
        throws IOException
    {
        baseFileObject.write(buffer, 0, buffer.length);
    }

    /**
     * {@inheritDoc}
     *
     * @see java.io.OutputStream#write(int)
     */
    @Override
    public void write(int i)
        throws IOException
    {
        singleByteBuffer[0] = (byte)i;
        baseFileObject.write(singleByteBuffer, 0, 1);
    }

    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////
}
