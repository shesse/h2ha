/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.h2.store.fs.FileObject;

/**
 *
 * @author sth
 */
public class FileObjectHa
    implements FileObject
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(FileObjectHa.class);

    /** */
    private FileSystemHa fileSystem;

    /** */
    private String haName;
    
    /** */
    private FileObject baseFileObject;
    
    
    
    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public FileObjectHa(FileSystemHa fileSystem, String haName, FileObject baseFileObject)
    {
        log.debug("FileObjectHa()");
        
        this.fileSystem = fileSystem;
        this.haName = haName;
        this.baseFileObject = baseFileObject;
    }



    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * {@inheritDoc}
     *
     * @see org.h2.store.fs.FileObject#close()
     */
    public void close()
        throws IOException
    {
        baseFileObject.close();
        fileSystem.sendToReplicators(new CloseMessage(haName, fileSystem.getLastModified(haName)));
    }


    /**
     * {@inheritDoc}
     *
     * @see org.h2.store.fs.FileObject#getFilePointer()
     */
    public long getFilePointer()
        throws IOException
    {
        return baseFileObject.getFilePointer();
    }


    /**
     * {@inheritDoc}
     *
     * @see org.h2.store.fs.FileObject#getName()
     */
    public String getName()
    {
        return haName;
    }


    /**
     * {@inheritDoc}
     *
     * @see org.h2.store.fs.FileObject#length()
     */
    public long length()
        throws IOException
    {
        return baseFileObject.length();
    }


    /**
     * {@inheritDoc}
     *
     * @see org.h2.store.fs.FileObject#readFully(byte[], int, int)
     */
    public void readFully(byte[] b, int off, int len)
        throws IOException
    {
        baseFileObject.readFully(b, off, len);
    }


    /**
     * {@inheritDoc}
     *
     * @see org.h2.store.fs.FileObject#seek(long)
     */
    public void seek(long pos)
        throws IOException
    {
        baseFileObject.seek(pos);
    }


    /**
     * {@inheritDoc}
     *
     * @see org.h2.store.fs.FileObject#setFileLength(long)
     */
    public void setFileLength(long newLength)
        throws IOException
    {
        baseFileObject.setFileLength(newLength);
        fileSystem.sendToReplicators(new SetFileLengthMessage(haName, newLength));
    }


    /**
     * {@inheritDoc}
     *
     * @see org.h2.store.fs.FileObject#sync()
     */
    public void sync()
        throws IOException
    {
        baseFileObject.sync();
        // it would make sense to wait here until all queued 
        // data has been sent to all replicators. However, as
        // this may considerably degrade performance of the 
        // master DB, we will refrain from doing so and accept
        // some time shift between master and slave
    }


    /**
     * {@inheritDoc}
     *
     * @see org.h2.store.fs.FileObject#write(byte[], int, int)
     */
    public void write(byte[] b, int off, int len)
        throws IOException
    {
        long filePointer = baseFileObject.getFilePointer();
        baseFileObject.write(b, off, len);
        
        // b may be changed by the caller upon return, so 
        // we need to copy the data before placing it into the queue
        byte[] dupData = new byte[len];
        System.arraycopy(b, off, dupData, 0, len);
        fileSystem.sendToReplicators(new WriteMessage(haName, filePointer, dupData));
    }


    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////
    /** */
    private static class CloseMessage
    extends MessageToClient
    {
        private static final long serialVersionUID = 1L;
        String haName;
        long lastModified;
        CloseMessage(String haName, long lastModified)
        {
            this.haName = haName;
            this.lastModified = lastModified;
        }
        
        @Override
        protected void processMessageToClient(ReplicationClientInstance instance)
            throws Exception
        {
            instance.processFoCloseMessage(haName, lastModified);
        }

	@Override
	public int getSizeEstimate()
	{
	    return 30;
	}
    }

    /** */
    private static class SetFileLengthMessage
    extends MessageToClient
    {
        private static final long serialVersionUID = 1L;
        String haName;
        long newLength;
        SetFileLengthMessage(String haName, long newLength)
        {
            this.haName = haName;
            this.newLength = newLength;
        }
        
        @Override
        protected void processMessageToClient(ReplicationClientInstance instance)
            throws Exception
        {
            instance.processFoSetFileLengthMessage(haName, newLength);
        }

	@Override
	public int getSizeEstimate()
	{
	    return 30;
	}
    }

    /** */
    private static class WriteMessage
    extends MessageToClient
    {
        private static final long serialVersionUID = 1L;
        String haName;
        long filePointer;
        byte[] data;
        WriteMessage(String haName, long filePointer, byte[] data)
        {
            this.haName = haName;
            this.filePointer = filePointer;
            this.data = data;
        }
        
        @Override
        protected void processMessageToClient(ReplicationClientInstance instance)
            throws Exception
        {
            instance.processFoWriteMessage(haName, filePointer, data);
        }

	@Override
	public int getSizeEstimate()
	{
	    return 30 + data.length;
	}
   }


}
