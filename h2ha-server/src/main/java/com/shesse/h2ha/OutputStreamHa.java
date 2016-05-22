/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;
import org.h2.store.fs.FilePath;

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
	private FileSystemHa fileSystem;

	/** */
	private FilePathHa filePath;

	/** */
	private OutputStream baseOutputStream;

	/** */
	private long filePtr = 0;

	/** */
	private byte[] singleByteBuffer = new byte[1];


	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @throws IOException
	 */
	public OutputStreamHa(FileSystemHa fileSystem, FilePathHa filePath,
							OutputStream baseOutputStream, boolean append)
		throws IOException
	{
		log.debug("OutputStreamHa()");

		this.fileSystem = fileSystem;
		this.filePath = filePath;
		this.baseOutputStream = baseOutputStream;

		if (append) {
			FilePath basePath = filePath.getBasePath();
			if (basePath.exists()) {
				filePtr = basePath.size();
			}
		}

		fileSystem.sendTruncate(filePath, filePtr);
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
		baseOutputStream.close();
		fileSystem.sendClose(filePath);
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
		baseOutputStream.flush();
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
		if (length == 0) {
			return;
		}

		baseOutputStream.write(buffer, offset, length);

		if (log.isDebugEnabled()) {
			log.debug(filePath+": write from="+filePtr+", l="+length+", end="+(filePtr+length));
		}
		fileSystem.sendWrite(filePath, filePtr, buffer, offset, length);
		filePtr += length;
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
		if (buffer.length == 0) {
			return;
		}

		baseOutputStream.write(buffer);

		if (log.isDebugEnabled()) {
			log.debug(filePath+": write from="+filePtr+", l="+buffer.length+", end="+(filePtr+buffer.length));
		}
		fileSystem.sendWrite(filePath, filePtr, buffer, 0, buffer.length);
		filePtr += buffer.length;
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
		singleByteBuffer[0] = (byte) i;
		baseOutputStream.write(i);

		if (log.isDebugEnabled()) {
			log.debug(filePath+": write from="+filePtr+", l=1, end="+(filePtr+1));
		}
		fileSystem.sendWrite(filePath, filePtr, singleByteBuffer, 0, 1);
		filePtr += 1;
	}

	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////
}
