/**
 * (c) DICOS GmbH, 2013
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.log4j.Logger;

/**
 * This class implements (by delegation) everything of the FileChannel semantics
 * - except the map() call. It seems that H2 currently (as of 1.3.74) does not
 * use this method and we hope that if it ever starts using it, it will
 * implement a fall-back mode that works without map().
 * <p>
 * The reason to not implementing is that there is no (easy) way to wrap a
 * MappedByteBuffer so that every change is recorded and can be sent to the
 * listening slaves.
 * 
 * @author sth
 */
public class FileChannelHa
	extends FileChannel
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////
	/** */
	private static Logger log = Logger.getLogger(FileChannelHa.class);

	/** */
	private FileSystemHa fileSystem;

	/** */
	private FilePathHa filePath;

	/** */
	private FileChannel baseChannel;


	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
	 * @param accessMode
	 * @param baseChannel
	 * @param filePath
	 * @param fileSystemHa
	 */
	public FileChannelHa(FileSystemHa fileSystem, FilePathHa filePath, FileChannel baseChannel,
							String accessMode)
	{
		this.fileSystem = fileSystem;
		this.filePath = filePath;
		this.baseChannel = baseChannel;
	}

	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#read(java.nio.ByteBuffer)
	 */
	@Override
	public int read(ByteBuffer dst)
		throws IOException
	{
		if (dst.hasArray()) {
			long pos = baseChannel.position();
			int l = baseChannel.read(dst);
			if (log.isDebugEnabled()) {
				log.debug(filePath+": read from="+pos+", l="+l);
			}
			return l;
		} else {
			throw new IllegalArgumentException(
				"only ByteBuffers with hasArray() = true are supported");
		}
	}

	/**
	 * 
	 */
	public int readNoCache(ByteBuffer dst)
		throws IOException
	{
		return baseChannel.read(dst);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#read(java.nio.ByteBuffer[], int, int)
	 */
	@Override
	public long read(ByteBuffer[] dsts, int offset, int length)
		throws IOException
	{
		long total = 0;
		while (length > 0) {
			int l = read(dsts[offset]);
			if (l < 0) {
				if (total > 0) {
					return total;
				} else {
					return l;
				}
			}
			total += l;
			offset++;
			length--;
		}

		return total;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#write(java.nio.ByteBuffer)
	 */
	@Override
	public int write(ByteBuffer src)
		throws IOException
	{
		if (src.hasArray()) {
			long pos = baseChannel.position();
			int bpos = src.position() + src.arrayOffset();
			int l = baseChannel.write(src);
			if (log.isDebugEnabled()) {
				log.debug(filePath+": write from="+pos+", l="+l+", end="+(pos+l));
			}
			fileSystem.sendWrite(filePath, pos, src.array(), bpos, l);
			return l;
		} else {
			throw new IllegalArgumentException(
				"only ByteBuffers with hasArray() = true are supported");
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#write(java.nio.ByteBuffer[], int, int)
	 */
	@Override
	public long write(ByteBuffer[] srcs, int offset, int length)
		throws IOException
	{
		long l = 0;
		while (length > 0) {
			l += write(srcs[offset]);
		}
		return l;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#position()
	 */
	@Override
	public long position()
		throws IOException
	{
		return baseChannel.position();
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#position(long)
	 */
	@Override
	public FileChannel position(long newPosition)
		throws IOException
	{
		if (log.isDebugEnabled()) {
			log.debug(filePath+": position at "+newPosition);
		}
		baseChannel.position(newPosition);
		return this;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#size()
	 */
	@Override
	public long size()
		throws IOException
	{
		return baseChannel.size();
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#truncate(long)
	 */
	@Override
	public FileChannel truncate(long size)
		throws IOException
	{
		if (log.isDebugEnabled()) {
			log.debug(filePath+": truncate at "+size);
		}
		baseChannel.truncate(size);
		fileSystem.sendTruncate(filePath, size);
		return this;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#force(boolean)
	 */
	@Override
	public void force(boolean metaData)
		throws IOException
	{
		if (log.isDebugEnabled()) {
			log.debug(filePath+": force");
		}
		baseChannel.force(metaData);
		fileSystem.force();
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#transferTo(long, long,
	 *      java.nio.channels.WritableByteChannel)
	 */
	@Override
	public long transferTo(long position, long count, WritableByteChannel target)
		throws IOException
	{
		long l = baseChannel.transferTo(position, count, target);
		if (log.isDebugEnabled()) {
			log.debug(filePath+": transferTo from="+position+", l="+count+", target="+target.getClass().getName());
		}
		return l;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#transferFrom(java.nio.channels.ReadableByteChannel,
	 *      long, long)
	 */
	@Override
	public long transferFrom(ReadableByteChannel src, long position, long count)
		throws IOException
	{
		if (log.isDebugEnabled()) {
			log.debug(filePath+": transferFrom to="+position+", l="+count+", src="+src.getClass().getName());
		}
		long startPos = baseChannel.position();
		baseChannel.position(position);

		final int maxBufferSize = 8192;
		ByteBuffer buffer = null;
		long total = 0;
		while (count > 0) {
			int l = (count < maxBufferSize ? (int) count : maxBufferSize);
			if (buffer == null || l != buffer.capacity()) {
				buffer = ByteBuffer.allocate(l);
			} else {
				buffer.clear();
			}

			l = src.read(buffer);
			if (l <= 0) {
				// EOF reached or would block on non-blocking channel
				break;
			} else {
				buffer.flip();
				total += write(buffer);
			}
		}

		// position will not be changed!
		baseChannel.position(startPos);
		return total;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#read(java.nio.ByteBuffer, long)
	 */
	@Override
	public int read(ByteBuffer dst, long position)
		throws IOException
	{
		int l = baseChannel.read(dst, position);
		if (log.isDebugEnabled()) {
			log.debug(filePath+": read from="+position+", l="+l);
		}
		return l;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#write(java.nio.ByteBuffer, long)
	 */
	@Override
	public int write(ByteBuffer src, long position)
		throws IOException
	{
		int bpos = src.position();
		int l = baseChannel.write(src, position);
		if (log.isDebugEnabled()) {
			log.debug(filePath+": write from="+position+", l="+l+", end="+(position+l));
		}
		fileSystem.sendWrite(filePath, position, src.array(), bpos, l);
		return l;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#map(java.nio.channels.FileChannel.MapMode,
	 *      long, long)
	 */
	@Override
	public MappedByteBuffer map(MapMode mode, long position, long size)
		throws IOException
	{
		throw new UnsupportedOperationException(
			"map() is currently not implemented by H2HA ... and probably never will");
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#lock(long, long, boolean)
	 */
	@Override
	public FileLock lock(long position, long size, boolean shared)
		throws IOException
	{
		if (log.isDebugEnabled()) {
			log.debug(filePath+": lock at="+position+", size="+size+", shared="+shared);
		}
		return baseChannel.lock(position, size, shared);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#tryLock(long, long, boolean)
	 */
	@Override
	public FileLock tryLock(long position, long size, boolean shared)
		throws IOException
	{
		if (log.isDebugEnabled()) {
			log.debug(filePath+": tryLock at="+position+", size="+size+", shared="+shared);
		}
		return baseChannel.tryLock(position, size, shared);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.spi.AbstractInterruptibleChannel#implCloseChannel()
	 */
	@Override
	protected void implCloseChannel()
		throws IOException
	{
		if (log.isDebugEnabled()) {
			log.debug(filePath+": implCloseChannel");
		}
		fileSystem.sendClose(filePath);
	}



	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////


}
