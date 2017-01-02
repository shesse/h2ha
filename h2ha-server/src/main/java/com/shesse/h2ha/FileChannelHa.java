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

		if (log.isDebugEnabled()) {
			log.debug(DebugUtil.debugId(this)+": FileChannelHa("+filePath+
				") -> path="+filePath.getBasePath()+", base="+DebugUtil.debugId(baseChannel));
		}
	}
	
	



	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * @return the baseChannel
	 */
	public FileChannel getBaseChannel()
	{
		return baseChannel;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.nio.channels.FileChannel#read(java.nio.ByteBuffer)
	 */
	@Override
	public int read(ByteBuffer dst)
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
		return baseChannel.read(dsts, offset, length);
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
		if (position < 0) {
			throw new IOException("Negative seek offset");
		}
		
		int l = baseChannel.read(dst, position);
		if (log.isDebugEnabled()) {
			log.debug(DebugUtil.debugId(this)+": "+filePath+": read from="+position+", l="+l);
		}
		return l;
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
				log.debug(DebugUtil.debugId(this)+": "+filePath+": write from="+pos+", l="+l+", end="+(pos+l));
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
	 * @see java.nio.channels.FileChannel#write(java.nio.ByteBuffer, long)
	 */
	@Override
	public int write(ByteBuffer src, long position)
		throws IOException
	{
		int bpos = src.position();
		int l = baseChannel.write(src, position);
		if (log.isDebugEnabled()) {
			log.debug(DebugUtil.debugId(this)+". "+filePath+": write from="+position+", l="+l+", end="+(position+l));
		}
		fileSystem.sendWrite(filePath, position, src.array(), bpos, l);
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
			log.debug(DebugUtil.debugId(this)+". "+filePath+": position at "+newPosition);
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
			log.debug(DebugUtil.debugId(this)+". "+filePath+": truncate at "+size);
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
			log.debug(DebugUtil.debugId(this)+". "+filePath+": force");
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
			log.debug(DebugUtil.debugId(this)+". "+filePath+": transferTo from="+position+", l="+count+", target="+target.getClass().getName());
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
			log.debug(DebugUtil.debugId(this)+". "+filePath+": transferFrom to="+position+", l="+count+", src="+src.getClass().getName());
		}

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
				int wl = write(buffer, position);
				total += wl;
				position += wl;
				count -= wl;
			}
		}

		return total;
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
			log.debug(DebugUtil.debugId(this)+". "+filePath+": lock at="+position+", size="+size+", shared="+shared);
		}
		return new DummyLock(this, position, size, shared);
		//return baseChannel.lock(position, size, shared);
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
			log.debug(DebugUtil.debugId(this)+". "+filePath+": tryLock at="+position+", size="+size+", shared="+shared);
		}
		return new DummyLock(this, position, size, shared);
		//return baseChannel.tryLock(position, size, shared);
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
			log.debug(DebugUtil.debugId(this)+". "+filePath+": implCloseChannel");
		}
		baseChannel.close();
		fileSystem.sendClose(filePath);
	}



	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////
	/**
	 * Work around to the file locking beahvour on Windows: 
	 * WIndows seems to enforce file locking per channel, not
	 * per JVM. This would prevent our file accesses for
	 * HA synchronization.
	 * <p>
	 * As a remedy we replace the locking mechanism by 
	 * a dummy which does not actually lock. This behaviour seems
	 * acceptable, since the Java API says: "File locks are held on 
	 * behalf of the entire Java virtual machine. They are not suitable 
	 * for controlling access to a file by multiple threads within
	 * the same virtual machine". This means, that H2 file locking
	 * would only help against access by another process - and
	 * that will also be avoided by thge lock file mechanism. 
	 */
	private static class DummyLock
	extends FileLock
	{
		boolean valid = true;
		
		protected DummyLock(FileChannel channel, long position, long size, boolean shared)
		{
			super(channel, position, size, shared);
		}

		/**
		 * {@inheritDoc}
		 *
		 * @see java.nio.channels.FileLock#isValid()
		 */
		@Override
		public boolean isValid()
		{
			return valid;
		}

		/**
		 * {@inheritDoc}
		 *
		 * @see java.nio.channels.FileLock#release()
		 */
		@Override
		public void release()
			throws IOException
		{
			valid = false;
		}
		
	}

}
