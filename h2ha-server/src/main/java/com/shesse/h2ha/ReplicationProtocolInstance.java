/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;


import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.net.SocketFactory;

import org.apache.log4j.Logger;

import com.shesse.h2ha.H2HaServer.FailoverState;


/**
 * 
 * @author sth
 */
public abstract class ReplicationProtocolInstance
	implements Runnable
{
	// /////////////////////////////////////////////////////////
	// Class Members
	// /////////////////////////////////////////////////////////

	/** */
	private static Logger log = Logger.getLogger(ReplicationProtocolInstance.class);

	/** */
	protected String instanceName;

	/** */
	private long maxEnqueueWait;

	/** */
	private int maxWaitingMessages;

	/** */
	protected Socket socket = null;

	/** */
	protected ObjectOutputStream oos = null;

	/** */
	private ReplicationProtocolReceiver receiver = null;

	/** */
	protected Thread instanceThread = null;

	/** */
	protected BlockingQueue<ReplicationMessage> messageQueue;

	/** */
	protected ReplicationMessage terminateMessage = new ReplicationMessage() {
		private static final long serialVersionUID = 1L;

		@Override
		protected void process(ReplicationProtocolInstance instance)
			throws Exception
		{
		}

		@Override
		public int getSizeEstimate()
		{
			return 4;
		}

		@Override
		public String toString()
		{
			return "terminate";
		}
	};

	/** */
	protected Map<Integer, WaitingOperation<?>> waitingOperations =
		new HashMap<Integer, WaitingOperation<?>>();

	/** */
	protected int nextWaitingOperationId = 0;

	/** */
	protected Timer timer = new Timer();
	
	/** */
	private long idleTimeout = 20000;
	
	/** */
	private boolean connectionCanceled = false;

	/** */
	private long totalMessagesEnqueued = 0;

	/** */
	private long totalMessagesDequeued = 0;

	/** */
	private long totalBytesTransmitted = 0;

	/** */
	private long totalBytesReceived = 0;

	/** */
	private long lastStatisticsTimestamp = 0L;

	/** */
	private long lastStatisticsMessagesEnqueued = 0L;

	/** */
	private long lastStatisticsMessagesDequeued = 0L;

	/** */
	private long lastStatisticsBytesTransmitted = 0L;

	/** */
	private long lastStatisticsBytesReceived = 0L;

	/** */
	private long nextHeartbeatToSend = 0L;

	/** */
	private TimerTask idleTimer = null;

	/** */
	private long lastSendDelay = 0L;

	/** */
	private int objectsSentWithoutReset = 0;


	// /////////////////////////////////////////////////////////
	// Constructors
	// /////////////////////////////////////////////////////////
	/**
     * 
     */
	public ReplicationProtocolInstance(String instanceName, int maxQueueSize)
	{
		this.instanceName = instanceName;

		if (maxQueueSize > 0) {
			messageQueue = new LinkedBlockingQueue<ReplicationMessage>(maxQueueSize);
		} else {
			messageQueue = new LinkedBlockingQueue<ReplicationMessage>();
		}
	}


	// /////////////////////////////////////////////////////////
	// Methods
	// /////////////////////////////////////////////////////////
	/**
	 * @throws IOException
	 * 
	 */
	protected void setSocket(Socket socket)
		throws IOException
	{
		this.socket = socket;
		this.oos = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
		nextWaitingOperationId = 0;
		connectionCanceled = false;
		nextHeartbeatToSend = 0L;
		lastSendDelay = 0L;

		restartIdleTimer();
		
		receiver = new ReplicationProtocolReceiver(this, socket);
		receiver.start();

	}

	/**
     * 
     */
	protected void setInstanceName(String instanceName)
	{
		this.instanceName = instanceName;
	}

	/**
	 * @param idleTimeout2 
     * 
     */
	public void setParameters(long maxEnqueueWait, int maxWaitingMessages, long idleTimeout)
	{
		this.maxEnqueueWait = maxEnqueueWait;
		this.maxWaitingMessages = maxWaitingMessages;
		this.idleTimeout = idleTimeout;
	}

	/**
	 * Tries to initiate a connection to the peer.
	 * 
	 * @return true if connection was successful.
	 */
	public boolean tryToConnect(String peerHost, int peerPort, int connectTimeout)
	{
		try {
			log.debug(instanceName + ": try to connect to " + peerHost + ":" + peerPort);
			Socket socket = SocketFactory.getDefault().createSocket();
			try {
				SocketAddress addr = new InetSocketAddress(peerHost, peerPort);
				if (connectTimeout > 0) {
					socket.connect(addr, connectTimeout);
				} else {
					socket.connect(addr);
				}
				log.debug(instanceName + ": successfully connected to peer " + peerHost + ":" +
					peerPort);

				setSocket(socket);

				return true;

			} catch (IOException x) {
				socket.close();
				socket = null;
				throw x;
			}

		} catch (IOException x) {
			log.info(instanceName + ": Cannot connect to peer " + peerHost + ":" + peerPort);
			return false;
		}
	}

	/**
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 */
	protected void closeSocket()
	{
		if (oos != null) {
			try {
				oos.flush();
			} catch (IOException x) {
			}
		}

		if (socket != null) {
			try {
				socket.shutdownOutput();
			} catch (IOException x) {
			}
			socket = null;
		}
		if (receiver != null) {
			try {
				receiver.join(10000);
			} catch (InterruptedException x) {
			}
			receiver.terminate();
			receiver = null;
		}
		if (oos != null) {
			try {
				oos.close();
			} catch (IOException x) {
			}
			oos = null;
		}
	}

	/**
     * 
     */
	public boolean isConnected()
	{
		return socket != null;
	}

	/**
     * 
     */
	public void run()
	{
		instanceThread = Thread.currentThread();

		try {
			body();

		} catch (Throwable x) {
			log.error(instanceName + ": caught unexpected exception within ReplicationInstance", x);

		} finally {
			closeSocket();
			timer.cancel();
		}
	}

	/**
	 * @throws SQLException
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 */
	protected void body()
		throws IOException, InterruptedException
	{
		log.debug(instanceName + ": replication instance has started");

		processMessagesForSingleConnection();
	}
	
	/**
	 * @throws IOException 
	 * @throws InterruptedException 
	 * 
	 */
	protected void processMessagesForSingleConnection()
		throws InterruptedException, IOException
	{
		try {
			processProtocolMessages();

		} finally {
			// call process on any remaining elements in the queue -
			// if they require this.
			ReplicationMessage message;
			while ((message = messageQueue.poll()) != null) {
				totalMessagesDequeued++;
				if (!message.callOnlyIfConnected()) {
					try {
						message.process(this);
					} catch (Exception x) {
						log.error(instanceName +
							": unexpected exception when processing message in replication queue",
							x);
					}
				}
			}

			// signaling all still waiting operations that the connection has
			// failed
			synchronized (waitingOperations) {
				for (WaitingOperation<?> wo : waitingOperations.values()) {
					wo.exception =
						new IOException("connection terminated when waiting for confirm");
					wo.waitGate.release();
					wo.watcher.cancel();
				}
				waitingOperations.clear();
			}

			log.debug(instanceName + ": replication instance has ended");
		}

	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */
	private void processProtocolMessages()
		throws InterruptedException, IOException
	{
		log.debug(instanceName + ": processProtocolMessages");
		try {
			while (oos != null) {
				long now = System.currentTimeMillis();
				if (now >= nextHeartbeatToSend) {
					nextHeartbeatToSend = now + idleTimeout/2;
					sendHeartbeat();
				}

				long nextActivity = nextHeartbeatToSend;
				
				long delta = nextActivity - now;
				ReplicationMessage message = messageQueue.poll(delta, TimeUnit.MILLISECONDS);

				if (message != null) {
					totalMessagesDequeued++;

					if (message == terminateMessage) {
						log.info(instanceName + ": message processor is terminating");
						break;
					}
					
					log.debug("process message: " + message);
					message.process(this);
				}
			}

		} catch (TerminateThread x) {
			x.logError(log, instanceName);
			log.error(instanceName + ": terminating connection!");

		} catch (SocketException x) {
			if (x.getMessage().contains("closed")) {
				log.info(instanceName + ": connection has been closed");
				
			} else {
				log.error(instanceName + ": error on socket to peer: "+x.getMessage());
				log.error(instanceName + ": terminating connection!");
			}
			
		} catch (Exception x) {
			log.error(instanceName + ": unexpected exception when processing message from peer", x);
			log.error(instanceName + ": terminating connection!");

		} finally {
			closeSocket();
			log.debug(instanceName + ": leaving processProtocolMessages");
		}
	}
	
	/**
	 * 
	 */
	protected void logStatistics()
	{
		long now = System.currentTimeMillis();
		if (lastStatisticsTimestamp != 0) {
			double enqueuedMessagesPerSecond =
				(totalMessagesEnqueued - lastStatisticsMessagesEnqueued) /
				((now - lastStatisticsTimestamp) / 1000.);

			double dequeuedMessagesPerSecond =
				(totalMessagesDequeued - lastStatisticsMessagesDequeued) /
				((now - lastStatisticsTimestamp) / 1000.);

			double transmittedBytesPerSecond =
				(totalBytesTransmitted - lastStatisticsBytesTransmitted) /
				((now - lastStatisticsTimestamp) / 1000.);

			double receivedBytesPerSecond =
				(totalBytesReceived - lastStatisticsBytesReceived) /
				((now - lastStatisticsTimestamp) / 1000.);

			log.info(instanceName +
				String.format(": transmit/receive rate = %7.1f/%7.1f KB/sec",
					transmittedBytesPerSecond / 1000, receivedBytesPerSecond / 1000));

			log.info(instanceName +
				String.format(": enqueue/dequeue rate  = %7.1f/%7.1f Msg/sec",
					enqueuedMessagesPerSecond, dequeuedMessagesPerSecond));

			log.info(instanceName +
				String.format(": queue size    = %5d", messageQueue.size()));
		}
		
		lastStatisticsMessagesEnqueued = totalMessagesEnqueued;
		lastStatisticsMessagesDequeued = totalMessagesDequeued;
		lastStatisticsBytesTransmitted = totalBytesTransmitted;
		lastStatisticsBytesReceived = totalBytesReceived;
		lastStatisticsTimestamp = now;
	}


	/**
	 * @throws IOException
	 * 
	 */
	protected void sendHeartbeat()
		throws IOException
	{
		sendToPeer(new HeartbeatMessage(idleTimeout));
	}


	/**
     * 
     */
	protected FailoverState getCurrentFailoverState()
	{
		return FailoverState.INITIAL;
	}

	/**
	 * @param senderIdleTimeout 
	 * @param uuid
	 * @param masterPriority
	 * 
	 */
	protected void activityReceived(long senderIdleTimeout)
	{
		log.debug("activityReceived");
		if (senderIdleTimeout > 0) {
			idleTimeout = senderIdleTimeout;
		}
		
		restartIdleTimer();
	}
	
	/**
	 * 
	 */
	private synchronized void restartIdleTimer()
	{
		stopIdleTimer();
		
		if (oos == null) {
			return;
		}
		
		idleTimer = new TimerTask() {
			@Override
			public void run()
			{
				log.error(instanceName +
					": inactivity timeout - terminating connection");
				try {
					socket.close();
				} catch (IOException x) {
					log.error("cannot close socket: "+x.getMessage());
				}
			}
		};
		timer.schedule(idleTimer, idleTimeout);
	}
	
	/**
	 * 
	 */
	private synchronized void stopIdleTimer()
	{
		if (idleTimer != null) {
			idleTimer.cancel();
			idleTimer = null;
		}
	}

	/**
	 * @param peerState
	 * @param peerMasterPriority
	 * @param peerUuid
	 */
	protected void peerStatusReceived(FailoverState peerState, int peerMasterPriority,
		String peerUuid)
	{
		log.debug("peerStatusReceived peerState=" + peerState);
	}


	/**
	 * Enqueues a message for sending to the peer. This method may be called
	 * from any thread. The actual sending of messages will be carried out in a
	 * sequence conserving manner
	 */
	public void send(final ReplicationMessage message)
	{
		final long enqueueTimestamp = System.currentTimeMillis();
		enqueue(new ReplicationMessage() {
			private static final long serialVersionUID = 1L;

			@Override
			protected void process(ReplicationProtocolInstance instance)
				throws Exception
			{
				long now = System.currentTimeMillis();
				lastSendDelay = now - enqueueTimestamp;

				try {
					if (message.needToSend(instance)) {
						sendToPeer(message);
					}

				} catch (SocketException x) {
					if (x.getMessage().contains("closed")) {
						log.info(instanceName + ": socket has been closed");
					} else {
						log.error(instanceName +
							": unexpected exception when sending message to peer", x);
						log.error(instanceName + ": terminating connection!");
					}
					closeSocket();
					
				} catch (IOException x) {
					log.error(instanceName + ": unexpected exception when sending message to peer",
						x);
					log.error(instanceName + ": terminating connection!");

					closeSocket();
				}
			}

			@Override
			public int getSizeEstimate()
			{
				return 4;
			}

			@Override
			public String toString()
			{
				return "send to peer: " + message;
			}
		});
	}

	/**
     * 
     */
	public void enqueue(ReplicationMessage message)
	{
		if (connectionCanceled)
			return;

		if (maxWaitingMessages > 0 && messageQueue.size() > maxWaitingMessages) {
			log.error(instanceName +
				": too many waiting messages - replicator connection will be canceled");
			cancelConnection();
			return;
		}

		try {
			if (maxEnqueueWait > 0) {
				if (messageQueue.offer(message, maxEnqueueWait, TimeUnit.MILLISECONDS)) {
					totalMessagesEnqueued++;

				} else {
					log.error(instanceName +
						": replication connection is too slow - it will be terminated.");
					log.error(instanceName + ": we could not enqueue within " + maxEnqueueWait +
						" ms when at a queue size of " + messageQueue.size());
					// logStacksOfAllThreads(log);
					cancelConnection();
				}

			} else {
				messageQueue.put(message);
				totalMessagesEnqueued++;
			}

		} catch (InterruptedException e) {
			log.error(instanceName +
				": enqueue to replication connection has been interrupted - terminating it");
			cancelConnection();
		}

	}

	/**
     */
	public static void logStacksOfAllThreads(Logger slog)
	{
		slog.info("begin stack trace of all threads");
		for (Map.Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet()) {
			Thread t = e.getKey();
			slog.info("Thread " + t.getName() + " (" + t.getState() + ")");
			for (StackTraceElement se : e.getValue()) {
				slog.info("  at " + se.getClassName() + "." + se.getMethodName() + "(" +
					se.getFileName() + ":" + se.getLineNumber() + ")");
			}
			slog.info("");
		}
		slog.info("end stack trace of all threads");
	}

	/**
     * 
     */
	private void cancelConnection()
	{
		connectionCanceled = true;

		messageQueue.clear();

		enqueue(new ReplicationMessage() {
			private static final long serialVersionUID = 1L;

			@Override
			protected void process(ReplicationProtocolInstance instance)
				throws Exception
			{
				throw new TerminateThread("connection has been canceled!");
			}

			@Override
			public int getSizeEstimate()
			{
				return 4;
			}

			@Override
			public String toString()
			{
				return "cancel connection";
			}
		});

		if (instanceThread != null) {
			instanceThread.interrupt();
		}
	}

	/**
     * 
     */
	protected void processReceivedMessage(Object message)
	{
		if (message instanceof ReplicationMessage) {
			if (log.isDebugEnabled()) {
				log.debug(instanceName + ": got from protocol connection: " + message);
			}

			// treat every message as a life sign of the peer
			activityReceived(-1);

			ReplicationMessage repmsg = (ReplicationMessage) message;
			int estimatedSize = repmsg.getSizeEstimate();
			totalBytesReceived += estimatedSize;
			enqueue(repmsg);

		} else {
			log.debug(instanceName + ": got unexpected object from protocol connection: " +
				(message == null ? "null" : message.getClass().getName()));
		}

	}

	/**
	 * Will cause all outstanding messages to be sent. This method will wait
	 * until the sending has been completed. Not that this does not necessarily
	 * mean that the peer has processed all messages. They even may still get
	 * lost on network connection or peer failure.
	 * 
	 * @throws InterruptedException
	 */
	public void flush()
		throws InterruptedException
	{
		final Semaphore sema = new Semaphore(0);

		enqueue(new ReplicationMessage() {
			private static final long serialVersionUID = 1L;

			@Override
			protected void process(ReplicationProtocolInstance instance)
				throws Exception
			{
				sema.release();
			}

			@Override
			protected boolean callOnlyIfConnected()
			{
				return false;
			}

			@Override
			public int getSizeEstimate()
			{
				return 4;
			}

			@Override
			public String toString()
			{
				return "flush";
			}
		});

		sema.acquire();
	}

	/**
	 * This method behaves like flush() with additionally making sure that the
	 * outstanding messages have been processed by the peer.
	 * <p>
	 * If no confirmation can be received from the peer, an IOException will be
	 * raised.
	 * 
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void syncConnection()
		throws InterruptedException, IOException
	{
		WaitingOperation<Void> wo = new WaitingOperation<Void>(new SyncRequestMessage());

		log.debug(instanceName + ": sending sync request to peer");
		wo.sendAndGetResult();
		log.debug(instanceName + ": sync has been confirmed");
	}

	/**
     * 
     */
	public void terminate()
	{
		enqueue(terminateMessage);
	}

	/**
	 * @return the instanceName
	 */
	public String getInstanceName()
	{
		return instanceName;
	}


	/**
	 * @return the totalBytesTransmitted
	 */
	public long getTotalBytesTransmitted()
	{
		return totalBytesTransmitted;
	}

	/**
     * 
     */
	public int getQueueSize()
	{
		return messageQueue.size();
	}

	/**
	 * @return the lastSendDelay
	 */
	public long getLastSendDelay()
	{
		return lastSendDelay;
	}


	/**
	 * @throws IOException
	 * 
	 */
	protected void sendToPeer(ReplicationMessage message)
		throws IOException
	{
		sendToPeer(message, message.getSizeEstimate());
	}

	/**
	 * @throws IOException
	 * 
	 */
	protected void sendToPeer(Serializable message, int sizeEstimate)
		throws IOException
	{
		log.debug(instanceName + ": sending message to peer: " + message + ", size=" + sizeEstimate);
		if (instanceThread == null) {
			instanceThread = Thread.currentThread();
		} else if (Thread.currentThread() != instanceThread) {
			throw new IllegalStateException("sendToPeer used by non-owener thread");
		}

		oos.writeObject(message);

		// we need to reset() the oos to force the oos to release all references
		// to already written objects. However, we try to allow the oos to
		// optimize
		// by calling reset not on every write. The factor is arbitray and may
		// be changed
		// to enhance optimization
		if (++objectsSentWithoutReset > 20) {
			oos.reset();
			objectsSentWithoutReset = 0;
		}

		oos.flush();

		totalBytesTransmitted += sizeEstimate;
	}


	// /////////////////////////////////////////////////////////
	// Inner Classes
	// /////////////////////////////////////////////////////////
	/**
     * 
     */
	protected static abstract class OperationRequestMessage<CnfType>
		extends ReplicationMessage
	{
		private static final long serialVersionUID = 1L;
		private int operationId = -1;
		private Class<CnfType> cls;

		OperationRequestMessage(Class<CnfType> cls)
		{
			this.cls = cls;
		}

		@Override
		protected void process(ReplicationProtocolInstance instance)
			throws Exception
		{
			log.debug("processing operation " + operationId);
			CnfType cnf = performOperation(instance);

			log.debug("sending result " + operationId);
			instance.sendToPeer(new OperationConfirmMessage<CnfType>(cls, operationId, cnf));
		}

		protected abstract CnfType performOperation(ReplicationProtocolInstance instance);

		protected CnfType castConfirm(Object cnf)
		{
			return cls.cast(cnf);
		}

		@Override
		public String toString()
		{
			return "op req id=" + operationId;
		}
	}

	/**
     * 
     */
	protected static class OperationConfirmMessage<CnfType>
		extends ReplicationMessage
	{
		private static final long serialVersionUID = 1L;
		private int operationId;
		private CnfType cnf;

		OperationConfirmMessage(Class<CnfType> cls, int operationId, CnfType cnf)
		{
			this.operationId = operationId;
			this.cnf = cnf;
		}

		@Override
		protected void process(ReplicationProtocolInstance instance)
			throws Exception
		{
			log.debug("processing operation confirm " + operationId);
			synchronized (instance.waitingOperations) {
				WaitingOperation<?> wo = instance.waitingOperations.remove(operationId);
				if (wo != null) {
					wo.cnf = cnf;
					wo.waitGate.release();
					wo.watcher.cancel();
				} else {
					log.debug("could not find waiting instance for confirm");
				}
			}
		}

		@Override
		public int getSizeEstimate()
		{
			return 12;
		}

		@Override
		public String toString()
		{
			return "op cnf op=" + operationId;
		}
	}

	/**
     * 
     */
	protected class WaitingOperation<CnfType>
	{
		private int operationId;
		private Semaphore waitGate;
		private TimerTask watcher;
		private IOException exception = null;
		private Object cnf = null;
		private OperationRequestMessage<CnfType> request;

		WaitingOperation(OperationRequestMessage<CnfType> request)
		{
			this.request = request;
			operationId = nextWaitingOperationId++;

			log.debug(instanceName + ": waitingOperation " + operationId + " - reqClass=" +
				request.getClass().getName());

			request.operationId = operationId;
			waitGate = new Semaphore(0);
			watcher = new TimerTask() {
				@Override
				public void run()
				{
					exception = new IOException("timeout when waiting for confirm");
					waitGate.release();

					synchronized (waitingOperations) {
						waitingOperations.remove(operationId);
					}
				}

			};
		}

		public CnfType sendAndGetResult()
			throws InterruptedException, IOException
		{
			synchronized (waitingOperations) {
				waitingOperations.put(operationId, this);
				timer.schedule(watcher, 20000L);
			}

			try {
				log.debug(instanceName + ": send WaitingOperation " + operationId);
				send(request);
				log.debug(instanceName + ": wait for result " + operationId);
				waitGate.acquire();
				log.debug(instanceName + ": got result " + operationId);

			} finally {
				synchronized (waitingOperations) {
					waitingOperations.remove(operationId);
				}
			}

			if (exception != null) {
				IOException x = new IOException(exception.getMessage());
				x.initCause(exception);
				throw x;
			}
			return request.castConfirm(cnf);
		}
	}


	/**
     * 
     */
	protected static class SyncRequestMessage
		extends OperationRequestMessage<Void>
	{
		private static final long serialVersionUID = 1L;

		SyncRequestMessage()
		{
			super(Void.class);
		}

		@Override
		protected Void performOperation(ReplicationProtocolInstance instance)
		{
			return null;
		}

		@Override
		public int getSizeEstimate()
		{
			return 4;
		}

		@Override
		public String toString()
		{
			return "sync req";
		}
	}

	/**
     * 
     */
	protected static class HeartbeatMessage
		extends ReplicationMessage
	{
		private static final long serialVersionUID = 1L;
		
		private long senderIdleTimeout;

		public HeartbeatMessage(long senderIdleTimeout)
		{
			this.senderIdleTimeout = senderIdleTimeout;
		}

		@Override
		protected void process(ReplicationProtocolInstance instance)
			throws Exception
		{
			instance.activityReceived(senderIdleTimeout);
		}

		@Override
		public int getSizeEstimate()
		{
			return 12;
		}

		@Override
		public String toString()
		{
			return "heartbeat";
		}
	}
}