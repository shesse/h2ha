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


/**
 *
 * @author sth
 */
public class ReplicationProtocolInstance
implements Runnable
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////

    /** */
    private static Logger log = Logger.getLogger(ReplicationProtocolInstance.class);

    /** */
    private String instanceName;
    
    /** */
    private int maxWaitingMessages;

    /** */
    protected Socket socket = null;
    
    /** */
    protected ObjectOutputStream oos = null;
    
    /** */
    private ReplicationProtocolReceiver receiver = null;
    
    /** */
    private Thread instanceThread = null;
    
    /** */
    protected BlockingQueue<ReplicationMessage> messageQueue = new LinkedBlockingQueue<ReplicationMessage>();
    
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
   };
    
    /** */
    protected Map<Integer, WaitingOperation<?>> waitingOperations = new HashMap<Integer, WaitingOperation<?>>();
    
    /** */
    protected int nextWaitingOperationId = 0;
    
    /** */
    protected Timer timer = new Timer();

    /** */
    private boolean connectionCanceled = false;

    /** */
    private long totalBytesTransmitted = 0;
    
    /** */
    private long lastStatisticsTimestamp = 0L;
    
    /** */
    private long lastStatisticsBytesTransmitted = 0L;
    
    /** */
    private long nextStatisticsTimestamp = 0L;
    

    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     * 
     */
    public ReplicationProtocolInstance(String instanceName, int maxWaitingMessages)
    {
	this.instanceName = instanceName;
	this.maxWaitingMessages = maxWaitingMessages;
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
    
        receiver = new ReplicationProtocolReceiver(this, socket);
        receiver.start();
        
    }

    /**
     * Tries to initiate a connection to the peer.
     * @return true if connection was successful.
     */
    public boolean tryToConnect(String peerHost, int peerPort, int connectTimeout)
    {
        try {
            log.debug(instanceName+": try to connect to "+peerHost+":"+peerPort);
            Socket socket = SocketFactory.getDefault().createSocket();
            try {
                SocketAddress addr = new InetSocketAddress(peerHost, peerPort);
                if (connectTimeout > 0) {
                    socket.connect(addr, connectTimeout);
                } else {
                    socket.connect(addr);
                }
                log.debug(instanceName+": successfully connected to peer "+peerHost+":"+peerPort);
                
                setSocket(socket);
                
                return true;

            } catch (IOException x) {
                socket.close();
                socket = null;
                throw x;
            }
            
        } catch (IOException x) {
            log.info(instanceName+": Cannot connect to peer "+peerHost+":"+peerPort);
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
                receiver.join();
            } catch (InterruptedException x) {
            }
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
        try {
            body();
    
        } catch (Throwable x) {
            log.error(instanceName+": caught unexpected exception within ReplicationInstance", x);
    
        } finally {
            closeSocket();
        }
    }

    /**
     * @throws SQLException 
     * @throws IOException 
     * @throws InterruptedException 
     * 
     */
    private void body()
        throws SQLException, IOException, InterruptedException
    {
        try {
            log.debug(instanceName+": replication instance has started");
    
            processProtocolMessages();
    
        } finally {
            // call process on any remaining elements in the queue -
            // if they require this.
            ReplicationMessage message;
            while ((message = messageQueue.poll()) != null) {
                if (!message.callOnlyIfConnected()) {
                    try {
                        message.process(this);
                    } catch (Exception x) {
                        log.error(instanceName+": unexpected exception when processing message in replication queue", x);
                    }
                }
            }
    
            // signaling all still waiting operations that the connection has failed
            synchronized (waitingOperations) {
                for (WaitingOperation<?> wo: waitingOperations.values()) {
                    wo.exception = new IOException("connection terminated when waiting for confirm");
                    wo.waitGate.release();
                    wo.watcher.cancel();
                }
                waitingOperations.clear();
            }
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
	try {
	    for (;;) {
		long now = System.currentTimeMillis();
		if (now >= nextStatisticsTimestamp) {
		    if (lastStatisticsTimestamp != 0L) {
			double transmittedBytesPerSecond =
			    (totalBytesTransmitted - lastStatisticsBytesTransmitted) /
				((now - lastStatisticsTimestamp) / 1000.);
			
			log.info(instanceName+String.format(": transmit rate = %7.1f KB/sec", transmittedBytesPerSecond/1000));
			log.info(instanceName+String.format(": queue size    = %5d", messageQueue.size()));
		    }
		    
		    lastStatisticsBytesTransmitted = totalBytesTransmitted;
		    lastStatisticsTimestamp = now;
		    nextStatisticsTimestamp = now + 300000;
		}
		
		long delta = nextStatisticsTimestamp - now;
		ReplicationMessage message = messageQueue.poll(delta, TimeUnit.MILLISECONDS);
		
		if (message != null) {
		    if (message == terminateMessage) break;

		    message.process(this);
		}
	    }
	    
	} catch (TerminateThread x) {
	    x.logError(log, instanceName);

	} catch (Exception x) {
	    log.error(instanceName+": unexpected exception when processing message from peer", x);
	    log.error(instanceName+": terminating connection!");
	}
	
	closeSocket();
    }

    /**
     * Enqueues a message for sending to the peer. This
     * method may be called from any thread. The actual 
     * sending of messages will be carried out in a 
     * sequence conserving manner
     */
    public void send(final ReplicationMessage message)
    {
	if (connectionCanceled) return;
	
	if (maxWaitingMessages > 0 && messageQueue.size() > maxWaitingMessages) {
	    log.error(instanceName+": too many waiting messages - replicator connection will be canceled");
	    cancelConnection();
	    return;
	}
	
	
        messageQueue.add(new ReplicationMessage() {
            private static final long serialVersionUID = 1L;
    
            @Override
            protected void process(ReplicationProtocolInstance instance)
                throws Exception
            {
                try {
                    sendToPeer(message);
                } catch (IOException x) {
                    log.error(instanceName+": unexpected exception when sending message to peer", x);
                    log.error(instanceName+": terminating connection!");
                    
                    closeSocket();
                }
            }

            @Override
            public int getSizeEstimate()
            {
        	return 4;
            }
       });
    }

    /**
     * 
     */
    private void cancelConnection()
    {
	connectionCanceled = true;
	
	messageQueue.add(new ReplicationMessage() {
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
	});

    }
    /**
     * 
     */
    protected void processReceivedMessage(Object message)
    {
        if (message instanceof ReplicationMessage) {
            log.debug(instanceName+": got "+message.getClass().getName()+" from protocol connection");
            messageQueue.add((ReplicationMessage)message);
    
        } else {
            log.debug(instanceName+": got unexpected object from protocol connection: "+(message == null ? "null" : message.getClass().getName()));
        }
    
    }

    /**
     * Will cause all outstanding messages to be sent. 
     * This method will wait until the sending has been completed.
     * Not that this does not necessarily mean that the peer
     * has processed all messages. The even may still get lost 
     * on network connection or peer failure.
     * @throws InterruptedException 
     */
    public void flush()
        throws InterruptedException
    {
        final Semaphore sema = new Semaphore(0);
        
        messageQueue.add(new ReplicationMessage() {
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
       });
        
        sema.acquire();
    }

    /**
     * This method behaves like flush() with additionally making
     * sure that the outstanding messages have been processed by 
     * the peer.
     * <p>
     * If no confirmation can be received from the peer, an
     * IOException will be raised.
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public void syncConnection()
        throws InterruptedException, IOException
    {
        WaitingOperation<Void> wo = new WaitingOperation<Void>(new SyncRequestMessage());

        log.debug(instanceName+": sending sync request to peer");
        wo.sendAndGetResult();
        log.debug(instanceName+": sync has been confirmed");
    }

    /**
     * 
     */
    public void terminate()
    {
        messageQueue.add(terminateMessage);
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
        log.debug(instanceName+": sending message to peer: "+message.getClass().getName());
        if (instanceThread == null) {
            instanceThread = Thread.currentThread();
        } else if (Thread.currentThread() != instanceThread) {
            throw new IllegalStateException("sendToPeer used by non-owener thread");
        }
        
        oos.writeObject(message);
        oos.flush();
        
        totalBytesTransmitted  += sizeEstimate;
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
        protected void process(ReplicationProtocolInstance instance) throws Exception
        {
            log.debug("processing operation "+operationId);
            CnfType cnf = performOperation(instance);
            
            log.debug("sending result "+operationId);
            instance.sendToPeer(new OperationConfirmMessage<CnfType>(cls, operationId, cnf));
        }
        
        protected abstract CnfType performOperation(ReplicationProtocolInstance instance);
        
        protected CnfType castConfirm(Object cnf)
        {
            return cls.cast(cnf);
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
        protected void process(ReplicationProtocolInstance instance) throws Exception
        {
            log.debug("processing operation confirm "+operationId);
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
            
            log.debug(instanceName+": waitingOperation "+operationId+" - reqClass="+request.getClass().getName());
            
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
                log.debug(instanceName+": send WaitingOperation "+operationId);
                send(request);
                log.debug(instanceName+": wait for result "+operationId);
                waitGate.acquire();
                log.debug(instanceName+": got result "+operationId);

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
    }

}