package net.spy.memcached;

import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.VBucketAware;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.*;

/**
 * Created by Liang Luo Local on 11/2/2015.
 * Needs to implements this because we don't need channels.
 * TODO: we need to refactor (lifting this and MemcchedConnection to a common base class).
 */
public class MemcachedUDPSudoConnection extends MemcachedConnection {
    public MemcachedUDPSudoConnection(final int bufSize, final ConnectionFactory f,
                                      final List<InetSocketAddress> a, final Collection<ConnectionObserver> obs,
                                      final FailureMode fm, final OperationFactory opfactory) throws IOException {
        super(bufSize, f, a, obs, fm, opfactory);
    }

    @Override
    protected List<MemcachedNode> createConnections(
            final Collection<InetSocketAddress> addrs) throws IOException {
        List<MemcachedNode> connections = new ArrayList<MemcachedNode>(addrs.size());

        for (SocketAddress sa : addrs) {
            DatagramChannel dc = DatagramChannel.open();
            dc.configureBlocking(false);
            MemcachedNode qa = connectionFactory.createMemcachedNode(sa, dc, bufSize);
            qa.setConnection(this);
            connections.add(qa);
            int ops = 0;
            try {
                dc.connect(sa);
                getLogger().info("Connected to %s immediately", qa);
                connected(qa);

                selector.wakeup();
                qa.setSk(dc.register(selector, ops, qa));
                assert dc.isConnected()
                        || qa.getSk().interestOps() == SelectionKey.OP_CONNECT
                        : "Not connected, and not wanting to connect";
            } catch (SocketException e) {
                getLogger().warn("Socket error on initial connect", e);
                queueReconnect(qa);
            }
            connections.add(qa);
        }
        return connections;
    }

    protected void queueReconnect(final MemcachedNode node) {
        if (shutDown) {
            return;
        }
        getLogger().warn("Closing, and reopening %s, attempt %d.", node,
                node.getReconnectCount());

        if (node.getSk() != null) {
            node.getSk().cancel();
            assert !node.getSk().isValid() : "Cancelled selection key is valid";
        }
        node.reconnecting();

        if (node.getChannel() != null && ((DatagramChannel) node.getChannel()).socket() != null) {
            ((DatagramChannel) node.getChannel()).socket().close();
        } else {
            getLogger().info("The channel or socket was null for %s", node);
        }
        node.setChannel(null);

        long delay = (long) Math.min(maxDelay, Math.pow(2,
                node.getReconnectCount())) * 1000;
        long reconnectTime = System.currentTimeMillis() + delay;
        while (reconnectQueue.containsKey(reconnectTime)) {
            reconnectTime++;
        }

        reconnectQueue.put(reconnectTime, node);
        metrics.incrementCounter(RECON_QUEUE_METRIC);

        node.setupResend();
        if (failureMode == FailureMode.Redistribute) {
            redistributeOperations(node.destroyInputQueue());
        } else if (failureMode == FailureMode.Cancel) {
            cancelOperations(node.destroyInputQueue());
        }
    }

    protected boolean selectorsMakeSense() {
        for (MemcachedNode qa : locator.getAll()) {
            if (qa.getSk() != null && qa.getSk().isValid()) {
                if (((DatagramChannel) qa.getChannel()).isConnected()) {
                    int sops = qa.getSk().interestOps();
                    int expected = 0;
                    if (qa.hasReadOp()) {
                        expected |= SelectionKey.OP_READ;
                    }
                    if (qa.hasWriteOp()) {
                        expected |= SelectionKey.OP_WRITE;
                    }
                    if (qa.getBytesRemainingToWrite() > 0) {
                        expected |= SelectionKey.OP_WRITE;
                    }
                    assert sops == expected : "Invalid ops:  " + qa + ", expected "
                            + expected + ", got " + sops;
                } else {
                    int sops = qa.getSk().interestOps();
                    assert sops == SelectionKey.OP_CONNECT
                            : "Not connected, and not watching for connect: " + sops;
                }
            }
        }
        getLogger().debug("Checked the selectors.");
        return true;
    }

    @Override
    protected void attemptReconnects() {
        final long now = System.currentTimeMillis();
        final Map<MemcachedNode, Boolean> seen =
                new IdentityHashMap<MemcachedNode, Boolean>();
        final List<MemcachedNode> rereQueue = new ArrayList<MemcachedNode>();
        DatagramChannel ch = null;


        Iterator<MemcachedNode> i = reconnectQueue.headMap(now).values().iterator();
        while (i.hasNext()) {
            final MemcachedNode node = i.next();
            i.remove();
            metrics.decrementCounter(RECON_QUEUE_METRIC);

            try {
                if (!belongsToCluster(node)) {
                    getLogger().debug("Node does not belong to cluster anymore, "
                            + "skipping reconnect: %s", node);
                    continue;
                }

                if (!seen.containsKey(node)) {
                    seen.put(node, Boolean.TRUE);
                    getLogger().info("Reconnecting %s", node);

                    ch = DatagramChannel.open();
                    ch.configureBlocking(false);
                    int ops = 0;
                    ch.connect(node.getSocketAddress());
                    connected(node);
                    addedQueue.offer(node);
                    getLogger().info("Immediately reconnected to %s", node);
                    assert ch.isConnected();
                    node.registerChannel(ch, ch.register(selector, ops, node));
                    assert node.getChannel() == ch : "Channel was lost.";
                } else {
                    getLogger().debug("Skipping duplicate reconnect request for %s",
                            node);
                }
            } catch (SocketException e) {
                getLogger().warn("Error on reconnect", e);
                rereQueue.add(node);
            } catch (Exception e) {
                getLogger().error("Exception on reconnect, lost node %s", node, e);
            } finally {
                potentiallyCloseLeakingChannel(ch, node);
            }
        }

        for (MemcachedNode n : rereQueue) {
            queueReconnect(n);
        }
    }

    @Override
    protected void handleReads(MemcachedNode node) throws IOException {
        Operation currentOp = null;
        ByteBuffer rbuf = node.getRbuf();
        final DatagramChannel channel =(DatagramChannel)node.getChannel();
        int read = channel.read(rbuf);
        metrics.updateHistogram(OVERALL_AVG_BYTES_READ_METRIC, read);
        while (read > 0) {
            getLogger().debug("Read %d bytes", read);
            rbuf.flip();
            while (rbuf.remaining() > 0) {
                long Headers = 0;
                Headers = rbuf.getLong();
                short sequenceNumber = (short)(Headers >>48);
                //System.out.println("Current Sequence Number::" + sequenceNumber);
                currentOp = node.getCurrentReadOp(sequenceNumber);
                long timeOnWire =
                        System.nanoTime() - currentOp.getWriteCompleteTimestamp();
                metrics.updateHistogram(OVERALL_AVG_TIME_ON_WIRE_METRIC,
                        (int)(timeOnWire / 1000));
                metrics.markMeter(OVERALL_RESPONSE_METRIC);
                synchronized(currentOp) {
                    readBufferAndLogMetrics(currentOp, rbuf, node);
                    if(currentOp.getState()== OperationState.COMPLETE || currentOp.getState() == OperationState.RETRY)
                    {
                        node.removeCurrentReadOp(sequenceNumber);
                    }
                }
                //currentOp = node.getCurrentReadOp();
            }
            rbuf.clear();
            read = channel.read(rbuf);
            node.completedRead();
        }
    }

    @Override
    protected void potentiallyCloseLeakingChannel(final AbstractSelectableChannel ch,
                                                  final MemcachedNode node) {
        if (ch != null && !((DatagramChannel) ch).isConnected()) {
            try {
                ch.close();
            } catch (IOException e) {
                getLogger().error("Exception closing channel: %s", node, e);
            }
        }
    }

    @Override
    protected void connected(final MemcachedNode node) {
        assert ((DatagramChannel) node.getChannel()).isConnected() : "Not connected.";
        int rt = node.getReconnectCount();
        node.connected();

        for (ConnectionObserver observer : connObservers) {
            observer.connectionEstablished(node.getSocketAddress(), rt);
        }
    }

    @Override
    protected void handleIO(final SelectionKey sk) {
        MemcachedNode node = (MemcachedNode) sk.attachment();

        try {
            getLogger().debug("Handling IO for:  %s (r=%s, w=%s, c=%s, op=%s)", sk,
                    sk.isReadable(), sk.isWritable(), sk.isConnectable(),
                    sk.attachment());
            if (sk.isConnectable() && belongsToCluster(node)) {
                getLogger().debug("Connection state changed for %s", sk);
                final DatagramChannel channel = (DatagramChannel) node.getChannel();
                finishConnect(sk, node);
            } else {
                handleReadsAndWrites(sk, node);
            }
        } catch (ClosedChannelException e) {
            if (!shutDown) {
                getLogger().info("Closed channel and not shutting down. Queueing"
                        + " reconnect on %s", node, e);
                lostConnection(node);
            }
        } catch (ConnectException e) {
            getLogger().info("Reconnecting due to failure to connect to %s", node, e);
            queueReconnect(node);
        } catch (OperationException e) {
            node.setupForAuth();
            getLogger().info("Reconnection due to exception handling a memcached "
                            + "operation on %s. This may be due to an authentication failure.",
                    node, e);
            lostConnection(node);
        } catch (Exception e) {
            node.setupForAuth();
            getLogger().info("Reconnecting due to exception on %s", node, e);
            lostConnection(node);
        }
        node.fixupOps();
    }

    //byte[] SanityCheck = new byte[8];
    @Override
    protected void readBufferAndLogMetrics(final Operation currentOp,
                                           final ByteBuffer rbuf, MemcachedNode node) throws IOException {
        //rbuf.get(SanityCheck);
        currentOp.readFromBuffer(rbuf);
        if (currentOp.getState() == OperationState.COMPLETE) {
            getLogger().debug("Completed read op: %s and giving the next %d "
                    + "bytes", currentOp, rbuf.remaining());
            Operation op = currentOp;
            if (op.hasErrored()) {
                metrics.markMeter(OVERALL_RESPONSE_FAIL_METRIC);
            } else {
                metrics.markMeter(OVERALL_RESPONSE_SUCC_METRIC);
            }
        } else if (currentOp.getState() == OperationState.RETRY) {
            handleRetryInformation(currentOp.getErrorMsg());
            getLogger().debug("Reschedule read op due to NOT_MY_VBUCKET error: "
                    + "%s ", currentOp);
            ((VBucketAware) currentOp).addNotMyVbucketNode(
                    currentOp.getHandlingNode());
            //Operation op = node.removeCurrentReadOp();
            retryOps.add(currentOp);
            metrics.markMeter(OVERALL_RESPONSE_RETRY_METRIC);
        }
    }
}
