package net.spy.memcached.protocol;

import net.spy.memcached.*;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Liang Luo Local on 11/2/2015.
 * We don't need fancy functions, and only need TEXT protocol.
 * This is a dirty way of doing this.
 * TODO:Might need to lift both classes to form a common base..
 */
public abstract class UDPMemcachedNodeImpl extends MemcachedNodeImpl{
    public UDPMemcachedNodeImpl(SocketAddress sa, DatagramChannel dc, int bufSize,
                                BlockingQueue<Operation> rq, BlockingQueue<Operation> wq,
                                BlockingQueue<Operation> iq, long opQueueMaxBlockTime,
                                boolean waitForAuth, long dt, long authWaitTime, ConnectionFactory fact) {
        super(sa,dc,bufSize,rq,wq,iq,opQueueMaxBlockTime,waitForAuth,dt,authWaitTime,fact);
    }

    @Override
    public  int getSelectionOps() {
        int rv = 0;
        if (((DatagramChannel) getChannel()).isConnected()) {
            if (hasReadOp()) {
                rv |= SelectionKey.OP_READ;
            }
            if (toWrite > 0 || hasWriteOp()) {
                rv |= SelectionKey.OP_WRITE;
            }
        } else {
            rv = SelectionKey.OP_CONNECT;
        }
        return rv;
    }
    @Override
    public boolean isActive() {
        return reconnectAttempt.get() == 0 && ((SocketChannel) getChannel()) != null
                && ((DatagramChannel) getChannel()).isConnected();
    }

    @Override
    public int writeSome() throws IOException {
        int wrote = ((DatagramChannel) channel).write(wbuf);
        assert wrote >= 0 : "Wrote negative bytes?";
        toWrite -= wrote;
        assert toWrite >= 0 : "toWrite went negative after writing " + wrote
                + " bytes for " + this;
        getLogger().debug("Wrote %d bytes", wrote);
        return wrote;
    }
}