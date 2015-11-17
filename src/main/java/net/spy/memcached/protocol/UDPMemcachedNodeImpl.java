package net.spy.memcached.protocol;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Liang Luo Local on 11/2/2015.
 * We don't need fancy functions, and only need TEXT protocol.
 * This is a dirty way of doing this.
 * TODO:Might need to lift both classes to form a common base..
 */
public abstract class UDPMemcachedNodeImpl extends MemcachedNodeImpl{

    Short sequence = 0;
    Map<Short, Operation> readMap;

    public UDPMemcachedNodeImpl(SocketAddress sa, DatagramChannel dc, int bufSize,
                                Map<Short, Operation> rMap, BlockingQueue<Operation> wq,
                                BlockingQueue<Operation> iq, long opQueueMaxBlockTime,
                                boolean waitForAuth, long dt, long authWaitTime, ConnectionFactory fact) {
        super(sa, dc, bufSize, null, wq, iq, opQueueMaxBlockTime, waitForAuth, dt, authWaitTime, fact);
        //readQ = writeQ = null; //do not allow.
        readMap = rMap;
    }

    @Override
    public Operation getCurrentReadOp(short seq) {
        return readMap.get(seq);
    }

    @Override
    public Operation removeCurrentReadOp(short seq)
    {
        return readMap.remove(seq);
    }

    @Override
    public boolean hasReadOp() {
        return readMap.size() != 0;
    }

    @Override
    public int getSelectionOps() {
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
        return reconnectAttempt.get() == 0 && ((DatagramChannel) getChannel()) != null
                && ((DatagramChannel) getChannel()).isConnected();
    }

    //int UdpSequence = 0;
    int last4Bytes = 1 << 16;
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

    @Override
    protected Operation getNextWritableOp() {
        Operation o = getCurrentWriteOp();
        while (o != null && o.getState() == OperationState.WRITE_QUEUED) {
            synchronized (o) {
                if (o.isCancelled()) {
                    getLogger().debug("Not writing cancelled op.");
                    Operation cancelledOp = removeCurrentWriteOp();
                    assert o == cancelledOp;
                } else if (o.isTimedOut(defaultOpTimeout)) {
                    getLogger().debug("Not writing timed out op.");
                    Operation timedOutOp = removeCurrentWriteOp();
                    assert o == timedOutOp;
                } else {
                    o.writing();
                    short seq = sequence++;
                    o.setId(seq);
                    readMap.put(seq, o);
                    return o;
                }
                o = getCurrentWriteOp();
            }
        }
        return o;
    }

    @Override
    public void fillWriteBuffer(boolean shouldOptimize) {
        if (toWrite == 0) {
            getWbuf().clear();
            Operation o = getNextWritableOp();

            while (o != null && toWrite < getWbuf().capacity()) {
                synchronized (o) {
                    assert o.getState() == OperationState.WRITING;

                    ByteBuffer obuf = o.getBuffer();
                    assert obuf != null : "Didn't get a write buffer from " + o;
                    int bytesToCopy = Math.min(getWbuf().remaining() + 8, obuf.remaining() + 8);
                    getWbuf().putShort(o.getId());
                    getWbuf().putShort((short)0);
                    getWbuf().putInt(last4Bytes);
                    byte[] b = new byte[bytesToCopy - 8];
                    obuf.get(b);
                    getWbuf().put(b);
                    getLogger().debug("After copying stuff from %s: %s", o, getWbuf());
                    if (!o.getBuffer().hasRemaining()) {
                        o.writeComplete();
                        transitionWriteItem();

                        preparePending();
                        if (shouldOptimize) {
                            optimize();
                        }

                        o = getNextWritableOp();
                    }
                    toWrite += bytesToCopy;
                }
            }
            getWbuf().flip();
            assert toWrite <= getWbuf().capacity() : "toWrite exceeded capacity: "
                    + this;
            assert toWrite == getWbuf().remaining() : "Expected " + toWrite
                    + " remaining, got " + getWbuf().remaining();
        } else {
            getLogger().debug("Buffer is full, skipping");
        }
    }

    @Override
    public String toString() {
        int sops = 0;
        if (getSk() != null && getSk().isValid()) {
            sops = getSk().interestOps();
        }
        int rsize = readMap.size() + (optimizedOp == null ? 0 : 1);
        int wsize = readMap.size();
        int isize = inputQueue.size();
        return "{QA sa=" + getSocketAddress() + ", #Rops=" + rsize
                + ", #Wops=" + wsize
                + ", #iq=" + isize
                + ", toWrite=" + toWrite
                + ", interested=" + sops + "}";
    }
}
