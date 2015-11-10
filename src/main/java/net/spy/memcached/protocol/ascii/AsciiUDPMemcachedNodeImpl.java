package net.spy.memcached.protocol.ascii;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.protocol.ProxyCallback;
import net.spy.memcached.protocol.UDPMemcachedNodeImpl;

import java.net.SocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Liang Luo Local on 11/3/2015.
 */
public class AsciiUDPMemcachedNodeImpl extends UDPMemcachedNodeImpl {

    public AsciiUDPMemcachedNodeImpl(SocketAddress sa, DatagramChannel c, int bufSize,
                                     Map<Short,Operation> rMap, BlockingQueue<Operation> wq,
                                     BlockingQueue<Operation> iq, Long opQueueMaxBlockTimeNs, long dt,
                                     long at, ConnectionFactory fa) {
        // ASCII never does auth
        super(sa, c, bufSize, rMap, wq, iq, opQueueMaxBlockTimeNs, false, dt, at, fa);
    }

    @Override
    protected void optimize() {
        if (writeQ.peek() instanceof GetOperation) {
            optimizedOp = writeQ.remove();
            if (writeQ.peek() instanceof GetOperation) {
                OptimizedGetImpl og = new OptimizedGetImpl((GetOperation) optimizedOp);
                optimizedOp = og;

                while (writeQ.peek() instanceof GetOperation) {
                    GetOperationImpl o = (GetOperationImpl) writeQ.remove();
                    if (!o.isCancelled()) {
                        og.addOperation(o);
                    }
                }

                // Initialize the new mega get
                optimizedOp.initialize();
                assert optimizedOp.getState() == OperationState.WRITE_QUEUED;
                ProxyCallback pcb = (ProxyCallback) og.getCallback();
                getLogger().debug("Set up %s with %s keys and %s callbacks", this,
                        pcb.numKeys(), pcb.numCallbacks());
            }
        }
    }
}
