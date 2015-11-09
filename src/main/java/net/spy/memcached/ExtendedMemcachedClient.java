package net.spy.memcached;

import net.spy.memcached.CachedData;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.transcoders.Transcoder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

/**
 * Created by Liang Luo Local on 11/8/2015.
 */
public class ExtendedMemcachedClient extends MemcachedClient {

    public ExtendedMemcachedClient(InetSocketAddress... ia) throws IOException {
        super(ia);
    }

    public ExtendedMemcachedClient(List<InetSocketAddress> addrs) throws IOException {
        super(addrs);
    }

    public ExtendedMemcachedClient(ConnectionFactory cf, List<InetSocketAddress> addrs) throws IOException {
        super(cf, addrs);
    }

    public GetFuture<Object> asyncArithmeticAdd(final String key) {

        final CountDownLatch latch = new CountDownLatch(1);
        final GetFuture<Object> rv = new GetFuture<Object>(latch, operationTimeout, key,
                executorService);
        final Transcoder<Object> tc = transcoder;
        Operation op = opFact.get(key, new GetOperation.Callback() {
            private Future<Object> val;

            @Override
            public void receivedStatus(OperationStatus status) {
                rv.set(val, status);
            }

            @Override
            public void gotData(String k, int flags, byte[] data) {
                assert key.equals(k) : "Wrong key returned";
                val =
                        tcService.decode(tc, new CachedData(flags, data, tc.getMaxSize()));
            }

            @Override
            public void complete() {
                latch.countDown();
                rv.signalComplete();
            }
        });
        rv.setOperation(op);
        mconn.enqueueOperation(key, op);
        return rv;
    }
}
