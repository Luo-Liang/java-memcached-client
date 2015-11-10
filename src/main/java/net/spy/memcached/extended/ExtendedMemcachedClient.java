package net.spy.memcached.extended;

import net.spy.memcached.CachedData;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.KeyUtil;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.extended.ArithmeticOperation;
import net.spy.memcached.protocol.ascii.extended.AsciiOperationExtendedFactory;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.util.StringUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

/**
 * Created by Liang Luo Local on 11/8/2015.
 */
public class ExtendedMemcachedClient extends MemcachedClient {

    AsciiOperationExtendedFactory opFactory = new AsciiOperationExtendedFactory();

    public ExtendedMemcachedClient(InetSocketAddress... ia) throws IOException {
        super(ia);
    }

    public ExtendedMemcachedClient(List<InetSocketAddress> addrs) throws IOException {
        super(addrs);
    }

    public ExtendedMemcachedClient(ConnectionFactory cf, List<InetSocketAddress> addrs) throws IOException {
        super(cf, addrs);
    }



    public GetFuture<Object> asyncCalculate(final Collection<String> keys, String cmd) throws NoSuchAlgorithmException {

        final CountDownLatch latch = new CountDownLatch(1);
        String sudoReuqestKey = KeyUtil.getKeysDigest(keys);
        final GetFuture<Object> rv = new GetFuture<Object>(latch, operationTimeout, sudoReuqestKey,
                executorService);
        final Transcoder<Object> tc = transcoder;
        Operation op = opFactory.calculate(cmd, keys, new ArithmeticOperation.Callback() {
            private Future<Object> val;

            @Override
            public void receivedStatus(OperationStatus status) {
                rv.set(val, status);
            }

            @Override
            public void gotData(String k, int flags, byte[] data) {
                assert sudoReuqestKey.equals(k) : "Wrong key returned";
                //TODO:we haven't decided which keys to return yet. We currently choose to return a
                //TODO:digest of all keys.
                val = tcService.decode(tc, new CachedData(flags, data, tc.getMaxSize()));
            }

            @Override
            public String getArithmeticType() {
                return cmd;
            }

            @Override
            public void complete() {
                latch.countDown();
                rv.signalComplete();
            }
        });
        rv.setOperation(op);
        mconn.enqueueOperation(sudoReuqestKey, op);
        return rv;
    }
}
