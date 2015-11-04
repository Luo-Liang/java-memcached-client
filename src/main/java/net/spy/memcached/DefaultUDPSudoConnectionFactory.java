package net.spy.memcached;

import net.spy.memcached.protocol.ascii.AsciiOperationFactory;
import net.spy.memcached.protocol.UDPMemcachedNodeImpl;
import net.spy.memcached.protocol.ascii.AsciiUDPMemcachedNodeImpl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.DatagramChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.List;

/**
 * Created by Liang Luo Local on 11/2/2015.
 */
public class DefaultUDPSudoConnectionFactory extends DefaultConnectionFactory {

    @Override
    public MemcachedNode createMemcachedNode(SocketAddress sa, AbstractSelectableChannel dc, int bufSize)
    {
        OperationFactory of = getOperationFactory();
        if (of instanceof AsciiOperationFactory) {
            return new AsciiUDPMemcachedNodeImpl(sa,
                    (DatagramChannel)dc,
                    bufSize,
                    createReadOperationQueue(),
                    createWriteOperationQueue(),
                    createOperationQueue(),
                    getOpQueueMaxBlockTime(),
                    getOperationTimeout(),
                    getAuthWaitTime(),
                    this);
        } else {
            throw new IllegalStateException("Unhandled operation factory type " + of);
        }
    }

    public MemcachedConnection createConnection(List<InetSocketAddress> addrs)
            throws IOException {
        return new MemcachedUDPSudoConnection(getReadBufSize(), this, addrs,
                getInitialObservers(), getFailureMode(), getOperationFactory());
    }

}
