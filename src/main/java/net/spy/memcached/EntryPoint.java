package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * Created by liangluo on 11/4/2015.
 */
public class EntryPoint {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        InetSocketAddress[] sa = new InetSocketAddress[]{new InetSocketAddress("173.250.235.211",11222)};
        MemcachedClient client = new MemcachedClient(new DefaultUDPSudoConnectionFactory(), Arrays.asList(sa));
        System.out.println(client.set("Microsoft",0,"Windows 10 Professional").get());
        System.out.println(client.get("Microsoft"));
    }
}
