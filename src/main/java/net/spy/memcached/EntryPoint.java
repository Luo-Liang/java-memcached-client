package net.spy.memcached;

import net.spy.memcached.extended.ExtendedMemcachedClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * Created by liangluo on 11/4/2015.
 */
public class EntryPoint {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException {
        Short s = (short)0xff00;
        InetSocketAddress[] sa = new InetSocketAddress[]{new InetSocketAddress("127.0.0.1",11211)};
        ExtendedMemcachedClient client = new ExtendedMemcachedClient(new DefaultUDPSudoConnectionFactory(), Arrays.asList(sa));
        System.out.println(client.set("UW",0,"Seattle").get());
        System.out.println(client.get("UW"));
        ArrayList<String> keys = new ArrayList<String>();
        keys.add("UW");
        keys.add("UPENN");
        client.asyncCalculate(keys,"Add").get();
    }
}
