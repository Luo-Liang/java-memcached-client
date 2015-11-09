package net.spy.memcached.protocol.ascii.extended;

import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.protocol.ascii.*;
import net.spy.memcached.protocol.binary.*;

import java.util.Collection;
import java.util.Collections;

/**
 * Created by liangluo on 11/6/2015.
 */
public class BaseArithmeticOpImpl extends BaseGetOpImpl {
    static String EXTENSION_PREFIX = "extension.arithmetic.";
    public BaseArithmeticOpImpl(String c, OperationCallback cb, Collection<String> k) {
        super(EXTENSION_PREFIX+c, cb, k);
    }

    public BaseArithmeticOpImpl(String c, int e, OperationCallback cb, String k) {
      super(EXTENSION_PREFIX+c,e,cb,k);
    }
    //handle read and handle line are exactly the same as Get.
    //we'll keep the format the same.
    //expiration in the Arithmetic extension is not allowed, same as GET.
    //Note Spymemcached Gets seem to allow expiration, but that's not in the Memcached standard.
    //Expect reply to be in the same format.
}
