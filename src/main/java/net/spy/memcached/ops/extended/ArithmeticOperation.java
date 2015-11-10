package net.spy.memcached.ops.extended;

import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.KeyedOperation;
import net.spy.memcached.ops.OperationCallback;

/**
 * Created by Liang Luo Local on 11/9/2015.
 */
public interface ArithmeticOperation extends KeyedOperation {
    interface Callback extends OperationCallback {
        /**
         * Callback for each result from a get.
         *
         * @param key the key that was retrieved
         * @param flags the flags for this value
         * @param data the data stored under this key
         */
        void gotData(String key, int flags, byte[] data);

        String getArithmeticType();
    }
}
