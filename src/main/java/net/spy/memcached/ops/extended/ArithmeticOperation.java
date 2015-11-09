package net.spy.memcached.ops.extended;

import net.spy.memcached.ops.KeyedOperation;

/**
 * Created by Liang Luo Local on 11/9/2015.
 */
public interface ArithmeticOperation extends KeyedOperation {
    String getArithmeticOpType();
}
