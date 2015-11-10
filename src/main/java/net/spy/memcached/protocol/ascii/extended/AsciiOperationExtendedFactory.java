package net.spy.memcached.protocol.ascii.extended;

import net.spy.memcached.ops.extended.ArithmeticOperation;
import net.spy.memcached.protocol.ascii.AsciiOperationFactory;

import java.util.Collection;

/**
 * Created by Liang Luo Local on 11/9/2015.
 */
public class AsciiOperationExtendedFactory extends AsciiOperationFactory {
    //TODO: fill in the extended arithmetic operations.
    public ArithmeticOperation calculate(String cmd, Collection<String> keys,ArithmeticOperation.Callback callback)
    {
        return new BaseArithmeticOpImpl(cmd,callback,keys);
    }
}
