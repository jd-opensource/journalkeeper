package io.journalkeeper.core;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.StateFactory;

import java.io.Serializable;
import java.util.Properties;

/**
 * @author LiYue
 * Date: 2019-08-12
 */
public class JdkSerializerBootStrap<E extends Serializable, ER extends Serializable, Q extends Serializable, QR extends Serializable>
        extends BootStrap<E, ER, Q, QR> {
    public JdkSerializerBootStrap(RaftServer.Roll roll, StateFactory<E, ER, Q, QR> stateFactory,
                                  Class<E> eClass, Class<ER> erClass, Class<Q> qClass, Class<QR> qrClass,
                                  Properties properties) {
        super(roll, stateFactory,
                JdkSerializerFactory.createSerializer(eClass),
                JdkSerializerFactory.createSerializer(erClass),
                JdkSerializerFactory.createSerializer(qClass),
                JdkSerializerFactory.createSerializer(qrClass),
                properties);
    }
}
