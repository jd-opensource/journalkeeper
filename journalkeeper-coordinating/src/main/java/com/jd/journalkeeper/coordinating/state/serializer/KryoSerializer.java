package com.jd.journalkeeper.coordinating.state.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.jd.journalkeeper.base.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * KryoSerializer
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public class KryoSerializer implements Serializer {

    private static final int BUFFER_SIZE = 1024 * 5;

    private final KryoPool kryoPool;

    private Class<?> type;

    public KryoSerializer() {
        this(null);
    }

    public KryoSerializer(Class<?> type) {
        this.type = type;
        this.kryoPool = new KryoPool.Builder(new KryoFactory() {
            @Override
            public Kryo create() {
                Kryo kryo = new Kryo();
                if (type != null) {
                    kryo.register(type);
                }
                return kryo;
            }
        }).build();
    }

    @Override
    public int sizeOf(Object o) {
        return serialize(o).length;
    }

    @Override
    public byte[] serialize(Object entry) {
        Kryo kryo = kryoPool.borrow();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(BUFFER_SIZE);
        Output output = new Output(outputStream);

        if (type == null) {
            kryo.writeClassAndObject(output, entry);
        } else {
            kryo.writeObject(output, entry);
        }
        kryoPool.release(kryo);
        output.flush();
        byte[] result = outputStream.toByteArray();
        output.close();
        return result;
    }

    @Override
    public Object parse(byte[] bytes) {
        Kryo kryo = kryoPool.borrow();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        Input input = new Input(inputStream);

        Object result = (type == null ? kryo.readClassAndObject(input) : kryo.readObject(input, type));
        kryoPool.release(kryo);
        input.close();
        return result;
    }
}