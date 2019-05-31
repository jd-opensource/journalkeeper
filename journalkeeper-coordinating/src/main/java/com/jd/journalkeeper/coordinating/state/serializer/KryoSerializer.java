package com.jd.journalkeeper.coordinating.state.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
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

    private static final int BUFFER_SIZE = 1024 * 1024 * 2;

    private Class<?> type;

    public KryoSerializer(Class<?> type) {
        this.type = type;
    }

    @Override
    public int sizeOf(Object o) {
        return serialize(o).length;
    }

    @Override
    public byte[] serialize(Object entry) {
        Kryo kryo = new Kryo();
        kryo.register(type);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(BUFFER_SIZE);
        Output output = new Output(outputStream);
        kryo.writeObject(output, entry);
        output.flush();
        byte[] result = outputStream.toByteArray();
        output.close();
        return result;
    }

    @Override
    public Object parse(byte[] bytes) {
        Kryo kryo = new Kryo();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        Input input = new Input(inputStream);

        try {
            return kryo.readObject(input, type);
        } finally {
            input.close();
        }
    }
}