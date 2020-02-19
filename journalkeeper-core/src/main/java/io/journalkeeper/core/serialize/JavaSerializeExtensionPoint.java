package io.journalkeeper.core.serialize;

import io.journalkeeper.core.exception.SerializeException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * @author LiYue
 * Date: 2020/2/18
 */
public class JavaSerializeExtensionPoint implements SerializeExtensionPoint {
    @Override
    @SuppressWarnings("unchecked")
    public <E> E parse(byte[] bytes, int offset, int length) {
        if(length == 0) {
            return null;
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInput in = new ObjectInputStream(bis)) {
            return (E) in.readObject();
        } catch (IOException | ClassNotFoundException ioe) {
            throw new SerializeException(ioe);
        }
    }

    @Override
    public <E> byte[] serialize(E entry) {
        if(null == entry) {
            return new byte[0];
        }
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(entry);
            return bos.toByteArray();
        } catch (IOException ioe) {
            throw new SerializeException(ioe);
        }
    }
}
