/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.sql.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoPool;
import io.journalkeeper.base.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * KryoSerializer
 * author: gaohaoxiang
 * date: 2019/5/30
 */
public class KryoSerializer<T> implements Serializer<T> {

    private static final int BUFFER_SIZE = 1024 * 5;

    private final KryoPool kryoPool;

    private Class<T> type;

    public KryoSerializer(Class<T> type) {
        this.type = type;
        this.kryoPool = new KryoPool.Builder(() -> {
            Kryo kryo = new Kryo();
            if (type != null) {
                kryo.register(type);
            }
            return kryo;
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
    public T parse(byte[] bytes) {
        Kryo kryo = kryoPool.borrow();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        Input input = new Input(inputStream);

        T result = kryo.readObject(input, type);
        kryoPool.release(kryo);
        input.close();
        return result;
    }
}