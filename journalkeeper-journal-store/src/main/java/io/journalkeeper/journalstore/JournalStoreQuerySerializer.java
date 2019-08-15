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
package io.journalkeeper.journalstore;

import io.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;

public class JournalStoreQuerySerializer implements Serializer<JournalStoreQuery> {
    private static final int SIZE = Byte.BYTES + Short.BYTES + Integer.BYTES + Long.BYTES;

    @Override
    public byte[] serialize(JournalStoreQuery query) {
        byte [] bytes = new byte[SIZE];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.put((byte) query.getCmd());
        buffer.putShort((short) query.getPartition());
        buffer.putLong(query.getIndex());
        buffer.putInt(query.getSize());
        return bytes;
    }

    @Override
    public JournalStoreQuery parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new JournalStoreQuery(buffer.get(), buffer.getShort(), buffer.getLong(), buffer.getInt());
    }
}
