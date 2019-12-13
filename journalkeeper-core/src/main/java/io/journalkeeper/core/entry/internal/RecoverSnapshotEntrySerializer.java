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
package io.journalkeeper.core.entry.internal;

import io.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;

/**
 * RecoverSnapshotEntrySerializer
 *
 * type BYTE(1)
 * index LONG(8)
 *
 * author: gaohaoxiang
 * date: 2019/12/12
 */
public class RecoverSnapshotEntrySerializer implements Serializer<RecoverSnapshotEntry> {

    @Override
    public byte[] serialize(RecoverSnapshotEntry entry) {
        int size = sizeOf(entry);
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        byteBuffer.put((byte) InternalEntryType.TYPE_RECOVER_SNAPSHOT.value());
        byteBuffer.putLong(entry.getIndex());
        return byteBuffer.array();
    }

    @Override
    public RecoverSnapshotEntry parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        RecoverSnapshotEntry result = new RecoverSnapshotEntry();
        buffer.get();
        result.setIndex(buffer.getLong());
        return result;
    }

    protected int sizeOf(RecoverSnapshotEntry entry) {
        return Byte.BYTES +  // type: 1 byte
                Long.BYTES; // index: 8 byte
    }
}