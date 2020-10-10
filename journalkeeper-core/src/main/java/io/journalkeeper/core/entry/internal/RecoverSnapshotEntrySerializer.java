/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
public class RecoverSnapshotEntrySerializer extends InternalEntrySerializer<RecoverSnapshotEntry> {

    @Override
    protected byte[] serialize(RecoverSnapshotEntry entry, byte[] header) {
        int size = header.length + Long.BYTES;
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        byteBuffer.put(header);
        byteBuffer.putLong(entry.getIndex());
        return byteBuffer.array();
    }

    @Override
    protected RecoverSnapshotEntry parse(ByteBuffer byteBuffer, int type, int version) {
        RecoverSnapshotEntry result = new RecoverSnapshotEntry(version);
        result.setIndex(byteBuffer.getLong());
        return result;
    }

}