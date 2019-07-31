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
package io.journalkeeper.core.entry.reserved;

import io.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * @author LiYue
 * Date: 2019-05-09
 *
 * Type: 1 byte
 * Partition: 2 bytes
 * To index: 8 bytes
 * Partition: 2 bytes
 * To index: 8 bytes
 * Partition: 2 bytes
 * To index: 8 bytes
 * ...
 *
 */
public class CompactJournalEntrySerializer implements Serializer<CompactJournalEntry> {
    @Override
    public int sizeOf(CompactJournalEntry compactJournalEntry) {
        return Byte.BYTES +  (Short.BYTES + Long.BYTES) * compactJournalEntry.getCompactIndices().size();    }

    @Override
    public byte[] serialize(CompactJournalEntry entry) {
        byte [] bytes = new byte[sizeOf(entry)];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.put((byte) entry.getType());
        entry.getCompactIndices().forEach((partition, index) -> {
            buffer.putShort(partition.shortValue());
            buffer.putLong(index);
        });
        return bytes;
    }

    @Override
    public CompactJournalEntry parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, Byte.BYTES, bytes.length - Byte.BYTES);
        Map<Integer, Long> toIndices = new HashMap<>();
        while (buffer.hasRemaining()) {
            toIndices.put((int) buffer.getShort(), buffer.getLong());
        }
        return new CompactJournalEntry(toIndices);
    }
}
