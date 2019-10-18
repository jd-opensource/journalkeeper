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
import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author LiYue
 * Date: 2019-05-09
 *
 * Cmd : 1 Byte
 * Code: 1 Byte
 * Index: 8 Bytes
 * Entries: Variable
 *  Entries size: 2 Bytes
 *  JournalEntry: Variable
 *  JournalEntry: Variable
 *  ...
 *
 * Boundaries
 *  Boundaries size: 2 Bytes
 *  Bourdary: 18 Bytes
 *      Partition(Key): 2 Bytes
 *      Boundary(Value): 16 Bytes
 *          Min: 8 Bytes
 *          Max: 8 Bytes
 *  Bourdary: 18 Bytes
 *  ...
 *
 */
public class JournalStoreQueryResultSerializer implements Serializer<JournalStoreQueryResult> {
    private static final int FIXED_LENGTH =
            Byte.BYTES + /* Cmd */
                    Byte.BYTES + /* Code */
                    Short.BYTES + /* Entries size */
                    Short.BYTES + /* Boundaries size */
                    Long.BYTES; /* Index size */
    private final JournalEntryParser journalEntryParser;

    public JournalStoreQueryResultSerializer(JournalEntryParser journalEntryParser) {
        this.journalEntryParser = journalEntryParser;
    }

    private int sizeOf(JournalStoreQueryResult journalStoreQueryResult) {
        return

                (journalStoreQueryResult.getBoundaries() == null ? 0 :
                        journalStoreQueryResult.getBoundaries().size() * (Short.BYTES + Long.BYTES + Long.BYTES)) +
                (journalStoreQueryResult.getEntries() == null ? 0 :
                        journalStoreQueryResult.getEntries().stream().mapToInt(JournalEntry::getLength)
                        .sum()) + FIXED_LENGTH;

    }

    @Override
    public byte[] serialize(JournalStoreQueryResult journalStoreQueryResult) {
        byte [] bytes = new byte[sizeOf(journalStoreQueryResult)];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        buffer.put((byte) journalStoreQueryResult.getCmd());
        buffer.put((byte) journalStoreQueryResult.getCode());
        buffer.putLong(journalStoreQueryResult.getIndex());
        List<JournalEntry> entries = journalStoreQueryResult.getEntries();
        if(entries == null) {
            entries = Collections.emptyList();
        }
        buffer.putShort((short) entries.size());
        entries.forEach(entry -> buffer.put(entry.getSerializedBytes()));

        Map<Integer, JournalStoreQueryResult.Boundary> boundaryMap = journalStoreQueryResult.getBoundaries();
        if(boundaryMap == null) {
            boundaryMap = Collections.emptyMap();
        }
        buffer.putShort((short) boundaryMap.size());
        boundaryMap.forEach((partition, boundary) -> {
            buffer.putShort(partition.shortValue());
            buffer.putLong(boundary.getMin());
            buffer.putLong(boundary.getMax());
        });

        return bytes;
    }

    @Override
    public JournalStoreQueryResult parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int cmd = buffer.get();
        int code = buffer.get();
        long index = buffer.getLong();
        int entriesSize = buffer.getShort();
        List<JournalEntry> entries = new ArrayList<>(entriesSize);
        byte [] headerBytes = new byte[journalEntryParser.headerLength()];
        for (int i = 0; i < entriesSize; i++) {
            buffer.mark();
            buffer.get(headerBytes);
            buffer.reset();
            JournalEntry header = journalEntryParser.parseHeader(headerBytes);
            int length = header.getLength();
            byte [] raw = new byte[length];
            buffer.get(raw);
            JournalEntry entry = journalEntryParser.parse(raw);
            entries.add(entry);
        }

        int boundariesSize = buffer.getShort();
        Map<Integer, JournalStoreQueryResult.Boundary> boundaries = new HashMap<>(boundariesSize);
        for (int i = 0; i < boundariesSize; i++) {
            boundaries.put((int) buffer.getShort(),
                    new JournalStoreQueryResult.Boundary(buffer.getLong(), buffer.getLong()));
        }

        return new JournalStoreQueryResult(entries, boundaries, cmd, index, code);

    }
}
