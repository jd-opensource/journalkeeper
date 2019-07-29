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
package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.core.api.RaftEntry;
import com.jd.journalkeeper.core.api.RaftEntryHeader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liyue25
 * Date: 2019-05-09
 *
 * Cmd : 1 Byte
 * Code: 1 Byte
 * Entries: Variable
 *  Entries size: 2 Bytes
 *  RaftEntry: Variable
 *      Header
 *      -----------
 *      Payload length: 4 Bytes
 *      Partition: 2 Bytes
 *      BatchSize: 2 Bytes
 *      Offset: 2 Bytes
 *
 *      Data
 *      -----------
 *      Entry payload: Variable Bytes (Header.PayloadLength)
 *  RaftEntry: Variable
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
    private static final int FIXED_LENGTH = Byte.BYTES + Byte.BYTES + Short.BYTES + Short.BYTES;
    private static final int ENTRY_HEADER_LENGTH = Integer.BYTES + Short.BYTES  + Short.BYTES + Short.BYTES;

    @Override
    public int sizeOf(JournalStoreQueryResult journalStoreQueryResult) {
        return

                (journalStoreQueryResult.getBoundaries() == null ? 0 :
                        journalStoreQueryResult.getBoundaries().size() * (Short.BYTES + Long.BYTES + Long.BYTES)) +
                (journalStoreQueryResult.getEntries() == null ? 0 :
                        journalStoreQueryResult.getEntries().stream().mapToInt(entry -> entry.getHeader().getPayloadLength()
                                + ENTRY_HEADER_LENGTH)
                        .sum()) + FIXED_LENGTH;

    }

    @Override
    public byte[] serialize(JournalStoreQueryResult journalStoreQueryResult) {
        byte [] bytes = new byte[sizeOf(journalStoreQueryResult)];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        buffer.put((byte) journalStoreQueryResult.getCmd());
        buffer.put((byte) journalStoreQueryResult.getCode());

        List<RaftEntry> entries = journalStoreQueryResult.getEntries();
        if(entries == null) {
            entries = Collections.emptyList();
        }
        buffer.putShort((short) entries.size());
        entries.forEach(entry -> {
            buffer.putInt(entry.getHeader().getPayloadLength());
            buffer.putShort((short )entry.getHeader().getPartition());
            buffer.putShort((short )entry.getHeader().getBatchSize());
            buffer.putShort((short )entry.getHeader().getOffset());
            buffer.put(entry.getEntry());
        });

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
        int entriesSize = buffer.getShort();
        List<RaftEntry> entries = new ArrayList<>(entriesSize);
        for (int i = 0; i < entriesSize; i++) {
            RaftEntry entry = new RaftEntry();
            RaftEntryHeader header = new RaftEntryHeader();
            entry.setHeader(header);
            header.setPayloadLength(buffer.getInt());
            header.setPartition(buffer.getShort());
            header.setBatchSize(buffer.getShort());
            header.setOffset(buffer.getShort());
            byte[] entryBytes = new byte[header.getPayloadLength()];
            buffer.get(entryBytes);
            entry.setEntry(entryBytes);
            entries.add(entry);
        }

        int boundariesSize = buffer.getShort();
        Map<Integer, JournalStoreQueryResult.Boundary> boundaries = new HashMap<>(boundariesSize);
        for (int i = 0; i < boundariesSize; i++) {
            boundaries.put((int) buffer.getShort(),
                    new JournalStoreQueryResult.Boundary(buffer.getLong(), buffer.getLong()));
        }

        return new JournalStoreQueryResult(entries, boundaries, cmd, code);

    }
}
