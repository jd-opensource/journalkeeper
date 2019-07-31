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
import io.journalkeeper.core.api.RaftEntry;
import io.journalkeeper.core.api.RaftEntryHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Header
 * -----------
 * Length: 4 Bytes
 * Partition: 2 Bytes
 * BatchSize: 2 Bytes
 * Offset: 2 Bytes
 *
 * Data
 * -----------
 * Entry payload: Variable Bytes (HEADER_LENGTH - 10)
 *
 * @author LiYue
 * Date: 2019-04-24
 */
public class RaftEntrySerializer implements Serializer<RaftEntry> {
    public final static int HEADER_LENGTH = Integer.BYTES + Short.BYTES  + Short.BYTES + Short.BYTES;
    private static final Logger logger = LoggerFactory.getLogger(RaftEntrySerializer.class);
    @Override
    public int sizeOf(RaftEntry entries) {
        return entries.getHeader().getPayloadLength();
    }

    @Override
    public byte[] serialize(RaftEntry entry) {
        byte [] bytes = new byte[sizeOf(entry)];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.putInt(entry.getHeader().getPayloadLength());
        byteBuffer.putShort((short )entry.getHeader().getPartition());
        byteBuffer.putShort((short )entry.getHeader().getBatchSize());
        byteBuffer.putShort((short )entry.getHeader().getOffset());
        byteBuffer.put(entry.getEntry());
        return bytes;
    }

    @Override
    public RaftEntry parse(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        RaftEntry entry = new RaftEntry();
        RaftEntryHeader header = new RaftEntryHeader();
        entry.setHeader(header);
        header.setPayloadLength(byteBuffer.getInt());
        header.setPartition(byteBuffer.getShort());
        header.setBatchSize(byteBuffer.getShort());
        header.setOffset(byteBuffer.getShort());
        entry.setEntry(
                Arrays.copyOfRange(bytes,HEADER_LENGTH, bytes.length)
        );
        return entry;
    }
}
