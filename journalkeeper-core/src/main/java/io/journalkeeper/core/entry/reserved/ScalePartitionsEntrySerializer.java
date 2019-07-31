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
import java.util.LinkedList;
import java.util.List;

/**
 * @author LiYue
 * Date: 2019-05-09
 *
 * Type: 1 byte
 * Partition: 2 bytes
 * Partition: 2 bytes
 * Partition: 2 bytes
 * ...
 *
 */
public class ScalePartitionsEntrySerializer implements Serializer<ScalePartitionsEntry> {
    @Override
    public int sizeOf(ScalePartitionsEntry scalePartitionsEntry) {
        return Byte.BYTES + Short.BYTES * scalePartitionsEntry.getPartitions().length;
    }

    @Override
    public byte[] serialize(ScalePartitionsEntry entry) {
        byte [] bytes = new byte[sizeOf(entry)];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.put((byte) entry.getType());
        for (int partition : entry.getPartitions()) {
            buffer.putShort((short) partition);
        }
        return bytes;
    }

    @Override
    public ScalePartitionsEntry parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, Byte.BYTES, bytes.length - Byte.BYTES);
        List<Integer> partitionList = new LinkedList<>();
        while (buffer.hasRemaining()) {
            partitionList.add(Short.valueOf(buffer.getShort()).intValue());
        }
        return new ScalePartitionsEntry(partitionList.stream().mapToInt(Integer::intValue).toArray());
    }
}
