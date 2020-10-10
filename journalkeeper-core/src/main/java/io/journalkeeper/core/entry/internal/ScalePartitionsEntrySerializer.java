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
import java.util.HashSet;
import java.util.Set;

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
public class ScalePartitionsEntrySerializer extends InternalEntrySerializer<ScalePartitionsEntry> {
    private int sizeOf(ScalePartitionsEntry scalePartitionsEntry) {
        return Short.BYTES * scalePartitionsEntry.getPartitions().size();
    }

    @Override
    protected byte[] serialize(ScalePartitionsEntry entry, byte[] header) {
        byte[] bytes = new byte[header.length + sizeOf(entry)];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.put(header);
        for (int partition : entry.getPartitions()) {
            buffer.putShort((short) partition);
        }
        return bytes;
    }

    @Override
    protected ScalePartitionsEntry parse(ByteBuffer buffer, int type, int version) {
        Set<Integer> partitions = new HashSet<>();
        while (buffer.hasRemaining()) {
            partitions.add(Short.valueOf(buffer.getShort()).intValue());
        }
        return new ScalePartitionsEntry(partitions, version);
    }
}
