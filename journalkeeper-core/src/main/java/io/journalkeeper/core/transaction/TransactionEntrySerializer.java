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
package io.journalkeeper.core.transaction;

import io.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;


/**
 *  transactionId           16 bytes
 *  timestamp               8 bytes
 *  type                    1 byte
 *  partition               2 bytes
 *  commitOrAbort           1 byte
 *  batchSize               2 bytes
 *  entry                   variable bytes
 *      length              4 bytes
 *      bytes               variable bytes
 *  context map             variable bytes
 *      map size            4 bytes
 *      map entry           variable bytes
 *          key string      variable bytes
 *              length      4 bytes
 *              bytes       variable bytes
 *          value string    variable bytes
 *              length      4 bytes
 *              bytes       variable bytes
 *      map entry           variable bytes
 *      map entry           variable bytes
 *
 *
 * @author LiYue
 * Date: 2019/10/22
 */
public class TransactionEntrySerializer implements Serializer<TransactionEntry> {
    private static final int FIXED_LENGTH = 30;

    @Override
    public byte[] serialize(TransactionEntry entry) {
        int size = FIXED_LENGTH +
                Integer.BYTES + (entry.getEntry() == null ? 0 : entry.getEntry().length) +
                Integer.BYTES + (
                entry.getContext() == null ? 0 :
                        Stream.concat(entry.getContext().keySet().stream(), entry.getContext().values().stream())
                                .mapToInt(s -> Integer.BYTES + s.getBytes(StandardCharsets.UTF_8).length).sum()
        );

        byte[] bytes = new byte[size];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // transactionId
        serializeUUID(entry.getTransactionId(), buffer);

        // timestamp
        buffer.putLong(entry.getTimestamp());

        // type
        buffer.put((byte) entry.getType().value());

        // partition
        buffer.putShort((short) entry.getPartition());
        // commitOrAbort
        buffer.put(entry.isCommitOrAbort() ? (byte) 1 : (byte) 0);
        // batchSize
        buffer.putShort((short) entry.getBatchSize());
        // entry
        if (entry.getEntry() == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(entry.getEntry().length);
            buffer.put(entry.getEntry());
        }
        // context
        if (entry.getContext() == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(entry.getContext().size());
            entry.getContext().entrySet().stream()
                    .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
                    .forEach(s -> {
                        byte[] sb = s.getBytes(StandardCharsets.UTF_8);
                        buffer.putInt(sb.length);
                        buffer.put(sb);
                    });
        }

        return bytes;
    }

    private void serializeUUID(UUID uuid, ByteBuffer buffer) {
        long mostSigBits = 0L;
        long leastSigBits = 0L;
        if (null != uuid) {
            mostSigBits = uuid.getMostSignificantBits();
            leastSigBits = uuid.getLeastSignificantBits();
        }
        buffer.putLong(mostSigBits);
        buffer.putLong(leastSigBits);
    }

    private UUID parseUUID(ByteBuffer buffer) {
        long mostSigBits = buffer.getLong();
        long leastSigBits = buffer.getLong();

        if (mostSigBits == 0L && leastSigBits == 0L) {
            return null;
        } else {
            return new UUID(mostSigBits, leastSigBits);
        }

    }

    @Override
    public TransactionEntry parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte[] entryBytes = null;
        buffer.position(FIXED_LENGTH);
        int length = buffer.getInt();
        if (length >= 0) {
            entryBytes = new byte[length];
            buffer.get(entryBytes);
        }
        Map<String, String> context = null;
        int size = buffer.getInt();
        if (size >= 0) {
            context = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                context.put(getString(buffer), getString(buffer));
            }
        }

        buffer.clear();
        return new TransactionEntry(
                parseUUID(buffer),
                buffer.getLong(),
                TransactionEntryType.valueOf(buffer.get()),
                buffer.getShort(),
                buffer.get() == (byte) 1,
                buffer.getShort(),
                entryBytes,
                context
        );
    }

    private String getString(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length >= 0) {
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return null;
    }
}
