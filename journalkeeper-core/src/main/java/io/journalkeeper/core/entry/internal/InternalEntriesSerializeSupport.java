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
import io.journalkeeper.exceptions.SerializeException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author LiYue
 * Date: 2019-08-27
 */
public class InternalEntriesSerializeSupport {
    private static Map<Class<? extends InternalEntry>, Serializer<? extends InternalEntry>> serializerMap = new HashMap<>();
    private static Map<InternalEntryType, Class<? extends InternalEntry>> typeMap = new HashMap<>();

    static {
        registerType(InternalEntryType.TYPE_LEADER_ANNOUNCEMENT, LeaderAnnouncementEntry.class, new LeaderAnnouncementEntrySerializer());
        registerType(InternalEntryType.TYPE_CREATE_SNAPSHOT, CreateSnapshotEntry.class, new CreateSnapshotEntrySerializer());
        registerType(InternalEntryType.TYPE_SCALE_PARTITIONS, ScalePartitionsEntry.class, new ScalePartitionsEntrySerializer());
        registerType(InternalEntryType.TYPE_UPDATE_VOTERS_S1, UpdateVotersS1Entry.class, new UpdateVotersS1EntrySerializer());
        registerType(InternalEntryType.TYPE_UPDATE_VOTERS_S2, UpdateVotersS2Entry.class, new UpdateVotersS2EntrySerializer());
        registerType(InternalEntryType.TYPE_SET_PREFERRED_LEADER, SetPreferredLeaderEntry.class, new SetPreferredLeaderEntrySerializer());
        registerType(InternalEntryType.TYPE_RECOVER_SNAPSHOT, RecoverSnapshotEntry.class, new RecoverSnapshotEntrySerializer());
    }

    @SuppressWarnings("unchecked")
    public static <E extends InternalEntry> E parse(byte[] buffer, Class<E> eClass) {
        Object entry = serializerMap.get(eClass).parse(buffer);
        if (eClass.isAssignableFrom(entry.getClass())) {
            return (E) entry;
        } else {
            throw new SerializeException("Type mismatch!");
        }
    }

    @SuppressWarnings("unchecked")
    public static <E extends InternalEntry> E parse(byte[] buffer, int offset, int length, Class<E> eClass) {
        byte[] body = offset == 0 ? buffer : Arrays.copyOfRange(buffer, offset, offset + length);
        Object entry = serializerMap.get(eClass).parse(body);
        if (eClass.isAssignableFrom(entry.getClass())) {
            return (E) entry;
        } else {
            throw new SerializeException("Type mismatch!");
        }
    }

    public static <E extends InternalEntry> E parse(byte[] buffer) {
        InternalEntryType type = parseEntryType(buffer);
        @SuppressWarnings("unchecked")
        Class<E> eClass = (Class<E>) typeMap.get(type);
        if (null == eClass) {
            throw new SerializeException(String.format("Unknown entry type: %s!", type.name()));
        } else {
            return parse(buffer, eClass);
        }

    }

    public static <E extends InternalEntry> E parse(byte[] buffer, int offset, int length) {
        InternalEntryType type = parseEntryType(buffer, offset);
        @SuppressWarnings("unchecked")
        Class<E> eClass = (Class<E>) typeMap.get(type);
        if (null == eClass) {
            throw new SerializeException(String.format("Unknown entry type: %s!", type.name()));
        } else {
            return parse(buffer, offset, length, eClass);
        }

    }

    public static <E extends InternalEntry> byte[] serialize(E entry) {
        @SuppressWarnings("unchecked")
        Serializer<E> serializer = (Serializer<E>) serializerMap.get(entry.getClass());
        if (serializer == null) {
            throw new SerializeException(String.format("Unknown entry class type: %s", entry.getClass().toString()));
        }

        return serializer.serialize(entry);
    }

    public static InternalEntryType parseEntryType(byte[] bytes) {
        return parseEntryType(bytes,0);
    }

    public static InternalEntryType parseEntryType(byte[] bytes, int offset) {
        int type;

        if (bytes[offset] == InternalEntrySerializer.ENTRY_HEADER_MAGIC_CODE) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes,offset, Byte.BYTES + Short.BYTES);
            buffer.get(); // magic code
            type = buffer.getShort();
        } else {
            type = bytes[offset];
        }
        return InternalEntryType.valueOf(type);
    }

    private static <E extends InternalEntry> void registerType(InternalEntryType type, Class<E> eClass, Serializer<E> serializer) {
        serializerMap.put(eClass, serializer);
        typeMap.put(type, eClass);
    }

}
