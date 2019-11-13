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
import io.journalkeeper.core.exception.SerializeException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author LiYue
 * Date: 2019-08-27
 */
public class ReservedEntriesSerializeSupport {
    private static Map<Class<? extends ReservedEntry>, Serializer<? extends ReservedEntry>> serializerMap = new HashMap<>();
    private static Map<ReservedEntryType, Class<? extends ReservedEntry>> typeMap = new HashMap<>();

    static {
        registerType(ReservedEntryType.TYPE_LEADER_ANNOUNCEMENT, LeaderAnnouncementEntry.class, new LeaderAnnouncementEntrySerializer());
        registerType(ReservedEntryType.TYPE_COMPACT_JOURNAL, CompactJournalEntry.class, new CompactJournalEntrySerializer());
        registerType(ReservedEntryType.TYPE_SCALE_PARTITIONS, ScalePartitionsEntry.class, new ScalePartitionsEntrySerializer());
        registerType(ReservedEntryType.TYPE_UPDATE_VOTERS_S1, UpdateVotersS1Entry.class, new UpdateVotersS1EntrySerializer());
        registerType(ReservedEntryType.TYPE_UPDATE_VOTERS_S2, UpdateVotersS2Entry.class, new UpdateVotersS2EntrySerializer());
        registerType(ReservedEntryType.TYPE_SET_PREFERRED_LEADER, SetPreferredLeaderEntry.class, new SetPreferredLeaderEntrySerializer());
    }

    @SuppressWarnings("unchecked")
    public static  <E extends ReservedEntry> E parse(byte [] buffer, Class<E> eClass) {
        Object entry =  serializerMap.get(eClass).parse(buffer);
        if (eClass.isAssignableFrom(entry.getClass())) {
            return (E) entry;
        } else {
            throw new SerializeException("Type mismatch!");
        }
    }

    @SuppressWarnings("unchecked")
    public static  <E extends ReservedEntry> E parse(byte [] buffer, int offset, int length, Class<E> eClass) {
        byte [] body = offset == 0 ? buffer : Arrays.copyOfRange(buffer, offset, offset + length);
        Object entry =  serializerMap.get(eClass).parse(body);
        if (eClass.isAssignableFrom(entry.getClass())) {
            return (E) entry;
        } else {
            throw new SerializeException("Type mismatch!");
        }
    }

    public static  <E extends ReservedEntry> E parse(byte [] buffer) {
        ReservedEntryType type = parseEntryType(buffer);
        @SuppressWarnings("unchecked")
        Class<E> eClass = (Class<E> )typeMap.get(type);
        if(null == eClass) {
            throw new SerializeException(String.format("Unknown entry type: %s!", type.name()));
        } else {
            return parse(buffer, eClass);
        }

    }

    public static  <E extends ReservedEntry> E parse(byte [] buffer, int offset, int length) {
        ReservedEntryType type = parseEntryType(buffer, offset, length);
        @SuppressWarnings("unchecked")
        Class<E> eClass = (Class<E> )typeMap.get(type);
        if(null == eClass) {
            throw new SerializeException(String.format("Unknown entry type: %s!", type.name()));
        } else {
            return parse(buffer, offset, length, eClass);
        }

    }

    public static <E extends ReservedEntry> byte [] serialize(E  entry) {
        @SuppressWarnings("unchecked")
        Serializer<E> serializer = (Serializer<E>) serializerMap.get(entry.getClass());
        if(serializer == null) {
            throw new SerializeException(String.format("Unknown entry class type: %s", entry.getClass().toString()));
        }

        return serializer.serialize(entry);
    }

    public static ReservedEntryType parseEntryType(byte [] buffer) {
        return ReservedEntryType.valueOf(buffer[0]);
    }

    public static ReservedEntryType parseEntryType(byte [] buffer, int offset, int length) {
        return ReservedEntryType.valueOf(buffer[offset]);
    }

    private static <E extends ReservedEntry> void registerType(ReservedEntryType type, Class<E> eClass, Serializer<E> serializer) {
        serializerMap.put(eClass, serializer);
        typeMap.put(type, eClass);
    }

}
