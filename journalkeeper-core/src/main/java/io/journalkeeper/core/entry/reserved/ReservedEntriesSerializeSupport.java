package io.journalkeeper.core.entry.reserved;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.exception.SerializeException;

import java.util.HashMap;
import java.util.Map;

/**
 * @author LiYue
 * Date: 2019-08-27
 */
public class ReservedEntriesSerializeSupport {
    private static Map<Class<? extends ReservedEntry>, Serializer<? extends ReservedEntry>> serializerMap = new HashMap<>();
    private static Map<Integer, Class<? extends ReservedEntry>> typeMap = new HashMap<>();

    static {
        registerType(ReservedEntry.TYPE_LEADER_ANNOUNCEMENT, LeaderAnnouncementEntry.class, new LeaderAnnouncementEntrySerializer());
        registerType(ReservedEntry.TYPE_COMPACT_JOURNAL, CompactJournalEntry.class, new CompactJournalEntrySerializer());
        registerType(ReservedEntry.TYPE_SCALE_PARTITIONS, ScalePartitionsEntry.class, new ScalePartitionsEntrySerializer());
        registerType(ReservedEntry.TYPE_UPDATE_VOTERS_S1, UpdateVotersS1Entry.class, new UpdateVotersS1EntrySerializer());
        registerType(ReservedEntry.TYPE_UPDATE_VOTERS_S2, UpdateVotersS2Entry.class, new UpdateVotersS2EntrySerializer());
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

    public static  <E extends ReservedEntry> E parse(byte [] buffer) {
        int type = parseEntryType(buffer);
        @SuppressWarnings("unchecked")
        Class<E> eClass = (Class<E> )typeMap.get(type);
        if(null == eClass) {
            throw new SerializeException(String.format("Unknown entry type: %d!", type));
        } else {
            return parse(buffer, eClass);
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

    public static int parseEntryType(byte [] buffer) {
        return (int) buffer[0];
    }

    private static <E extends ReservedEntry> void registerType(int type, Class<E> eClass, Serializer<E> serializer) {
        serializerMap.put(eClass, serializer);
        typeMap.put(type, eClass);
    }

}
