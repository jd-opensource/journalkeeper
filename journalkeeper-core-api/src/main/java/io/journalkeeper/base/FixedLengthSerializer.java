package io.journalkeeper.base;

/**
 * @author LiYue
 * Date: 2019/10/11
 */
public interface FixedLengthSerializer<T> extends Serializer<T> {
    int serializedEntryLength();
}
