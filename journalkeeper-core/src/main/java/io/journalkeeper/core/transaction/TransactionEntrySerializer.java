package io.journalkeeper.core.transaction;

import io.journalkeeper.base.Serializer;

/**
 * @author LiYue
 * Date: 2019/10/22
 */
public class TransactionEntrySerializer implements Serializer<TransactionEntry> {
    @Override
    public byte[] serialize(TransactionEntry entry) {
        return new byte[0];
    }

    @Override
    public TransactionEntry parse(byte[] bytes) {
        return null;
    }
}
