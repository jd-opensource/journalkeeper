package io.journalkeeper.core.api;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.UpdateRequest;

/**
 * @author LiYue
 * Date: 2019/11/14
 */
public class SerializedUpdateRequest extends UpdateRequest<byte[]> {
    public <E> SerializedUpdateRequest(UpdateRequest<E> updateRequest, Serializer<E> serializer) {
        super(serializer.serialize(updateRequest.getEntry()), updateRequest.getPartition(), updateRequest.getBatchSize());
    }

    public SerializedUpdateRequest(byte [] entry , int partition, int batchSize) {
        super(entry, partition, batchSize);
    }
}
