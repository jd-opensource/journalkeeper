package io.journalkeeper.core.api.transaction;

import io.journalkeeper.core.api.transaction.TransactionId;

import java.util.Objects;
import java.util.UUID;


/**
 * @author LiYue
 * Date: 2019/11/29
 */
public class UUIDTransactionId  implements TransactionId {
    private final UUID uuid;
    public UUIDTransactionId(UUID uuid) {
        this.uuid = uuid;
    }

    public UUID getUuid() {
        return uuid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UUIDTransactionId that = (UUIDTransactionId) o;
        return Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid);
    }

    @Override
    public String toString() {
        return "UUIDTransactionId{" +
                "uuid=" + uuid +
                '}';
    }
}
