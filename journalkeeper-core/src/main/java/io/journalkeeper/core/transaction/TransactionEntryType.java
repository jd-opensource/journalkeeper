package io.journalkeeper.core.transaction;

/**
 * @author LiYue
 * Date: 2019/10/22
 */
public enum TransactionEntryType {
    TRANSACTION_EMPTY(-1),
    TRANSACTION_START(0),
    TRANSACTION_ENTRY(1),
    TRANSACTION_PRE_COMPLETE(2),
    TRANSACTION_COMPLETE(3);

    private int value;

    TransactionEntryType(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static TransactionEntryType valueOf(final int value) {
        switch (value) {
            case 0:
                return TRANSACTION_START;
            case 1:
                return TRANSACTION_ENTRY;
            case 2:
                return TRANSACTION_PRE_COMPLETE;
            case 3:
                return TRANSACTION_COMPLETE;
            case -1:
                return TRANSACTION_EMPTY;
            default:
                throw new IllegalArgumentException("Illegal TransactionEntryType value!");
        }
    }
}
