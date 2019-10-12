package io.journalkeeper.core.api;

/**
 * @author LiYue
 * Date: 2019/10/12
 */
public interface JournalEntry {
    int getBatchSize();
    void setBatchSize(int batchSize);
    int getPartition();
    void setPartition(int partition);
    int getOffset();
    void setOffset(int offset);
    int getTerm();
    void setTerm(int term);
    BytesFragment getPayload();

    byte[] getSerializedBytes();

    int getLength();

}
