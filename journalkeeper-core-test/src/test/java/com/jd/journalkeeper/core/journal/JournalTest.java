package com.jd.journalkeeper.core.journal;

import com.jd.journalkeeper.persistence.BufferPool;
import com.jd.journalkeeper.persistence.PersistenceFactory;
import com.jd.journalkeeper.rpc.RpcAccessPointFactory;
import com.jd.journalkeeper.utils.spi.ServiceSupport;
import com.jd.journalkeeper.utils.test.ByteUtils;
import com.jd.journalkeeper.utils.test.TestPathUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author liyue25
 * Date: 2019-04-03
 */
public class JournalTest {
    private Path path = null;
    Journal journal = null;
    @Before
    public void before() throws IOException {
       path = TestPathUtils.prepareBaseDir();

       journal = createJournal();
    }

    @Test
    public void writeReadEntryTest() {
        int maxLength = 1024;
        int size = 1024;
        int term = 8;
        List<byte []> entries = ByteUtils.createByteList(maxLength, size);
        List<StorageEntry> storageEntries =
                entries.stream()
                        .map(entry -> new StorageEntry(entry, term))
                        .collect(Collectors.toList());
        long maxIndex = 0L;
        for(StorageEntry storageEntry: storageEntries) {
            maxIndex = journal.append(storageEntry);
        }
        Assert.assertEquals(size, maxIndex);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());
        long index = journal.minIndex();

        while (index < journal.maxIndex()) {
            Assert.assertArrayEquals(entries.get((int)index), journal.read(index));
            index++;
        }

    }
    @Test
    public void recoverTest() throws IOException {
        int maxLength = 1024;
        int size = 1024;
        int term = 8;
        List<byte []> entries = ByteUtils.createByteList(maxLength, size);
        List<StorageEntry> storageEntries =
                entries.stream()
                        .map(entry -> new StorageEntry(entry, term))
                        .collect(Collectors.toList());
        long maxIndex = 0L;
        for(StorageEntry storageEntry: storageEntries) {
            maxIndex = journal.append(storageEntry);
        }
        Assert.assertEquals(size, maxIndex);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());
        long index = journal.minIndex();

        journal.flush();
        journal.close();

        journal = createJournal();
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());

        while (index < journal.maxIndex()) {
            Assert.assertArrayEquals(entries.get((int)index), journal.read(index));
            index++;
        }

    }

    private Journal createJournal() throws IOException {
        PersistenceFactory persistenceFactory = ServiceSupport.load(PersistenceFactory.class);
        BufferPool bufferPool = ServiceSupport.load(BufferPool.class);
        Journal journal = new Journal(
                persistenceFactory.createJournalPersistenceInstance(),
                persistenceFactory.createJournalPersistenceInstance(),
                bufferPool);
        journal.recover(path,new Properties());
        return journal;
    }

    @After
    public void after() throws IOException {
        journal.close();
        TestPathUtils.destroyBaseDir();

    }
}
