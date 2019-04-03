package com.jd.journalkeeper.core.journal;

import com.jd.journalkeeper.persistence.BufferPool;
import com.jd.journalkeeper.persistence.PersistenceFactory;
import com.jd.journalkeeper.rpc.RpcAccessPointFactory;
import com.jd.journalkeeper.utils.spi.ServiceSupport;
import com.jd.journalkeeper.utils.test.TestPathUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;

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

    private Journal createJournal() {
        PersistenceFactory persistenceFactory = ServiceSupport.load(PersistenceFactory.class);
        BufferPool bufferPool = ServiceSupport.load(BufferPool.class);
        return new Journal(
                persistenceFactory.createJournalPersistenceInstance(),
                persistenceFactory.createJournalPersistenceInstance(),
                bufferPool);
    }

    @After
    public void after() throws IOException {
        journal.close();
        TestPathUtils.destroyBaseDir();

    }
}
