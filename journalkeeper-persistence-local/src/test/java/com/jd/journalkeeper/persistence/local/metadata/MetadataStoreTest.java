package com.jd.journalkeeper.persistence.local.metadata;

import com.jd.journalkeeper.persistence.ServerMetadata;
import com.jd.journalkeeper.utils.test.TestPathUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author liyue25
 * Date: 2019-04-03
 */
public class MetadataStoreTest {
    private static final Logger logger = LoggerFactory.getLogger(MetadataStoreTest.class);
    private Path path = null;

    @Test
    public void readWriteTest() throws IOException {
        ServerMetadata writeMetadata = createServerMetadata();
        // voters is not set

        MetadataStore writeStore = new MetadataStore();
        writeStore.recover(path,new Properties());
        writeStore.save(writeMetadata);

        MetadataStore readStore = new MetadataStore();
        ServerMetadata readMetadata = readStore.recover(path, new Properties());

        checkServerMetadataEquals(writeMetadata, readMetadata);

    }

    private void checkServerMetadataEquals(ServerMetadata writeMetadata, ServerMetadata readMetadata) {
        Assert.assertEquals(writeMetadata.getCommitIndex(), readMetadata.getCommitIndex());
        Assert.assertEquals(writeMetadata.getCurrentTerm(), readMetadata.getCurrentTerm());
        Assert.assertEquals(writeMetadata.getParents(), readMetadata.getParents());
        Assert.assertEquals(writeMetadata.getThisServer(), readMetadata.getThisServer());
        Assert.assertEquals(writeMetadata.getVotedFor(), readMetadata.getVotedFor());
        Assert.assertEquals(writeMetadata.getVoters(), readMetadata.getVoters());
    }

    @Test
    public void brokenFileTest() throws IOException {
        ServerMetadata writeMetadata = createServerMetadata();

        MetadataStore writeStore = new MetadataStore();
        writeStore.recover(path,new Properties());
        writeStore.save(writeMetadata);

        try(RandomAccessFile raf = new RandomAccessFile(path.resolve("0").toFile(),"rw"); FileChannel fileChannel = raf.getChannel()) {
            fileChannel.truncate(fileChannel.size() - 10);
        }

        MetadataStore readStore = new MetadataStore();
        ServerMetadata readMetadata = readStore.recover(path, new Properties());

        checkServerMetadataEquals(writeMetadata, readMetadata);

        writeStore.save(writeMetadata);

        try(RandomAccessFile raf = new RandomAccessFile(path.resolve("1").toFile(),"rw"); FileChannel fileChannel = raf.getChannel()) {
            fileChannel.truncate(fileChannel.size() - 10);
        }

        readStore = new MetadataStore();
        readMetadata = readStore.recover(path, new Properties());

        checkServerMetadataEquals(writeMetadata, readMetadata);

    }

    private ServerMetadata createServerMetadata() {
        ServerMetadata writeMetadata = new ServerMetadata();
        writeMetadata.setCommitIndex(2345666L);
        writeMetadata.setCurrentTerm(88);
        writeMetadata.setParents(Arrays.asList(
                URI.create("jk://parent1:9999"),
                URI.create("jk://parent2:9999"),
                URI.create("jk://parent3:9999")
        ));
        writeMetadata.setThisServer(URI.create("jk://localhost:9999"));
        writeMetadata.setVotedFor(URI.create("jk://new_leader:9993"));
        // voters is not set
        return writeMetadata;
    }

    @Before
    public void before() throws Exception {
        prepareBaseDir();

    }

    @After
    public void after() {
        destroyBaseDir();

    }

    private void destroyBaseDir() {
        TestPathUtils.destroyBaseDir();

    }

    private void prepareBaseDir() throws IOException {

        path = TestPathUtils.prepareBaseDir();
        logger.info("Base directory: {}.", path);
    }
}
