/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.persistence.local.metadata;

import io.journalkeeper.persistence.ServerMetadata;
import io.journalkeeper.utils.test.TestPathUtils;
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

/**
 * @author LiYue
 * Date: 2019-04-03
 */
public class MetadataStoreTest {
    private static final Logger logger = LoggerFactory.getLogger(MetadataStoreTest.class);
    private Path path = null;

    @Test
    public void readWriteTest() throws IOException {
        ServerMetadata writeMetadata = createServerMetadata();
        // voters is not set

        JsonDoubleCopiesPersistence writeStore = new JsonDoubleCopiesPersistence();
        writeStore.save(path, writeMetadata);

        JsonDoubleCopiesPersistence readStore = new JsonDoubleCopiesPersistence();
        ServerMetadata readMetadata = readStore.load(path, ServerMetadata.class);

        Assert.assertEquals(writeMetadata, readMetadata);

    }

    @Test
    public void brokenFileTest() throws IOException {
        ServerMetadata writeMetadata = createServerMetadata();

        JsonDoubleCopiesPersistence writeStore = new JsonDoubleCopiesPersistence();
        writeStore.save(path, writeMetadata);

        try(RandomAccessFile raf = new RandomAccessFile(writeStore.getCopy(path, JsonDoubleCopiesPersistence.FIRST_COPY).toFile(),"rw"); FileChannel fileChannel = raf.getChannel()) {
            fileChannel.truncate(fileChannel.size() - 10);
        }

        JsonDoubleCopiesPersistence readStore = new JsonDoubleCopiesPersistence();
        ServerMetadata readMetadata = readStore.load(path, ServerMetadata.class);

        Assert.assertEquals(writeMetadata, readMetadata);

        writeStore.save(path, writeMetadata);

        try(RandomAccessFile raf = new RandomAccessFile(writeStore.getCopy(path, JsonDoubleCopiesPersistence.SECOND_COPY).toFile(),"rw"); FileChannel fileChannel = raf.getChannel()) {
            fileChannel.truncate(fileChannel.size() - 10);
        }

        readStore = new JsonDoubleCopiesPersistence();
        readMetadata = readStore.load(path, ServerMetadata.class);

        Assert.assertEquals(writeMetadata, readMetadata);

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
        path = path.resolve("metadata");
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
