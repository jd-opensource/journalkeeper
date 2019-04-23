package com.jd.journalkeeper.journalstore;


import com.jd.journalkeeper.core.api.StateFactory;
import com.jd.journalkeeper.core.state.LocalState;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * @author liyue25
 * Date: 2019-04-23
 */
public class JournalState extends LocalState<ByteBuffer, JournalStoreQuery, List<ByteBuffer>> {
    private final static String FILENAME = "index.map";
    private Path statePath;
    private NavigableMap<Long, Long> indexMap;
    protected JournalState(StateFactory<ByteBuffer, JournalStoreQuery, List<ByteBuffer>> stateFactory) {
        super(stateFactory);
    }

    @Override
    protected void recoverLocalState(Path path, Properties properties) {
        this.statePath = path;
        indexMap = recoverIndexMap(path.resolve(FILENAME));
    }

    private NavigableMap<Long, Long> recoverIndexMap(Path path) {
        NavigableMap<Long,Long> recoverMap = new TreeMap<>();

        try {
            byte [] bytes = FileUtils.readFileToByteArray(path.toFile());
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            if(bytes.length >= Integer.BYTES) {
                int skip = buffer.getInt();
                buffer.position(buffer.position() + skip * 2 * Long.BYTES);
                while (buffer.hasRemaining()) {
                    recoverMap.put(buffer.getLong(), buffer.getLong());
                }
            }

        } catch (IOException e) {
            // TODO
        }
        return recoverMap;
    }

    @Override
    public Map<String, String> execute(ByteBuffer entry) {
        return null;
    }

    @Override
    public CompletableFuture<List<ByteBuffer>> query(JournalStoreQuery query) {
        return null;
    }
}
