package com.jd.journalkeeper.core.state;

import com.jd.journalkeeper.core.api.State;
import com.jd.journalkeeper.core.api.StateFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public abstract class LocalState<E, Q, R> implements State<E, Q, R> {

    protected long lastApplied = 0;
    protected int lastIncludedTerm = 0;
    protected Path path;
    protected Properties properties;
    protected StateFactory<E, Q, R> factory;
    @Override
    public long lastApplied() {
        return lastApplied;
    }

    @Override
    public int lastIncludedTerm() {
        return lastIncludedTerm;
    }

    @Override
    public final void recover(Path path, Properties properties) {
        this.path = path;
        this.properties = properties;

        recoverLocalState(path, properties);
    }

    /**
     * 从本地文件恢复状态，如果不存在则创建新的。
     */
    protected abstract void recoverLocalState(Path path, Properties properties);

    /**
     * 列出所有复制时需要拷贝的文件
     */
    protected List<Path> listAllFiles() {
        return FileUtils.listFiles(
                path.toFile(),
                new RegexFileFilter("^(.*?)"),
                DirectoryFileFilter.DIRECTORY
        ).stream().map(File::toPath).collect(Collectors.toList());
    }

    @Override
    public State<E, Q, R> takeASnapshot(Path destPath) throws IOException {
        State<E, Q, R> state = factory.createState();
        //TODO: 复制文件的时候是否要停机?
        List<Path> srcFiles = listAllFiles();

        List<Path> destFiles = srcFiles.stream()
                .map(src -> src.relativize(path))
                .map(destPath::resolve)
                .collect(Collectors.toList());
        for (int i = 0; i < destFiles.size(); i++) {
            Path srcFile = srcFiles.get(i);
            Path destFile = destFiles.get(i);
            File folder = destFile.getParent().toFile();
            if(!folder.isDirectory()) {
                folder.mkdirs();
            }
            FileUtils.copyFile(srcFile.toFile(), destFile.toFile());
        }

        state.recover(destPath, properties);
        return state;
    }

    @Override
    public byte[] readSerializedData(long offset, long size) {

        return new byte[0];
    }

    @Override
    public long serializedDataSize() {
        return 0;
    }

    @Override
    public void install(byte[] data, long offset) {

    }

    @Override
    public void installFinish(long lastApplied, int lastIncludedTerm) {

    }

    @Override
    public void clear() {

    }

    @Override
    public CompletableFuture<R> query(Q query) {
        return null;
    }
}
