package io.journalkeeper.core.state;

import io.journalkeeper.base.ReplicableIterator;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author LiYue
 * Date: 2019/11/21
 */
public class FolderTrunkIterator implements ReplicableIterator {
    private final Path root;
    private final List<Path> files;
    private int fileIndex = 0;
    private long offsetOfCurrentFile = 0;
    private final int maxTrunkSize;

    public FolderTrunkIterator(Path root, List<Path> files, int maxTrunkSize) {
        this.root = root;
        this.maxTrunkSize = maxTrunkSize;
        this.files = toRelative(root, files);
    }

    private List<Path> toRelative(Path root, List<Path> files) {
        return files.stream()
                .map(src -> src.relativize(root))
                .collect(Collectors.toList());
    }

    // Trunk Header
    //      Length of file name bytes:  4 bytes
    //      File name bytes:            Variable bytes
    //      Trunk offset of the file    8 bytes
    // Trunk data                       Variable bytes
    @Override
    public byte[] nextTrunk() throws IOException {
        Path relFile = files.get(fileIndex);
        Path file = root.relativize(relFile);
        long fileSize = Files.size(file);
        long remainingSize ;

        byte [] trunk;
        ByteBuffer buffer;

        byte [] filenameBytes = relFile.toString().getBytes(StandardCharsets.UTF_8);
        remainingSize =
                fileSize +
                Integer.BYTES + filenameBytes.length  + Long.BYTES - // Header size
                offsetOfCurrentFile;

        trunk = new byte[(int) Math.min(remainingSize , maxTrunkSize)];
        buffer = ByteBuffer.wrap(trunk);
        buffer.putInt(filenameBytes.length);
        buffer.put(filenameBytes);
        buffer.putLong(offsetOfCurrentFile);

        int sizeToRead = buffer.remaining();
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r"); FileChannel fc = raf.getChannel()) {
            fc.position(offsetOfCurrentFile);
            while (buffer.hasRemaining()) {
                fc.read(buffer);
            }
        }

        offsetOfCurrentFile += sizeToRead;

        if (offsetOfCurrentFile == fileSize) {
           fileIndex ++;
           offsetOfCurrentFile = 0;
        }
        return trunk;
    }

    @Override
    public boolean hasMoreTrunks() {
        return fileIndex < files.size();
    }
}
