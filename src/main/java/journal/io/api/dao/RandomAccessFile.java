package journal.io.api.dao;


import journal.io.api.Journal;
import journal.io.api.Location;
import journal.io.api.operation.WriteCommand;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

public class RandomAccessFile implements FileAccess {
    private java.io.RandomAccessFile randomAccessFile;
    private FileAccessConfiguration configuration;

    public RandomAccessFile(File file, FileAccessConfiguration configuration) throws FileNotFoundException {
        randomAccessFile = new java.io.RandomAccessFile(file, "rw");
        this.configuration = configuration;
    }

    @Override
    public long writeFileHeader() throws IOException {
        try {
            randomAccessFile.write(configuration.getMagicStringBytes());
            randomAccessFile.writeInt(configuration.getStorageVersion());
            return configuration.getFileHeaderSize();
        } finally {
            randomAccessFile.close();
        }
    }

    @Override
    public void verifyFileHeader() throws IOException {
        java.io.RandomAccessFile raf = randomAccessFile;
        try {
            byte[] magic = new byte[configuration.getMagicStringSize()];
            if (raf.read(magic) == configuration.getMagicStringSize() && Arrays.equals(magic, configuration.getMagicStringBytes())) {
                int version = raf.readInt();
                if (version != configuration.getStorageVersion()) {
                    throw new IllegalStateException("Incompatible storage version, found: " + version + ", required: " + configuration.getStorageVersion());
                }
            } else {
                throw new IOException("Incompatible magic string!");
            }
        } finally {
            raf.close();
        }
    }

    @Override
    public void skip(int size) throws IOException {
        int n = 0;
        while (n < size) {
            int skipped = randomAccessFile.skipBytes(size - n);
            if (skipped == 0 && randomAccessFile.getFilePointer() >= randomAccessFile.length()) {
                throw new EOFException();
            }
            n += skipped;
        }
    }

    @Override
    public long getCurrentPosition() throws IOException {
        return randomAccessFile.getFilePointer();
    }

    @Override
    public long size() throws IOException {
        return randomAccessFile.length();
    }

    @Override
    public boolean skipLocationData(Location location) throws IOException {
        int toSkip = location.getSize() - configuration.getRecordHeaderSize();
        if (size() - getCurrentPosition() >= toSkip) {
            skip(toSkip);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        randomAccessFile.close();
    }

    @Override
    public void setSize(long size) throws IOException {
        randomAccessFile.setLength(size);
        sync();
    }

    @Override
    public void sync() throws IOException {
        randomAccessFile.getFD().sync();
    }

    @Override
    public byte[] readLocationData(Location location) throws IOException {
        if (location.isBatchControlRecord()) {
            byte[] checksum = new byte[configuration.getChecksumSize()];
            randomAccessFile.read(checksum);
            return checksum;
        } else {
            byte[] data = new byte[location.getSize() - configuration.getRecordHeaderSize()];
            randomAccessFile.readFully(data);
            return data;
        }
    }

    @Override
    public void seek(long position) throws IOException {
        randomAccessFile.seek(position);
    }

    @Override
    public HeaderRecord readRecordHeader() throws IOException {
        ByteBuffer headerBuffer = ByteBuffer.allocate(configuration.getRecordHeaderSize());
        randomAccessFile.getChannel().read(headerBuffer);
        headerBuffer.flip();

        int pointer = headerBuffer.getInt();
        int size = headerBuffer.getInt();
        byte type = headerBuffer.get();
        return HeaderRecord.of(pointer, size, type);
    }

    @Override
    public boolean hasRecordHeader(long position, boolean isJournalOpened) throws IOException {
        long remaining = size() - position;
        if (remaining >= configuration.getRecordHeaderSize()) {
            return true;
        } else if (remaining == 0) {
            return false;
        } else {

            if (isJournalOpened) {
                // If journal is open, it means the positions may be wrong due to
                // compaction:
                return false;
            } else {
                // If journal is not open yet, it means we are recovering and
                // need to signal a failure:
                throw new IllegalStateException("Remaining file length doesn't fit a record header at position: " + position);
            }
        }
    }

    @Override
    public void write(byte[] data) throws IOException {
        randomAccessFile.write(data);
    }

    @Override
    public byte[] createDataForWrite(int size, Queue<WriteCommand> writes, boolean checksum) {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        Checksum adler32 = new Adler32();
        WriteCommand control = writes.peek();


        // Write an empty batch control record.
        buffer.putInt(control.getLocation().getPointer());
        buffer.putInt(configuration.getBatchControlRecordSize());
        buffer.put(Location.BATCH_CONTROL_RECORD_TYPE);
        buffer.putLong(0);

        Iterator<WriteCommand> commands = writes.iterator();
        // Skip the control write:
        commands.next();
        // Process others:
        while (commands.hasNext()) {
            WriteCommand current = commands.next();
            buffer.putInt(current.getLocation().getPointer());
            buffer.putInt(current.getLocation().getSize());
            buffer.put(current.getLocation().getType());
            buffer.put(current.getData());
            if (checksum) {
                adler32.update(current.getData(), 0, current.getData().length);
            }
        }

        // Now we can fill in the batch control record properly.
        buffer.position(configuration.getRecordHeaderSize());
        if (checksum) {
            buffer.putLong(adler32.getValue());
        }
        return buffer.array();
    }
}
