package journal.io.api.dao;


import journal.io.api.Journal;
import journal.io.api.Location;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class RandomAccessFile implements FileAccessBase {
    private java.io.RandomAccessFile randomAccessFile;

    public RandomAccessFile(File file) throws FileNotFoundException {
        randomAccessFile = new java.io.RandomAccessFile(file, "rw");
    }

    @Override
    public long writeHeader() throws IOException {
        try {
            randomAccessFile.write(Journal.MAGIC_STRING);
            randomAccessFile.writeInt(Journal.STORAGE_VERSION);
            return Journal.FILE_HEADER_SIZE;
        } finally {
            randomAccessFile.close();
        }
    }

    @Override
    public void verifyHeader() throws IOException {
        java.io.RandomAccessFile raf = randomAccessFile;
        try {
            byte[] magic = new byte[Journal.MAGIC_SIZE];
            if (raf.read(magic) == Journal.MAGIC_SIZE && Arrays.equals(magic, Journal.MAGIC_STRING)) {
                int version = raf.readInt();
                if (version != Journal.STORAGE_VERSION) {
                    throw new IllegalStateException("Incompatible storage version, found: " + version + ", required: " + Journal.STORAGE_VERSION);
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
        int toSkip = location.getSize() - Journal.RECORD_HEADER_SIZE;
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
            byte[] checksum = new byte[Journal.CHECKSUM_SIZE];
            randomAccessFile.read(checksum);
            return checksum;
        } else {
            byte[] data = new byte[location.getSize() - Journal.RECORD_HEADER_SIZE];
            randomAccessFile.readFully(data);
            return data;
        }
    }

    @Override
    public void seek(long position) throws IOException {
        randomAccessFile.seek(position);
    }

    @Override
    public HeaderRecord readHeader() throws IOException {
        ByteBuffer headerBuffer = ByteBuffer.allocate(Journal.RECORD_HEADER_SIZE);
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
        if (remaining >= Journal.RECORD_HEADER_SIZE) {
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
}
