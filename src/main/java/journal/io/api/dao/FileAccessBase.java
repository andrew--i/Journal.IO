package journal.io.api.dao;


import journal.io.api.Location;
import journal.io.api.operation.WriteCommand;

import java.io.IOException;
import java.util.Queue;

public interface FileAccessBase {

    long writeHeader() throws IOException;

    void verifyHeader() throws IOException;

    void skip(int size) throws IOException;

    long getCurrentPosition() throws IOException;

    long size() throws IOException;

    boolean skipLocationData(Location location) throws IOException;

    void close() throws IOException;

    void setSize(long size) throws IOException;

    void sync() throws IOException;

    byte[] readLocationData(Location location) throws IOException;

    void seek(long position) throws IOException;

    HeaderRecord readHeader() throws IOException;

    boolean hasRecordHeader(long position, boolean isJournalOpened) throws IOException;

    void write(byte[] data) throws IOException;

    byte[] createDataForWrite(int size, Queue<WriteCommand> writes, boolean checksum);
}
