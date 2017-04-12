package journal.io.api.operation;

import journal.io.api.DataFile;
import journal.io.api.Journal;
import journal.io.api.Location;
import journal.io.api.ReplicationTarget;
import journal.io.api.dao.FileAccessBase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static journal.io.util.LogHelper.warn;

/**
 * Created by Andrew on 12.04.2017.
 */
public class WriteBatch {

    private static byte[] EMPTY_BUFFER = new byte[0];
    //
    private final DataFile dataFile;
    private final Queue<WriteCommand> writes = new ConcurrentLinkedQueue<>();
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile long offset;
    private volatile int pointer;
    private volatile int size;

    WriteBatch(DataFile dataFile, int pointer) throws IOException {
        this.dataFile = dataFile;
        this.offset = dataFile.getLength();
        this.pointer = pointer;
        this.size = Journal.BATCH_CONTROL_RECORD_SIZE;
    }

    boolean canBatch(WriteCommand write, int maxWriteBatchSize, int maxFileLength) throws IOException {
        int thisBatchSize = size + write.getLocation().getSize();
        long thisFileLength = offset + thisBatchSize;
        if (thisBatchSize > maxWriteBatchSize || thisFileLength > maxFileLength) {
            return false;
        } else {
            return true;
        }
    }

    WriteCommand prepareBatch() throws IOException {
        WriteCommand controlRecord = new WriteCommand(new Location(), EMPTY_BUFFER, false);
        Location location = controlRecord.getLocation();
        location.setType(Location.BATCH_CONTROL_RECORD_TYPE);
        location.setSize(Journal.BATCH_CONTROL_RECORD_SIZE);
        location.setDataFileId(dataFile.getDataFileId());
        location.setPointer(pointer);
        size = location.getSize();
        dataFile.incrementLength(size);
        writes.offer(controlRecord);
        return controlRecord;
    }

    void appendBatch(WriteCommand writeRecord) throws IOException {
        size += writeRecord.getLocation().getSize();
        dataFile.incrementLength(writeRecord.getLocation().getSize());
        writes.offer(writeRecord);
    }

    Location perform(FileAccessBase file, boolean checksum, boolean physicalSync, ReplicationTarget replicationTarget) throws IOException {

        WriteCommand control = writes.peek();
        byte[] dataToWrite = file.createDataForWrite(size, writes, checksum);


        // Now do the 1 big write.
        file.seek(offset);
        file.write(dataToWrite);

        // Then sync:
        if (physicalSync) {
            file.sync();
        }

        // And replicate:
        try {
            if (replicationTarget != null) {
                replicationTarget.replicate(control.getLocation(), dataToWrite);
            }
        } catch (Throwable ex) {
            warn("Cannot replicate!", ex);
        }

        control.getLocation().setThisFilePosition(offset);
        return control.getLocation();
    }

    DataFile getDataFile() {
        return dataFile;
    }

    int getSize() {
        return size;
    }

    CountDownLatch getLatch() {
        return latch;
    }

    Collection<WriteCommand> getWrites() {
        return Collections.unmodifiableCollection(writes);
    }

    boolean isEmpty() {
        return writes.isEmpty();
    }

    int incrementAndGetPointer() {
        return ++pointer;
    }
}
