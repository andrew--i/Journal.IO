package journal.io.api.operation;

import journal.io.api.DataFile;
import journal.io.api.Journal;
import journal.io.api.Location;
import journal.io.api.ReplicationTarget;
import journal.io.api.dao.FileAccessBase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

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

    WriteBatch() {
        this.dataFile = null;
        this.offset = -1;
        this.pointer = -1;
    }

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
        controlRecord.getLocation().setType(Location.BATCH_CONTROL_RECORD_TYPE);
        controlRecord.getLocation().setSize(Journal.BATCH_CONTROL_RECORD_SIZE);
        controlRecord.getLocation().setDataFileId(dataFile.getDataFileId());
        controlRecord.getLocation().setPointer(pointer);
        size = controlRecord.getLocation().getSize();
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
        ByteBuffer buffer = ByteBuffer.allocate(size);
        Checksum adler32 = new Adler32();
        WriteCommand control = writes.peek();

        // Write an empty batch control record.
        buffer.putInt(control.getLocation().getPointer());
        buffer.putInt(Journal.BATCH_CONTROL_RECORD_SIZE);
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
        buffer.position(Journal.RECORD_HEADER_SIZE);
        if (checksum) {
            buffer.putLong(adler32.getValue());
        }

        // Now do the 1 big write.
        file.seek(offset);
        file.write(buffer.array());

        // Then sync:
        if (physicalSync) {
            file.sync();
        }

        // And replicate:
        try {
            if (replicationTarget != null) {
                replicationTarget.replicate(control.getLocation(), buffer.array());
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
