/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package journal.io.api;

import journal.io.api.exception.ClosedJournalException;
import journal.io.api.exception.CompactedDataFileException;
import journal.io.api.operation.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import static journal.io.util.LogHelper.warn;

/**
 * Journal implementation based on append-only rotating logs and checksummed
 * records, with concurrent writes and reads, dynamic batching and logs
 * compaction. <br/><br/> Journal records can be written, read and deleted by
 * providing a {@link Location} object. <br/><br/> The whole Journal can be
 * replayed forward or backward by simply obtaining a redo or undo iterable and
 * going through it in a for-each block. <br/>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
public class Journal {
    //
    static final String WRITER_THREAD_GROUP = "Journal.IO - Writer Thread Group";
    static final String WRITER_THREAD = "Journal.IO - Writer Thread";
    static final String DISPOSER_THREAD_GROUP = "Journal.IO - Disposer Thread Group";
    static final String DISPOSER_THREAD = "Journal.IO - Disposer Thread";
    //
    static final int PRE_START_POINTER = -1;
    //
    static final String DEFAULT_DIRECTORY = ".";
    static final String DEFAULT_FILE_PREFIX = "db-";
    static final String DEFAULT_FILE_SUFFIX = ".log";
    static final int DEFAULT_MAX_FILE_LENGTH = 1024 * 1024 * 32;
    static final int DEFAULT_DISPOSE_INTERVAL = 1000 * 60 * 10;
    static final int MIN_FILE_LENGTH = 1024;
    static final int DEFAULT_MAX_BATCH_SIZE = DEFAULT_MAX_FILE_LENGTH;
    //
    private final ConcurrentNavigableMap<Integer, DataFile> dataFiles = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<Location, Long> hints = new ConcurrentSkipListMap<>();
    private final ConcurrentMap<Location, WriteCommand> inflightWrites = new ConcurrentHashMap<>();
    private final AtomicLong totalLength = new AtomicLong();
    //
    private volatile Location lastAppendLocation;
    //
    private volatile File directory = new File(DEFAULT_DIRECTORY);
    //
    private volatile String filePrefix = DEFAULT_FILE_PREFIX;
    private volatile String fileSuffix = DEFAULT_FILE_SUFFIX;
    private volatile int maxWriteBatchSize = DEFAULT_MAX_BATCH_SIZE;
    private volatile int maxFileLength = DEFAULT_MAX_FILE_LENGTH;
    private volatile long disposeInterval = DEFAULT_DISPOSE_INTERVAL;
    private volatile boolean physicalSync = false;
    private volatile boolean checksum = true;
    //
    private volatile boolean managedWriter;
    private volatile boolean managedDisposer;
    private volatile Executor writer;
    private volatile ScheduledExecutorService disposer;
    private volatile DataFileAppender appender;
    private volatile DataFileAccessor accessor;
    //
    private volatile boolean opened;
    //
    private RecoveryErrorHandler recoveryErrorHandler;
    //
    private ReplicationTarget replicationTarget;

    /**
     * Open the journal, eventually recovering it if already existent.
     *
     * @throws IOException
     */
    public synchronized void open() throws IOException {
        if (opened) {
            return;
        }

        if (maxFileLength < MIN_FILE_LENGTH) {
            throw new IllegalStateException("Max file length must be equal or greater than: " + MIN_FILE_LENGTH);
        }
        if (maxWriteBatchSize > maxFileLength) {
            throw new IllegalStateException("Max batch size must be equal or less than: " + maxFileLength);
        }
        if (writer == null) {
            managedWriter = true;
            writer = Executors.newSingleThreadExecutor(new JournalThreadFactory(WRITER_THREAD_GROUP, WRITER_THREAD));
        }
        if (disposer == null) {
            managedDisposer = true;
            disposer = Executors.newSingleThreadScheduledExecutor(new JournalThreadFactory(DISPOSER_THREAD_GROUP, DISPOSER_THREAD));
        }

        if (recoveryErrorHandler == null) {
            recoveryErrorHandler = RecoveryErrorHandler.ABORT;
        }

        accessor = new DataFileAccessor(this);
        accessor.open();

        appender = new DataFileAppender(this);
        appender.open();

        dataFiles.clear();

        File[] files = directory.listFiles((dir, n) -> dir.equals(directory) && n.startsWith(filePrefix) && n.endsWith(fileSuffix));
        if (files == null) {
            throw new IOException("Failed to access content of " + directory);
        }

        Arrays.sort(files, (f1, f2) -> {
            String name1 = f1.getName();
            int index1 = Integer.parseInt(name1.substring(filePrefix.length(), name1.length() - fileSuffix.length()));
            String name2 = f2.getName();
            int index2 = Integer.parseInt(name2.substring(filePrefix.length(), name2.length() - fileSuffix.length()));
            return index1 - index2;
        });
        if (files.length > 0) {
            for (File file : files) {
                try {
                    String name = file.getName();
                    int index = Integer.parseInt(name.substring(filePrefix.length(), name.length() - fileSuffix.length()));
                    DataFile dataFile = new DataFile(file, index);
                    if (!dataFiles.isEmpty()) {
                        dataFiles.lastEntry().getValue().setNext(dataFile);
                    }
                    dataFiles.put(dataFile.getDataFileId(), dataFile);
                    totalLength.addAndGet(dataFile.getLength());
                } catch (NumberFormatException e) {
                    // Ignore file that do not match the pattern.
                }
            }
            lastAppendLocation = recoveryCheck();
        }
        if (lastAppendLocation == null) {
            lastAppendLocation = new Location(1, PRE_START_POINTER);
        }

        opened = true;
    }

    /**
     * Close the journal.
     *
     * @throws IOException
     */
    public synchronized void close() throws IOException {
        if (!opened) {
            return;
        }
        //
        opened = false;
        accessor.close();
        appender.close();
        hints.clear();
        inflightWrites.clear();
        //
        if (managedWriter) {
            ((ExecutorService) writer).shutdown();
            writer = null;
        }
        if (managedDisposer) {
            disposer.shutdown();
            disposer = null;
        }
    }

    /**
     * Sync asynchronously written records on disk.
     *
     * @throws IOException
     * @throws ClosedJournalException
     */
    public void sync() throws IOException {
        try {
            appender.sync().get();
            if (appender.getAsyncException() != null) {
                throw new IOException(appender.getAsyncException());
            }
        } catch (Exception ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    /**
     * Read the record stored at the given {@link Location}, either by syncing
     * with the disk state (if {@code ReadType.SYNC}) or by taking advantage of
     * speculative disk reads (if {@code ReadType.ASYNC}); the latter is faster,
     * while the former is slower but will suddenly detect deleted records.
     *
     * @param location
     * @param read
     * @return
     * @throws IOException
     * @throws ClosedJournalException
     * @throws CompactedDataFileException
     */
    public byte[] read(Location location, ReadType read) throws IOException {
        return accessor.readLocation(location, read.equals(ReadType.SYNC));
    }

    /**
     * Write the given byte buffer record, either sync (if
     * {@code WriteType.SYNC}) or async (if {@code WriteType.ASYNC}), and
     * returns the stored {@link Location}.<br/> A sync write causes all
     * previously batched async writes to be synced too.
     *
     * @param data
     * @param write
     * @return
     * @throws IOException
     * @throws ClosedJournalException
     */
    public Location write(byte[] data, WriteType write) throws IOException {
        return write(data, write, Location.NoWriteCallback.INSTANCE);
    }

    /**
     * Write the given byte buffer record, either sync (if
     * {@code WriteType.SYNC}) or async (if {@code WriteType.ASYNC}), and
     * returns the stored {@link Location}.<br/> A sync write causes all
     * previously batched async writes to be synced too.<br/> The provided
     * callback will be invoked if sync is completed or if some error occurs
     * during syncing.
     *
     * @param data
     * @param write
     * @param callback
     * @return
     * @throws IOException
     * @throws ClosedJournalException
     */
    public Location write(byte[] data, WriteType write, WriteCallback callback) throws IOException {
        return appender.storeItem(data, Location.USER_RECORD_TYPE, write.equals(WriteType.SYNC), callback);
    }


    /**
     * Return an iterable to replay the journal by going through all records
     * locations.
     *
     * @return
     * @throws IOException
     * @throws ClosedJournalException
     * @throws CompactedDataFileException
     */
    public Iterable<Location> redo() throws IOException {
        Entry<Integer, DataFile> firstEntry = dataFiles.firstEntry();
        if (firstEntry == null) {
            return new Redo(this, null);
        }
        return new Redo(this, goToFirstLocation(firstEntry.getValue(), Location.USER_RECORD_TYPE, true));
    }

    /**
     * Return an iterable to replay the journal by going through all records
     * locations starting from the given one.
     *
     * @param start
     * @return
     * @throws IOException
     * @throws CompactedDataFileException
     * @throws ClosedJournalException
     */
    public Iterable<Location> redo(Location start) throws IOException {
        return new Redo(this, start);
    }

    /**
     * Return an iterable to replay the journal in reverse, starting with the
     * newest location and ending with the first. The iterable does not include
     * future writes - writes that happen after its creation.
     *
     * @return
     * @throws IOException
     * @throws CompactedDataFileException
     * @throws ClosedJournalException
     */
    public Iterable<Location> undo() throws IOException {
        return new Undo(redo());
    }

    /**
     * Return an iterable to replay the journal in reverse, starting with the
     * newest location and ending with the specified end location. The iterable
     * does not include future writes - writes that happen after its creation.
     *
     * @param end
     * @return
     * @throws IOException
     * @throws CompactedDataFileException
     * @throws ClosedJournalException
     */
    public Iterable<Location> undo(Location end) throws IOException {
        return new Undo(redo(end));
    }

    /**
     * Get the max length of each log file.
     *
     * @return
     */
    public int getMaxFileLength() {
        return maxFileLength;
    }

    /**
     * Set the max length of each log file.
     */
    public void setMaxFileLength(int maxFileLength) {
        this.maxFileLength = maxFileLength;
    }

    /**
     * Set the journal directory containing log files.
     */
    public void setDirectory(File directory) {
        this.directory = directory;
    }

    /**
     * Set the prefix for log files.
     *
     * @param filePrefix
     */
    public void setFilePrefix(String filePrefix) {
        this.filePrefix = filePrefix;
    }

    /**
     * Get the {@link ReplicationTarget} to replicate batch writes to.
     *
     * @return
     */
    public ReplicationTarget getReplicationTarget() {
        return replicationTarget;
    }

    /**
     * Set the {@link ReplicationTarget} to replicate batch writes to.
     *
     * @param replicationTarget
     */
    public void setReplicationTarget(ReplicationTarget replicationTarget) {
        this.replicationTarget = replicationTarget;
    }

    /**
     * Set the suffix for log files.
     *
     * @param fileSuffix
     */
    public void setFileSuffix(String fileSuffix) {
        this.fileSuffix = fileSuffix;
    }

    /**
     * Return true if records checksum is enabled, false otherwise.
     *
     * @return
     */
    public boolean isChecksum() {
        return checksum;
    }

    /**
     * Set true if records checksum is enabled, false otherwise.
     *
     * @param checksumWrites
     */
    public void setChecksum(boolean checksumWrites) {
        this.checksum = checksumWrites;
    }

    /**
     * Return true if every disk write is followed by a physical disk sync,
     * synchronizing file descriptor properties and flushing hardware buffers,
     * false otherwise.
     *
     * @return
     */
    public boolean isPhysicalSync() {
        return physicalSync;
    }

    /**
     * Set true if every disk write must be followed by a physical disk sync,
     * synchronizing file descriptor properties and flushing hardware buffers,
     * false otherwise.
     *
     * @return
     */
    public void setPhysicalSync(boolean physicalSync) {
        this.physicalSync = physicalSync;
    }

    /**
     * Get the max size in bytes of the write batch: must always be equal or
     * less than the max file length.
     *
     * @return
     */
    public int getMaxWriteBatchSize() {
        return maxWriteBatchSize;
    }

    /**
     * Set the max size in bytes of the write batch: must always be equal or
     * less than the max file length.
     *
     * @param maxWriteBatchSize
     */
    public void setMaxWriteBatchSize(int maxWriteBatchSize) {
        this.maxWriteBatchSize = maxWriteBatchSize;
    }

    /**
     * Get the milliseconds interval for resources disposal.
     *
     * @return
     */
    public long getDisposeInterval() {
        return disposeInterval;
    }

    /**
     * Set the milliseconds interval for resources disposal: i.e., un-accessed
     * files will be closed.
     *
     * @param disposeInterval
     */
    public void setDisposeInterval(long disposeInterval) {
        this.disposeInterval = disposeInterval;
    }

    /**
     * Set the RecoveryErrorHandler to invoke in case of checksum errors.
     *
     * @param recoveryErrorHandler
     */
    public void setRecoveryErrorHandler(RecoveryErrorHandler recoveryErrorHandler) {
        this.recoveryErrorHandler = recoveryErrorHandler;
    }

    public String toString() {
        return directory.toString();
    }

    public boolean isOpened() {
        return opened;
    }

    public Executor getWriter() {
        return writer;
    }

    /**
     * Set the Executor to use for writing new record entries.
     * <p>
     * Important note: the provided Executor must be manually closed.
     *
     * @param writer
     */
    public void setWriter(Executor writer) {
        this.writer = writer;
        this.managedWriter = false;
    }

    public ScheduledExecutorService getDisposer() {
        return disposer;
    }

    /**
     * Set the ScheduledExecutorService to use for internal resources disposing.
     * <p>
     * Important note: the provided ScheduledExecutorService must be manually
     * closed.
     *
     * @param disposer
     */
    public void setDisposer(ScheduledExecutorService disposer) {
        this.disposer = disposer;
        this.managedDisposer = false;
    }

    public DataFile getDataFile(Integer id) throws CompactedDataFileException {
        DataFile result = dataFiles.get(id);
        if (result != null) {
            return result;
        } else {
            throw new CompactedDataFileException(id);
        }
    }

    public ConcurrentNavigableMap<Location, Long> getHints() {
        return hints;
    }

    public ConcurrentMap<Location, WriteCommand> getInflightWrites() {
        return inflightWrites;
    }

    public DataFile getCurrentWriteDataFile() throws IOException {
        if (dataFiles.isEmpty()) {
            newDataFile();
        }
        return dataFiles.lastEntry().getValue();
    }

    public DataFile newDataFile() throws IOException {
        int nextNum = !dataFiles.isEmpty() ? dataFiles.lastEntry().getValue().getDataFileId() + 1 : 1;
        File file = getFile(nextNum);
        DataFile nextWriteFile = new DataFile(file, nextNum);
        nextWriteFile.writeHeader();
        if (!dataFiles.isEmpty()) {
            dataFiles.lastEntry().getValue().setNext(nextWriteFile);
        }
        dataFiles.put(nextWriteFile.getDataFileId(), nextWriteFile);
        return nextWriteFile;
    }

    public Location getLastAppendLocation() {
        return lastAppendLocation;
    }

    public void setLastAppendLocation(Location location) {
        this.lastAppendLocation = location;
    }

    public void addToTotalLength(int size) {
        totalLength.addAndGet(size);
    }

    private Location goToFirstLocation(DataFile file, final byte type, final boolean goToNextFile) throws IOException, IllegalStateException {
        Location start = null;
        while (file != null && start == null) {
            start = accessor.readLocationDetails(file.getDataFileId(), 0);
            file = goToNextFile ? file.getNext() : null;
        }
        if (start != null && (start.getType() == type || type == Location.ANY_RECORD_TYPE)) {
            return start;
        } else if (start != null) {
            return goToNextLocation(start, type, goToNextFile);
        } else {
            return null;
        }
    }

    public Location goToNextLocation(Location start, final byte type, final boolean goToNextFile) throws IOException {
        Integer currentDataFileId = start.getDataFileId();
        Location currentLocation = new Location(start);
        Location result = null;
        while (result == null) {
            if (currentLocation != null) {
                currentLocation = accessor.readNextLocationDetails(currentLocation, type);
                result = currentLocation;
            } else {
                if (goToNextFile) {
                    Entry<Integer, DataFile> candidateDataFile = dataFiles.higherEntry(currentDataFileId);
                    currentDataFileId = candidateDataFile != null ? candidateDataFile.getValue().getDataFileId() : null;
                    if (currentDataFileId != null) {
                        currentLocation = accessor.readLocationDetails(currentDataFileId, 0);
                        if (currentLocation != null && (currentLocation.getType() == type || type == Location.ANY_RECORD_TYPE)) {
                            result = currentLocation;
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
        return result;
    }

    private File getFile(int nextNum) {
        String fileName = filePrefix + nextNum + fileSuffix;
        File file = new File(directory, fileName);
        return file;
    }


    private Location recoveryCheck() throws IOException {
        List<Location> checksummedLocations = new LinkedList<Location>();
        DataFile currentFile = dataFiles.firstEntry().getValue();
        Location currentBatch = null;
        Location lastBatch = null;
        while (currentFile != null) {
            try {
                currentFile.verifyHeader();
            } catch (IOException ex) {
                DataFile toDelete = currentFile;
                currentFile = toDelete.getNext();
                warn(ex, "Data file fails header verification: %s", toDelete);
                continue;
            }
            try {
                currentBatch = goToFirstLocation(currentFile, Location.BATCH_CONTROL_RECORD_TYPE, false);
                while (currentBatch != null) {
                    try {
                        Location currentLocation = currentBatch;
                        hints.put(currentBatch, currentBatch.getThisFilePosition());
                        if (isChecksum()) {
                            ByteBuffer currentBatchBuffer = ByteBuffer.wrap(accessor.readLocation(currentBatch, false));
                            Checksum actualChecksum = new Adler32();
                            Location nextLocation = goToNextLocation(currentBatch, Location.ANY_RECORD_TYPE, false);
                            long expectedChecksum = currentBatchBuffer.getLong();
                            checksummedLocations.clear();
                            while (nextLocation != null && nextLocation.getType() != Location.BATCH_CONTROL_RECORD_TYPE) {
                                assert currentLocation.compareTo(nextLocation) < 0;
                                byte data[] = accessor.readLocation(nextLocation, false);
                                actualChecksum.update(data, 0, data.length);
                                checksummedLocations.add(nextLocation);
                                currentLocation = nextLocation;
                                nextLocation = goToNextLocation(nextLocation, Location.ANY_RECORD_TYPE, false);
                            }
                            if (checksummedLocations.isEmpty()) {
                                throw new IllegalStateException("Found empty batch!");
                            }
                            if (expectedChecksum != actualChecksum.getValue()) {
                                recoveryErrorHandler.onError(this, checksummedLocations);
                            }
                            if (nextLocation != null) {
                                assert currentLocation.compareTo(nextLocation) < 0;
                            }
                            lastBatch = currentBatch;
                            currentBatch = nextLocation;
                        } else {
                            lastBatch = currentBatch;
                            currentBatch = goToNextLocation(currentBatch, Location.BATCH_CONTROL_RECORD_TYPE, false);
                        }
                    } catch (Throwable ex) {
                        warn(ex, "Corrupted data found, deleting data starting from location %s up to the end of the file.", currentBatch);
                        accessor.deleteFromLocation(currentBatch);
                        break;
                    }
                }
            } catch (Throwable ex) {
                warn(ex, "Corrupted data found while reading first batch location, deleting whole data file: %s", currentFile);
            }
            currentFile = currentFile.getNext();
        }
        // Go through records on the last batch to get the last one:
        Location lastRecord = lastBatch;
        while (lastRecord != null) {
            Location next = goToNextLocation(lastRecord, Location.ANY_RECORD_TYPE, false);
            if (next != null) {
                lastRecord = next;
            } else {
                break;
            }
        }
        return lastRecord;
    }

    public enum ReadType {

        SYNC, ASYNC
    }

    public enum WriteType {

        SYNC, ASYNC
    }

    private static class JournalThreadFactory implements ThreadFactory {

        private final String groupName;
        private final String threadName;

        public JournalThreadFactory(String groupName, String threadName) {
            this.groupName = groupName;
            this.threadName = threadName;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(new ThreadGroup(groupName), r, threadName);
        }
    }

}
