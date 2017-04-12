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
package journal.io.api.operation;

import journal.io.api.DataFile;
import journal.io.api.Journal;
import journal.io.api.Location;
import journal.io.api.dao.FileAccessBase;
import journal.io.api.dao.HeaderRecord;
import journal.io.api.exception.ClosedJournalException;
import journal.io.api.exception.CompactedDataFileException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static journal.io.util.LogHelper.warn;

/**
 * File reader/updater to randomly access data files, supporting concurrent
 * thread-isolated reads and writes.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
public class DataFileAccessor {

    private final ConcurrentMap<Thread, ConcurrentMap<Integer, FileAccessBase>> perThreadDataFileRafs = new ConcurrentHashMap<>();
    private final ConcurrentMap<Thread, ConcurrentMap<Integer, Lock>> perThreadDataFileLocks = new ConcurrentHashMap<Thread, ConcurrentMap<Integer, Lock>>();
    private final ReadWriteLock accessorLock = new ReentrantReadWriteLock();
    private final Lock shared = accessorLock.readLock();
    private final Lock exclusive = accessorLock.writeLock();
    //
    private final Journal journal;
    private volatile boolean opened;
    //
    private volatile ScheduledExecutorService disposer;

    public DataFileAccessor(Journal journal) {
        this.journal = journal;
    }

    public void deleteFromLocation(Location source) throws IOException {
        Lock threadLock = getOrCreateLock(Thread.currentThread(), source.getDataFileId());
        shared.lock();
        threadLock.lock();
        try {
            if (opened) {
                if (journal.getInflightWrites().containsKey(source)) {
                    journal.sync();
                }
                //
                FileAccessBase raf = getOrCreateRaf(Thread.currentThread(), source.getDataFileId());
                long position = source.getThisFilePosition();
                if (position != Location.NOT_SET) {
                    DataFile dataFile = journal.getDataFile(source.getDataFileId());
                    raf.setSize(position);
                    dataFile.setLength(position);
                } else {
                    throw new IOException("Cannot find location: " + source);
                }
            } else {
                throw new ClosedJournalException("The journal is closed!");
            }
        } finally {
            threadLock.unlock();
            shared.unlock();
        }
    }

    public byte[] readLocation(Location location, boolean sync) throws IOException {
        if (location.getData() != null && !sync) {
            return location.getData();
        } else {
            Location read = readLocationDetails(location.getDataFileId(), location.getPointer());
            if (read != null && !read.isDeletedRecord()) {
                return read.getData();
            } else {
                throw new IOException("Invalid location: " + location + ", found: " + read);
            }
        }
    }

    public Location readLocationDetails(int file, int pointer) throws IOException {
        WriteCommand asyncWrite = journal.getInflightWrites().get(new Location(file, pointer));
        if (asyncWrite != null) {
            Location location = new Location(file, pointer);
            location.setPointer(asyncWrite.getLocation().getPointer());
            location.setSize(asyncWrite.getLocation().getSize());
            location.setType(asyncWrite.getLocation().getType());
            location.setData(asyncWrite.getData());
            return location;
        } else {
            Location location = new Location(file, pointer);
            Lock threadLock = getOrCreateLock(Thread.currentThread(), location.getDataFileId());
            shared.lock();
            threadLock.lock();
            try {
                if (opened) {
                    FileAccessBase raf = getOrCreateRaf(Thread.currentThread(), location.getDataFileId());
                    if (seekToLocation(raf, location, true, true)) {
                        if (location.getSize() > 0) {
                            location.setDataFileGeneration(journal.getDataFile(file).getDataFileGeneration());
                            location.setNextFilePosition(raf.getCurrentPosition());
                            return location;
                        } else {
                            return null;
                        }
                    } else {
                        return null;
                    }
                } else {
                    throw new ClosedJournalException("The journal is closed!");
                }
            } catch (CompactedDataFileException ex) {
                warn(ex.getMessage());
                return null;
            } finally {
                threadLock.unlock();
                shared.unlock();
            }
        }
    }

    public Location readNextLocationDetails(Location start, final int type) throws ClosedJournalException, IOException {
        // Try with the most immediate subsequent location among inflight writes:
        Location asyncLocation = new Location(start.getDataFileId(), start.getPointer() + 1);
        WriteCommand asyncWrite = journal.getInflightWrites().get(asyncLocation);
        if (asyncWrite != null && asyncWrite.getLocation().isBatchControlRecord() && type != Location.BATCH_CONTROL_RECORD_TYPE) {
            asyncLocation = new Location(start.getDataFileId(), start.getPointer() + 2);
            asyncWrite = journal.getInflightWrites().get(asyncLocation);
        }
        if (asyncWrite != null) {
            asyncLocation.setPointer(asyncWrite.getLocation().getPointer());
            asyncLocation.setSize(asyncWrite.getLocation().getSize());
            asyncLocation.setType(asyncWrite.getLocation().getType());
            asyncLocation.setData(asyncWrite.getData());
            return asyncLocation;
        } else if (!journal.getInflightWrites().containsKey(start)) {
            // Else read from file:
            Lock threadLock = getOrCreateLock(Thread.currentThread(), start.getDataFileId());
            shared.lock();
            threadLock.lock();
            try {
                if (opened) {
                    FileAccessBase raf = getOrCreateRaf(Thread.currentThread(), start.getDataFileId());
                    if (isIntoNextLocation(raf, start) || seekToLocation(raf, start, true, false)) {
                        if (raf.hasRecordHeader(raf.getCurrentPosition(), journal.isOpened())) {
                            Location next = new Location(start.getDataFileId());
                            do {
                                // Set this file pointer:
                                next.setThisFilePosition(raf.getCurrentPosition());
                                // Advance and read header:
                                HeaderRecord headerRecord = raf.readHeader();
                                next.setPointer(headerRecord.getPointer());
                                next.setSize(headerRecord.getSize());
                                next.setType(headerRecord.getType());
                                if (type != Location.ANY_RECORD_TYPE && next.getType() != type) {
                                    boolean skipped = raf.skipLocationData(next);
                                    assert skipped;
                                } else {
                                    break;
                                }
                            } while (raf.hasRecordHeader(raf.getCurrentPosition(), journal.isOpened()));
                            if (type == Location.ANY_RECORD_TYPE || next.getType() == type) {
                                next.setData(raf.readLocationData(next));
                                next.setDataFileGeneration(journal.getDataFile(start.getDataFileId()).getDataFileGeneration());
                                next.setNextFilePosition(raf.getCurrentPosition());
                                return next;
                            } else {
                                raf.seek(0);
                                return null;
                            }
                        } else {
                            return null;
                        }
                    } else {
                        return null;
                    }
                } else {
                    throw new ClosedJournalException("The journal is closed!");
                }
            } catch (CompactedDataFileException ex) {
                warn(ex.getMessage());
                return null;
            } finally {
                threadLock.unlock();
                shared.unlock();
            }
        } else {
            return null;
        }
    }

    public void open() {
        disposer = journal.getDisposer();
        disposer.scheduleAtFixedRate(new ResourceDisposer(), journal.getDisposeInterval(), journal.getDisposeInterval(), TimeUnit.MILLISECONDS);
        opened = true;
    }

    public void close() {
        exclusive.lock();
        try {
            opened = false;
            for (Entry<Thread, ConcurrentMap<Integer, FileAccessBase>> threadRafs : perThreadDataFileRafs.entrySet()) {
                for (Entry<Integer, FileAccessBase> raf : threadRafs.getValue().entrySet()) {
                    disposeByThread(threadRafs.getKey(), raf.getKey());
                }
            }
        } finally {
            exclusive.unlock();
        }
    }

    private void disposeByThread(Thread t, Integer dataFileId) {
        Lock lock = getOrCreateLock(t, dataFileId);
        lock.lock();
        try {
            removeRaf(t, dataFileId);
        } catch (IOException ex) {
            warn(ex, ex.getMessage());
        } finally {
            lock.unlock();
        }
    }

    private boolean isIntoNextLocation(FileAccessBase raf, Location source) throws CompactedDataFileException, IOException {
        int generation = journal.getDataFile(source.getDataFileId()).getDataFileGeneration();
        long position = raf.getCurrentPosition();
        return source.getDataFileGeneration() == generation
                && source.getNextFilePosition() == position;
    }

    private boolean seekToLocation(FileAccessBase raf, Location destination, boolean fillLocation, boolean fillData) throws IOException {
        // First try the next file position:
        long position = raf.getCurrentPosition();
        int pointer = -1;
        int size = -1;
        byte type = -1;
        if (raf.hasRecordHeader(position, journal.isOpened())) {
            HeaderRecord headerRecord = raf.readHeader();
            pointer = headerRecord.getPointer();
            size = headerRecord.getSize();
            type = headerRecord.getType();
        }
        // If pointer is wrong, seek by trying the saved file position:
        if (pointer != destination.getPointer()) {
            position = destination.getThisFilePosition();
            if (position != Location.NOT_SET && raf.hasRecordHeader(position, journal.isOpened())) {
                raf.seek(position);
                HeaderRecord headerRecord = raf.readHeader();
                pointer = headerRecord.getPointer();
                size = headerRecord.getSize();
                type = headerRecord.getType();
            }
            // Else seek by using hints:
            if (pointer != destination.getPointer()) {
                // TODO: what if destination is a batch?
                // We have to first check for equality and then lower entry!
                Entry<Location, Long> hint = journal.getHints().lowerEntry(destination);
                if (hint != null && hint.getKey().getDataFileId() == destination.getDataFileId()) {
                    position = hint.getValue();
                } else {
                    position = Journal.FILE_HEADER_SIZE;
                }
                raf.seek(position);
                if (raf.hasRecordHeader(position, journal.isOpened())) {
                    HeaderRecord headerRecord = raf.readHeader();
                    pointer = headerRecord.getPointer();
                    size = headerRecord.getSize();
                    type = headerRecord.getType();
                    while (pointer != destination.getPointer()) {
                        raf.skip(size - Journal.RECORD_HEADER_SIZE);
                        position = raf.getCurrentPosition();
                        if (raf.hasRecordHeader(position, journal.isOpened())) {
                            HeaderRecord headerRecord2 = raf.readHeader();
                            pointer = headerRecord2.getPointer();
                            size = headerRecord2.getSize();
                            type = headerRecord2.getType();
                        } else {
                            return false;
                        }
                    }
                } else {
                    return false;
                }
            }
        }
        // Filling data implies filling location metadata too:
        if (fillLocation || fillData) {
            // Fill location metadata:
            destination.setThisFilePosition(position);
            destination.setSize(size);
            destination.setType(type);
            // Maybe fill data too:
            if (fillData) {
                destination.setData(raf.readLocationData(destination));
                // Do not seek back to this location position so next read
                // can be sequential.
            } else {
                // Otherwise skip location data, so again next read can be
                // sequential
                boolean skipped = raf.skipLocationData(destination);
                assert skipped;
            }
        } else {
            // Otherwise get back to the header position, as the caller may be interested
            // in reading:
            raf.seek(position);
        }
        return true;
    }


    private FileAccessBase getOrCreateRaf(Thread thread, Integer file) throws IOException {
        ConcurrentMap<Integer, FileAccessBase> rafs = perThreadDataFileRafs.get(thread);
        if (rafs == null) {
            rafs = new ConcurrentHashMap<>();
            perThreadDataFileRafs.put(thread, rafs);
        }
        FileAccessBase raf = rafs.get(file);
        if (raf == null) {
            raf = journal.getDataFile(file).createDataAccess();
            raf.skip(journal.FILE_HEADER_SIZE);
            rafs.put(file, raf);
        }
        return raf;
    }

    private void removeRaf(Thread thread, Integer file) throws IOException {
        FileAccessBase fileAccessBase = perThreadDataFileRafs.get(thread).remove(file);
        fileAccessBase.close();
    }

    private Lock getOrCreateLock(Thread thread, Integer file) {
        ConcurrentMap<Integer, Lock> locks = perThreadDataFileLocks.get(thread);
        if (locks == null) {
            locks = new ConcurrentHashMap<Integer, Lock>();
            perThreadDataFileLocks.put(thread, locks);
        }
        Lock lock = locks.get(file);
        if (lock == null) {
            lock = new ReentrantLock();
            locks.put(file, lock);
        }
        return lock;
    }

    private class ResourceDisposer implements Runnable {

        public void run() {
            Set<Thread> deadThreads = new HashSet<Thread>();
            for (Entry<Thread, ConcurrentMap<Integer, FileAccessBase>> threadRafs : perThreadDataFileRafs.entrySet()) {
                for (Entry<Integer, FileAccessBase> raf : threadRafs.getValue().entrySet()) {
                    Lock lock = getOrCreateLock(threadRafs.getKey(), raf.getKey());
                    if (lock.tryLock()) {
                        try {
                            removeRaf(threadRafs.getKey(), raf.getKey());
                            if (!threadRafs.getKey().isAlive()) {
                                deadThreads.add(threadRafs.getKey());
                            }
                        } catch (IOException ex) {
                            warn(ex, ex.getMessage());
                        } finally {
                            lock.unlock();
                        }
                    }
                }
            }
            for (Thread deadThread : deadThreads) {
                perThreadDataFileRafs.remove(deadThread);
                perThreadDataFileLocks.remove(deadThread);
            }
        }
    }
}
