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

import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ReadWriteTest extends AbstractJournalTest {


	@Test
	public void testSyncLogWritingAndRedoing() throws Exception {
		int iterations = 10;
		for (int i = 0; i < iterations; i++) {
			journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
		}
		int i = 0;
		for (Location location : journal.redo()) {
			byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
			assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
		}
	}

	@Test
	public void testSyncLogWritingAndUndoing() throws Exception {
		int iterations = 10;
		for (int i = 0; i < iterations; i++) {
			journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
		}
		int i = 10;
		for (Location location : journal.undo()) {
			byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
			assertEquals("DATA" + --i, new String(buffer, "UTF-8"));
		}
	}

	@Test
	public void testAsyncLogWritingAndRedoing() throws Exception {
		int iterations = 10;
		for (int i = 0; i < iterations; i++) {
			journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.ASYNC);
		}
		int i = 0;
		for (Location location : journal.redo()) {
			byte[] buffer = journal.read(location, Journal.ReadType.SYNC);
			assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
		}
	}

	@Test
	public void testAsyncLogWritingAndUndoing() throws Exception {
		int iterations = 10;
		for (int i = 0; i < iterations; i++) {
			journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.ASYNC);
		}
		int i = 10;
		for (Location location : journal.undo()) {
			byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
			assertEquals("DATA" + --i, new String(buffer, "UTF-8"));
		}
	}

	@Test
	public void testMixedSyncAsyncLogWritingAndRedoing() throws Exception {
		int iterations = 10;
		for (int i = 0; i < iterations; i++) {
			Journal.WriteType sync = i % 2 == 0 ? Journal.WriteType.SYNC : Journal.WriteType.ASYNC;
			journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
		}
		int i = 0;
		for (Location location : journal.redo()) {
			byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
			assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
		}
	}

	@Test
	public void testMixedSyncAsyncLogWritingAndUndoing() throws Exception {
		int iterations = 10;
		for (int i = 0; i < iterations; i++) {
			Journal.WriteType sync = i % 2 == 0 ? Journal.WriteType.SYNC : Journal.WriteType.ASYNC;
			journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
		}
		int i = 10;
		for (Location location : journal.undo()) {
			byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
			assertEquals("DATA" + --i, new String(buffer, "UTF-8"));
		}
	}

	@Test
	public void testWriteCallbackOnSync() throws Exception {
		final int iterations = 10;
		final CountDownLatch writeLatch = new CountDownLatch(iterations);
		final Map<Location, Throwable> errors = new ConcurrentHashMap<Location, Throwable>();
		WriteCallback callback = new WriteCallback() {
			@Override
			public void onSync(Location syncedLocation) {
				writeLatch.countDown();
			}

			@Override
			public void onError(Location location, Throwable error) {
				errors.put(location, error);
			}
		};
		for (int i = 0; i < iterations; i++) {
			journal.write(new byte[]{(byte) i}, Journal.WriteType.ASYNC, callback);
		}
		journal.sync();
		assertTrue(writeLatch.await(5, TimeUnit.SECONDS));
		assertTrue("Caught errors: " + errors, errors.isEmpty());
	}

	@Test
	public void testSyncAndCallReplicator() throws Exception {
		final int iterations = 3;
		final CountDownLatch writeLatch = new CountDownLatch(1);
		ReplicationTarget replicator = (startLocation, data) -> {
            if (startLocation.getDataFileId() == 1 && startLocation.getPointer() == 0) {
                writeLatch.countDown();
            }
        };
		journal.setReplicationTarget(replicator);
		for (int i = 0; i < iterations; i++) {
			journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.ASYNC);
		}
		journal.sync();
		assertTrue(writeLatch.await(5, TimeUnit.SECONDS));
	}

	@Test
	public void testBatchWriteCompletesAfterClose() throws Exception {
		byte[] data = "DATA".getBytes();
		final int iterations = 10;
		for (int i = 0; i < iterations; i++) {
			journal.write(data, Journal.WriteType.ASYNC);
		}
		journal.close();
		assertTrue(journal.getInflightWrites().isEmpty());
	}

	@Test
	public void testNoBatchWriteWithSync() throws Exception {
		byte[] data = "DATA".getBytes();
		final int iterations = 10;
		for (int i = 0; i < iterations; i++) {
			journal.write(data, Journal.WriteType.SYNC);
			assertTrue(journal.getInflightWrites().isEmpty());
		}
	}

	@Override
	protected boolean configure(Journal journal) {
		journal.setMaxFileLength(1024 * 100);
		journal.setMaxWriteBatchSize(1024);
		return true;
	}
}