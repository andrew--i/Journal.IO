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
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Sergio Bossa
 */
public class ConcurrencyTest extends AbstractJournalTest {

	@Test
	public void testConcurrentWriteAndRead() throws Exception {
		final AtomicInteger counter = new AtomicInteger(0);
		ExecutorService executor = Executors.newFixedThreadPool(10);
		int iterations = 1000;
		//
		for (int i = 0; i < iterations; i++) {
			final int index = i;
			executor.submit(new Runnable() {
				public void run() {
					try {
						Journal.WriteType sync = index % 2 == 0 ? Journal.WriteType.SYNC : Journal.WriteType.ASYNC;
						String write = new String("DATA" + index);
						Location location = journal.write(write.getBytes("UTF-8"), sync);
						String read = new String(journal.read(location, Journal.ReadType.ASYNC), "UTF-8");
						if (read.equals("DATA" + index)) {
							counter.incrementAndGet();
						} else {
							System.out.println(write);
							System.out.println(read);
						}
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
			});
		}
		executor.shutdown();
		assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
		assertEquals(iterations, counter.get());
	}


	@Override
	protected boolean configure(Journal journal) {
		journal.setMaxFileLength(1024);
		journal.setMaxWriteBatchSize(1024);
		return true;
	}
}
