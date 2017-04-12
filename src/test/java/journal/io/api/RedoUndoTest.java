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

import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class RedoUndoTest extends AbstractJournalTest {

	@Test
	public void testRedoForwardOrder() throws Exception {
		journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		journal.write("B".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		journal.write("C".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		Iterator<Location> redo = journal.redo().iterator();
		assertTrue(redo.hasNext());
		assertEquals("A", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
		assertTrue(redo.hasNext());
		assertEquals("B", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
		assertTrue(redo.hasNext());
		assertEquals("C", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
		assertFalse(redo.hasNext());
	}

	@Test
	public void testRedoForwardOrderWithStartingLocation() throws Exception {
		Location a = journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		Location b = journal.write("B".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		Location c = journal.write("C".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		Iterator<Location> redo = journal.redo(b).iterator();
		assertTrue(redo.hasNext());
		assertEquals("B", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
		assertTrue(redo.hasNext());
		assertEquals("C", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
		assertFalse(redo.hasNext());
	}

	@Test
	public void testRedoEmptyJournal() throws Exception {
		int iterations = 0;
		for (Location loc : journal.redo()) {
			iterations++;
		}
		assertEquals(0, iterations);
	}



	@Test
	public void testRedoTakesNewWritesIntoAccount() throws Exception {
		journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		journal.write("B".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		Iterator<Location> redo = journal.redo().iterator();
		journal.write("C".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		assertEquals("A", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
		assertEquals("B", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
		assertEquals("C", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
	}



	@Test(expected = NoSuchElementException.class)
	public void testNoSuchElementExceptionWithRedoIterator() throws Exception {
		journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		Iterator<Location> itr = journal.redo().iterator();
		assertTrue(itr.hasNext());
		itr.next();
		assertFalse(itr.hasNext());
		itr.next();
	}

	@Test
	public void testUndoBackwardOrderWithEndingLocation() throws Exception {
		Location a = journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		Location b = journal.write("B".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		Location c = journal.write("C".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		Iterator<Location> undo = journal.undo(b).iterator();
		assertTrue(undo.hasNext());
		assertEquals("C", new String(journal.read(undo.next(), Journal.ReadType.ASYNC), "UTF-8"));
		assertTrue(undo.hasNext());
		assertEquals("B", new String(journal.read(undo.next(), Journal.ReadType.ASYNC), "UTF-8"));
		assertFalse(undo.hasNext());
	}

	@Test
	public void testUndoEmptyJournal() throws Exception {
		int iterations = 0;
		for (Location loc : journal.undo()) {
			iterations++;
		}
		assertEquals(0, iterations);
	}

	@Test
	public void testUndoLargeChunksOfData() throws Exception {
		byte parts = 127;
		for (byte i = 0; i < parts; i++) {
			journal.write(new byte[]{i}, Journal.WriteType.ASYNC);
		}
		parts = 127;
		for (Location loc : journal.undo()) {
			assertArrayEquals(new byte[]{--parts}, journal.read(loc, Journal.ReadType.ASYNC));
		}
		assertEquals(0, parts);
	}

	@Test
	public void testUndoDoesntTakeNewWritesIntoAccount() throws Exception {
		journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		journal.write("B".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		Iterator<Location> undo = journal.undo().iterator();
		journal.write("C".getBytes("UTF-8"), Journal.WriteType.ASYNC);
		assertEquals("B", new String(journal.read(undo.next(), Journal.ReadType.ASYNC), "UTF-8"));
		assertEquals("A", new String(journal.read(undo.next(), Journal.ReadType.ASYNC), "UTF-8"));
		assertFalse(undo.hasNext());
	}


	@Override
	protected boolean configure(Journal journal) {
		journal.setMaxFileLength(1024 * 100);
		journal.setMaxWriteBatchSize(1024);
		return true;
	}
}
