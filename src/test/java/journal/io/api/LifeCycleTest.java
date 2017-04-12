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
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.fail;

public class LifeCycleTest extends AbstractJournalTest {

	@Test(expected = ClosedJournalException.class)
	public void testWritingFailsAfterClose() throws Exception {
		journal.close();
		journal.write("data".getBytes("UTF-8"), Journal.WriteType.SYNC);
	}

	@Test(expected = ClosedJournalException.class)
	public void testReadingFailsAfterClose() throws Exception {
		Location location = journal.write("data".getBytes("UTF-8"), Journal.WriteType.SYNC);
		journal.close();
		journal.read(location, Journal.ReadType.SYNC);
	}


	@Test(expected = ClosedJournalException.class)
	public void testRedoingFailsAfterClose() throws Exception {
		Location location = journal.write("data".getBytes("UTF-8"), Journal.WriteType.SYNC);
		journal.close();
		journal.redo();
	}


	@Test(expected = IOException.class)
	public void testOpenFailsWithInvalidDirectory() throws IOException {
		File badDir = new File("non-existing");
		Journal badJournal = new Journal();
		badJournal.setDirectory(badDir);
		try {
			badJournal.open();
			fail("Should file if dir does not exist!");
		} finally {
			badJournal.close();
		}
	}
}