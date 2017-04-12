/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package journal.io.api;

import journal.io.api.dao.FileAccessBase;
import journal.io.api.dao.FileAccessFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
public class DataFile implements Comparable<DataFile> {

	private final Integer dataFileId;
	private final AtomicLong length;
	private volatile File file;
	private volatile Integer dataFileGeneration;
	private volatile DataFile next;

	DataFile(File file, int id) {
		this.file = file;
		this.dataFileId = id;
		this.dataFileGeneration = 0;
		this.length = new AtomicLong((file.exists() ? file.length() : 0));
	}

	File getFile() {
		return file;
	}

	public Integer getDataFileId() {
		return dataFileId;
	}

	public Integer getDataFileGeneration() {
		return dataFileGeneration;
	}

	DataFile getNext() {
		return next;
	}

	void setNext(DataFile next) {
		this.next = next;
	}

	public long getLength() {
		return this.length.get();
	}

	public void setLength(long length) {
		this.length.set(length);
	}

	public void incrementLength(int size) {
		this.length.addAndGet(size);
	}

	public FileAccessBase createDataAccess() throws IOException {
		return new FileAccessFactory().create(file);
	}

	void writeHeader() throws IOException {
		FileAccessBase dataAccess = createDataAccess();
		length.set(dataAccess.writeHeader());

	}

	void verifyHeader() throws IOException {
		FileAccessBase dataAccess = createDataAccess();
		dataAccess.verifyHeader();
	}

	@Override
	public int compareTo(DataFile df) {
		return dataFileId - df.dataFileId;
	}

	@Override
	public boolean equals(Object o) {
		boolean result = false;
		if (o instanceof DataFile) {
			result = compareTo((DataFile) o) == 0;
		}
		return result;
	}

	@Override
	public int hashCode() {
		return dataFileId;
	}

	@Override
	public String toString() {
		return file.getName() + ", number = " + dataFileId + ", generation = " + dataFileGeneration + ", length = " + length;
	}
}
