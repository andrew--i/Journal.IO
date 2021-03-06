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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A builder style API to create ready to use {@link Journal}.
 */
public class JournalBuilder {

	private final File directory;
	private Boolean checksum;
	private Long disposeInterval;
	private ScheduledExecutorService disposer;
	private String filePrefix;
	private String fileSuffix;
	private Integer maxFileLength;
	private Integer maxWriteBatchSize;
	private Boolean physicalSync;
	private RecoveryErrorHandler recoveryErrorHandler;
	private ReplicationTarget replicationTarget;
	private Executor writer;

	private JournalBuilder(File directory) {
		// Checks directory exists and is a directory
		// Also ensures current user has write access
		if (!directory.exists()) {
			throw new IllegalArgumentException("<" + directory + "> does not exist");
		}
		if (!directory.isDirectory()) {
			throw new IllegalArgumentException("<" + directory + "> is not a directory");
		}
		if (!directory.canWrite()) {
			throw new IllegalArgumentException("Cannot write to main directory <" + directory + ">");
		}

		this.directory = directory;
	}

	/**
	 * @param directory
	 * @return a {@link JournalBuilder} using {@code directory} as base
	 * directory
	 */
	public static JournalBuilder of(final File directory) {
		return new JournalBuilder(directory);
	}


	/**
	 * Set true to enable records checksum, false otherwise.
	 */
	public JournalBuilder setChecksum(Boolean checksum) {
		this.checksum = checksum;
		return this;
	}

	/**
	 * Set the milliseconds interval for resources disposal: i.e., un-accessed
	 * files will be closed.
	 */
	public JournalBuilder setDisposeInterval(Long disposeInterval) {
		this.disposeInterval = disposeInterval;
		return this;
	}

	/**
	 * Set the ScheduledExecutorService to use for internal resources disposing.
	 *
	 * Important note: the provided ScheduledExecutorService must be manually
	 * closed.
	 */
	public JournalBuilder setDisposer(ScheduledExecutorService disposer) {
		this.disposer = disposer;
		return this;
	}

	/**
	 * Set the prefix for log files.
	 */
	public JournalBuilder setFilePrefix(String filePrefix) {
		this.filePrefix = filePrefix;
		return this;
	}

	/**
	 * Set the suffix for log files.
	 */
	public JournalBuilder setFileSuffix(String fileSuffix) {
		this.fileSuffix = fileSuffix;
		return this;
	}

	/**
	 * Set the max length of each log file.
	 */
	public JournalBuilder setMaxFileLength(Integer maxFileLength) {
		this.maxFileLength = maxFileLength;
		return this;
	}

	/**
	 * Set the max size in bytes of the write batch: must always be equal or
	 * less than the max file length.
	 */
	public JournalBuilder setMaxWriteBatchSize(Integer maxWriteBatchSize) {
		this.maxWriteBatchSize = maxWriteBatchSize;
		return this;
	}

	/**
	 * Set true if every disk write must be followed by a physical disk sync,
	 * synchronizing file descriptor properties and flushing hardware buffers,
	 * false otherwise.
	 */
	public JournalBuilder setPhysicalSync(Boolean physicalSync) {
		this.physicalSync = physicalSync;
		return this;
	}

	/**
	 * Set the RecoveryErrorHandler to invoke in case of checksum errors.
	 */
	public JournalBuilder setRecoveryErrorHandler(RecoveryErrorHandler recoveryErrorHandler) {
		this.recoveryErrorHandler = recoveryErrorHandler;
		return this;
	}

	/**
	 * Set the {@link ReplicationTarget} to replicate batch writes to.
	 */
	public JournalBuilder setReplicationTarget(ReplicationTarget replicationTarget) {
		this.replicationTarget = replicationTarget;
		return this;
	}

	/**
	 * Set the Executor to use for writing new record entries.
	 *
	 * Important note: the provided Executor must be manually closed.
	 */
	public JournalBuilder setWriter(Executor writer) {
		this.writer = writer;
		return this;
	}

	/**
	 * @return a configured and opened {@link Journal}
	 * @throws IOException
	 */
	public Journal open() throws IOException {
		final Journal journal = new Journal();
		journal.setDirectory(this.directory);
		if (this.checksum != null) {
			journal.setChecksum(this.checksum);
		}
		if (this.disposeInterval != null) {
			journal.setDisposeInterval(this.disposeInterval);
		}
		if (this.disposer != null) {
			journal.setDisposer(this.disposer);
		}
		if (this.filePrefix != null) {
			journal.setFilePrefix(this.filePrefix);
		}
		if (this.fileSuffix != null) {
			journal.setFileSuffix(this.fileSuffix);
		}
		if (this.maxFileLength != null) {
			journal.setMaxFileLength(this.maxFileLength);
		}
		if (this.maxWriteBatchSize != null) {
			journal.setMaxWriteBatchSize(this.maxWriteBatchSize);
		}
		if (this.physicalSync != null) {
			journal.setPhysicalSync(this.physicalSync);
		}
		if (this.recoveryErrorHandler != null) {
			journal.setRecoveryErrorHandler(this.recoveryErrorHandler);
		}
		if (this.replicationTarget != null) {
			journal.setReplicationTarget(this.replicationTarget);
		}
		if (this.writer != null) {
			journal.setWriter(this.writer);
		}
		journal.open();
		return journal;
	}
}