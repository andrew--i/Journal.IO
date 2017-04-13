package journal.io.api.dao;

import journal.io.api.Location;
import journal.io.api.operation.WriteCommand;

import java.io.*;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

public class BufferedReaderAccessFile implements FileAccess {

	private File file;
	private BufferedReader bufferedReader;
	private FileAccessConfiguration configuration;
	private long currentPosition = 0;

	public BufferedReaderAccessFile(File file, FileAccessConfiguration configuration) throws IOException {
		this.file = file;
		if (!file.exists())
			file.createNewFile();
		bufferedReader = new BufferedReader(new FileReader(file));
		currentPosition = 0;
		this.configuration = configuration;
	}

	@Override
	public long writeFileHeader() throws IOException {
		try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file))) {
			bufferedWriter.write(configuration.getMagicString());
			bufferedWriter.write(configuration.getStorageVersion() + "\n");
			return configuration.getFileHeaderSize();
		}
	}

	@Override
	public void verifyFileHeader() throws IOException {

		try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
			char[] magic = new char[configuration.getMagicStringSize()];
			if (bufferedReader.read(magic) == configuration.getMagicStringSize() && Arrays.equals(magic, configuration.getMagicString().toCharArray())) {
				int version = Integer.parseInt(bufferedReader.readLine());
				if (version != configuration.getStorageVersion()) {
					throw new IllegalStateException("Incompatible storage version, found: " + version + ", required: " + configuration.getStorageVersion());
				}
			} else {
				throw new IOException("Incompatible magic string!");
			}
		}
	}

	@Override
	public void skip(int size) throws IOException {
		int n = 0;
		while (n < size) {
			long skipped = bufferedReader.skip(size - n);
			if (skipped == 0) {
				throw new EOFException();
			}
			n += skipped;
		}
		currentPosition += size;
	}

	@Override
	public long getCurrentPosition() throws IOException {
		return currentPosition;
	}

	@Override
	public long size() throws IOException {
		try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
			int size = 0;
			while (bufferedReader.read() > -1)
				size++;
			return size;
		}
	}

	@Override
	public boolean skipLocationData(Location location) throws IOException {
		int toSkip = location.getSize() - configuration.getRecordHeaderSize();
		if (size() - getCurrentPosition() >= toSkip) {
			skip(toSkip);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void close() throws IOException {
		bufferedReader.close();
		currentPosition = 0;
	}

	@Override
	public void setSize(long size) throws IOException {
		final File tempFile = File.createTempFile("journal", "tmp");
		copyFile(file, tempFile, size);
		file.delete();
		file.createNewFile();
		copyFile(tempFile, file, size);
		tempFile.delete();
		sync();
	}

	private void copyFile(File src, File dst, long size) throws IOException {
		try (BufferedReader reader = new BufferedReader(new FileReader(src))) {
			try (BufferedWriter writer = new BufferedWriter(new FileWriter(dst))) {
				int count = 0;
				int ch = -1;
				while ((ch = reader.read()) != -1 && count < size) {
					writer.write(ch);
					count++;
				}
			}
		}

	}

	@Override
	public void sync() throws IOException {
		close();
		bufferedReader = new BufferedReader(new FileReader(file));
		currentPosition = 0;

	}

	@Override
	public byte[] readLocationData(Location location) throws IOException {
		if (location.isBatchControlRecord()) {
			CharBuffer checksum = CharBuffer.allocate(configuration.getChecksumSize());
			final CharBuffer buffer = read(checksum);
			return new String(buffer.array()).getBytes(Charset.forName("UTF-8"));
		} else {
			CharBuffer data = CharBuffer.allocate(location.getSize() - configuration.getRecordHeaderSize());
			final CharBuffer buffer = read(data);
			return new String(buffer.array()).getBytes(Charset.forName("UTF-8"));
		}
	}

	@Override
	public void seek(long position) throws IOException {
		sync();
		while (currentPosition < position) {
			bufferedReader.read();
			currentPosition++;
		}
	}

	@Override
	public HeaderRecord readRecordHeader() throws IOException {
		CharBuffer headerBuffer = read(CharBuffer.allocate(configuration.getRecordHeaderSize()));

		final String[] items = new String(headerBuffer.array()).split("\n");

		int startIndex = items.length == 4 ? 1 : 0;
		int pointer = Integer.parseInt(items[startIndex].trim());
		int size = Integer.parseInt(items[startIndex + 1].trim());
		byte type = Byte.parseByte(items[startIndex + 2].trim());
		return HeaderRecord.of(pointer, size, type);
	}

	@Override
	public boolean hasRecordHeader(long position, boolean isJournalOpened) throws IOException {
		long remaining = size() - position;
		if (remaining >= configuration.getRecordHeaderSize()) {
			return true;
		} else if (remaining == 0) {
			return false;
		} else {

			if (isJournalOpened) {
				// If journal is open, it means the positions may be wrong due to
				// compaction:
				return false;
			} else {
				// If journal is not open yet, it means we are recovering and
				// need to signal a failure:
				throw new IllegalStateException("Remaining file length doesn't fit a record header at position: " + position);
			}
		}
	}

	@Override
	public void write(byte[] data) throws IOException {
		try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true))) {
			bufferedWriter.write(new String(data));
		}
	}

	@Override
	public byte[] createDataForWrite(int size, Queue<WriteCommand> writes, boolean checksum) {
		CharBuffer buffer = CharBuffer.allocate(size);
		Checksum adler32 = new Adler32();
		WriteCommand control = writes.peek();


		// Write an empty batch control record.
		buffer.append("\n" + intToString(control.getLocation().getPointer(), 4) + "\n");
		buffer.append(intToString(configuration.getBatchControlRecordSize(), 4) + "\n");
		buffer.append(Location.BATCH_CONTROL_RECORD_TYPE + "\n");
		buffer.append(intToString(0, 8) + "\n");

		Iterator<WriteCommand> commands = writes.iterator();
		// Skip the control write:
		commands.next();
		// Process others:
		while (commands.hasNext()) {
			WriteCommand current = commands.next();
			buffer.append("\n" + intToString(current.getLocation().getPointer(), 4) + "\n");
			buffer.append(intToString(current.getLocation().getSize(), 4) + "\n");
			buffer.append(current.getLocation().getType() + "\n");
			buffer.append(new String(current.getData()));
			if (checksum) {
				adler32.update(current.getData(), 0, current.getData().length);
			}
		}

		// Now we can fill in the batch control record properly.
		buffer.position(configuration.getRecordHeaderSize());
		if (checksum) {
			buffer.put(intToString(adler32.getValue(), 8) + "\n");
		}
		return new String(buffer.array()).getBytes(Charset.forName("UTF-8"));
	}

	private static String intToString(long value, int stringLength) {
		StringBuilder s = new StringBuilder(value + "");
		while (s.length() < stringLength) {
			s.insert(0, " ");
		}
		return s.toString();
	}

	private CharBuffer read(CharBuffer charBuffer) throws IOException {
		currentPosition += bufferedReader.read(charBuffer);
		return charBuffer;
	}
}
