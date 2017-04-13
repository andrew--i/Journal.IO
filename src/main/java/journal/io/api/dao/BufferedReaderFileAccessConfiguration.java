package journal.io.api.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static journal.io.api.dao.ConfigurationFactory.MAGIC_STRING;
import static journal.io.api.dao.ConfigurationFactory.MAGIC_STRING_BYTES;

public class BufferedReaderFileAccessConfiguration implements FileAccessConfiguration {

	@Override
	public String getMagicString() {
		return MAGIC_STRING + "\n";
	}

	@Override
	public byte[] getMagicStringBytes() {
		return MAGIC_STRING_BYTES;
	}

	@Override
	public int getMagicStringSize() {
		return MAGIC_STRING.length();
	}

	@Override
	public int getStorageVersion() {
		return 131;
	}

	@Override
	public int getStorageVersionSize() {
		return 5;
	}

	@Override
	public int getFileHeaderSize() {
		return getMagicStringSize() + getStorageVersionSize();
	}

	@Override
	public int getRecordPointerSize() {
		return 1 + 4 + 1;
	}

	@Override
	public int getRecordLengthSize() {
		return 4 + 1;
	}

	@Override
	public int getRecordTypeSize() {
		return 1 + 1;
	}

	@Override
	public int getRecordHeaderSize() {
		return getRecordPointerSize() + getRecordLengthSize() + getRecordTypeSize();
	}

	@Override
	public int getChecksumSize() {
		return 8 + 1;
	}

	@Override
	public int getBatchControlRecordSize() {
		return getRecordHeaderSize() + getChecksumSize();
	}

	@Override
	public int getDataLength(byte[] data) {
		return new String(data).length();
	}

	@Override
	public long getFileLength(File file) {
		if(!file.exists())
			return 0;
		int length = 0;
		try {
			try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
				while (reader.read() != -1) {
					length++;
				}
			}
			return length;
		} catch (IOException e) {
			return 0;
		}
	}
}
