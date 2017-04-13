package journal.io.api.dao;

import static journal.io.api.dao.ConfigurationFactory.MAGIC_STRING;
import static journal.io.api.dao.ConfigurationFactory.MAGIC_STRING_BYTES;

public class RandomFileAccessConfiguration implements FileAccessConfiguration {

	@Override
	public String getMagicString() {
		return MAGIC_STRING;
	}

	@Override
	public byte[] getMagicStringBytes() {
		return MAGIC_STRING_BYTES;
	}

	@Override
	public int getMagicStringSize() {
		return MAGIC_STRING_BYTES.length;
	}

	@Override
	public int getStorageVersion() {
		return 130;
	}

	@Override
	public int getStorageVersionSize() {
		return 4;
	}

	@Override
	public int getFileHeaderSize() {
		return getMagicStringSize() + getStorageVersionSize();
	}

	@Override
	public int getRecordPointerSize() {
		return 4;
	}

	@Override
	public int getRecordLengthSize() {
		return 4;
	}

	@Override
	public int getRecordTypeSize() {
		return 1;
	}

	@Override
	public int getRecordHeaderSize() {
		return getRecordPointerSize() + getRecordLengthSize() + getRecordTypeSize();
	}

	@Override
	public int getChecksumSize() {
		return 8;
	}

	@Override
	public int getBatchControlRecordSize() {
		return getRecordHeaderSize() + getChecksumSize();
	}
}
