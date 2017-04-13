package journal.io.api.dao;

import java.io.File;

public interface FileAccessConfiguration {
	String getMagicString();

	byte[] getMagicStringBytes();

	int getMagicStringSize();

	int getStorageVersion();

	int getStorageVersionSize();

	int getFileHeaderSize();

	int getRecordPointerSize();

	int getRecordLengthSize();

	int getRecordTypeSize();

	int getRecordHeaderSize();

	int getChecksumSize();

	int getBatchControlRecordSize();

	int getDataLength(byte[] data);

	long getFileLength(File file);
}
