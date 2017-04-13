package journal.io.api.dao;

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
}
