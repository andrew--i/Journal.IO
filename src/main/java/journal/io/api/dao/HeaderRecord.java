package journal.io.api.dao;


public class HeaderRecord {
    private int pointer;
    private int size;
    private byte type;

    protected HeaderRecord() {

    }

    public int getPointer() {
        return pointer;
    }

    public int getSize() {
        return size;
    }

    public byte getType() {
        return type;
    }

    public static HeaderRecord of(int pointer, int size, byte type) {
        HeaderRecord headerRecord = new HeaderRecord();
        headerRecord.pointer = pointer;
        headerRecord.size = size;
        headerRecord.type = type;
        return headerRecord;
    }
}
