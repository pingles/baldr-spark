package mastodonc;

public class BaldrRecord {
    private final long position;
    private final byte[] recordBytes;
    private final int recordLength;

    public BaldrRecord(long position, byte[] recordBytes, int recordLength) {
        this.position = position;
        this.recordBytes = recordBytes;
        this.recordLength = recordLength;
    }

    public long position() {
        return position;
    }

    public byte[] bytes() {
        return recordBytes;
    }

    public int length() {
        return recordLength;
    }
}
