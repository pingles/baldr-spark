package mastodonc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BaldrReader {
    private final long position;
    private InputStream stream;
    private final int RECORD_LENGTH_SIZE = 8;

    public BaldrReader(InputStream stream) {
        this.stream = stream;
        this.position = 0;
    }

    public BaldrRecord next() throws IOException {
        byte[] recordLengthBytes = new byte[RECORD_LENGTH_SIZE];
        int bytesRead = stream.read(recordLengthBytes, 0, RECORD_LENGTH_SIZE);
        if (bytesRead < 0) {
            return null;
        }

        ByteBuffer recordLengthBuffer = ByteBuffer.wrap(recordLengthBytes);
        recordLengthBuffer.order(ByteOrder.BIG_ENDIAN);
        long recordLength = recordLengthBuffer.getLong();

        if (recordLength > Integer.MAX_VALUE) {
            throw new RuntimeException("record larger than expected");
        }
        int recordLengthInt = (int)recordLength;
        byte[] recordBytes = new byte[recordLengthInt];
        stream.read(recordBytes, 0, recordLengthInt);

        return new BaldrRecord(position, recordBytes, recordLengthInt);
    }

    public void close() throws IOException {
        this.stream.close();
    }
}
